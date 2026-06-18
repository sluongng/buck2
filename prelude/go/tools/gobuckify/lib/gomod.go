/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

package gobuckifylib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const GoModBuckFileName = "BUCK.gomod"

type PackageError struct {
	Err string
}

type GoModPackage struct {
	Dir        string
	ImportPath string
	Name       string
	Standard   bool
	Goroot     bool
	Module     *Module
	Imports    []string

	GoFiles       []string
	CgoFiles      []string
	HFiles        []string
	CFiles        []string
	CXXFiles      []string
	SFiles        []string
	SysoFiles     []string
	EmbedFiles    []string
	EmbedPatterns []string
	TestGoFiles   []string
	XTestGoFiles  []string

	Error *PackageError
}

type goModTarget struct {
	Name               string
	Rule               string
	ImportPath         string
	PackageRoot        string
	Srcs               []string
	EmbedSrcs          []string
	GeneratedEmbedSrcs map[string]string
	Deps               []string
}

type goModArchive struct {
	Name        string
	Out         string
	URL         string
	SHA256      string
	StripPrefix string
	Type        string
	SubTargets  []string
	Patches     []string
	PatchArgs   []string
}

type goModProtoCompiler struct {
	Name       string
	PluginName string
	Plugin     string
	Suffix     string
	Options    []string
}

type goModState struct {
	RootDir       string
	RootModule    *Module
	Packages      map[string]*GoModPackage
	ProtoTargets  []*goModProtoTarget
	Labels        map[string]string
	TargetNames   map[string]string
	Archives      map[string]*goModArchive
	ProtoArchives map[string]*goModArchive
	ModulePatches map[string]goModModulePatch
	FilesByDir    map[string][]string
	NeedGRPCTool  bool
	ZipSHA256     map[string]string
}

type goModModulePatch struct {
	Patches   []string
	PatchArgs []string
}

func GenerateGoModBuckFiles(workDir string, patterns []string) error {
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	rootModule, err := queryRootModule(workDir)
	if err != nil {
		return err
	}
	if rootModule.Dir == "" {
		rootModule.Dir = workDir
	}

	protoScan, err := scanGoProtoTargets(rootModule.Dir)
	if err != nil {
		return err
	}
	patterns = append(patterns, protoScan.ToolPatterns...)
	patterns = append(patterns, protoScan.ExtraPackageImports...)

	pkgs, err := queryGoModPackages(workDir, patterns)
	if err != nil {
		return err
	}
	zipSHA256, err := queryModuleZipSHA256(workDir)
	if err != nil {
		return err
	}
	modulePatches, err := queryGoModulePatches(rootModule.Dir, rootModule.Path)
	if err != nil {
		return err
	}

	state := &goModState{
		RootDir:       rootModule.Dir,
		RootModule:    rootModule,
		Packages:      map[string]*GoModPackage{},
		ProtoTargets:  protoScan.Targets,
		Labels:        map[string]string{},
		TargetNames:   map[string]string{},
		Archives:      map[string]*goModArchive{},
		ProtoArchives: map[string]*goModArchive{},
		ModulePatches: modulePatches,
		FilesByDir:    map[string][]string{},
		NeedGRPCTool:  protoScan.NeedGRPCTool,
		ZipSHA256:     zipSHA256,
	}

	for _, pkg := range pkgs {
		if shouldSkipGoModPackage(pkg) {
			continue
		}
		files := packageFiles(pkg)
		if len(files) == 0 {
			continue
		}
		state.Packages[pkg.ImportPath] = pkg
		state.FilesByDir[pkg.ImportPath] = files
		state.Labels[pkg.ImportPath] = state.labelForPackage(pkg)
	}
	state.aliasReplaceModulePatches()
	if state.NeedGRPCTool {
		if err := state.addGRPCToolPackage(workDir); err != nil {
			return err
		}
	}
	for _, target := range state.ProtoTargets {
		state.Labels[target.ImportPath] = protoTargetLabel(target)
	}
	state.resolveProtoDeps()
	if err := state.attachExternalProtoSources(); err != nil {
		return err
	}

	if err := state.writeRootPackageFiles(); err != nil {
		return err
	}
	if err := state.writePatchBuildFiles(); err != nil {
		return err
	}
	return state.writeThirdPartyFile()
}

func queryRootModule(workDir string) (*Module, error) {
	cmd := exec.Command("go", "list", "-m", "-json")
	cmd.Dir = workDir
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to query root module: %w", err)
	}
	var mod Module
	if err := json.Unmarshal(out, &mod); err != nil {
		return nil, fmt.Errorf("failed to decode root module: %w", err)
	}
	return &mod, nil
}

func queryGoModulePatches(rootDir string, rootModulePath string) (map[string]goModModulePatch, error) {
	result := map[string]goModModulePatch{}
	rootRepoName, err := rootBazelRepoName(rootDir, rootModulePath)
	if err != nil {
		return nil, err
	}
	files, err := goDepsModuleFiles(rootDir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		for _, block := range goModuleOverrideBlocks(string(content)) {
			modulePath := stringValueInBlock(block, "path")
			if modulePath == "" {
				continue
			}
			var patches []string
			for _, patchLabel := range stringListValueInBlock(block, "patches") {
				patchLabel, ok := normalizeRootPatchLabel(patchLabel, rootRepoName)
				if !ok {
					continue
				}
				isSourcePatch, err := isGoSourcePatch(rootDir, patchLabel)
				if err != nil {
					return nil, err
				}
				if !isSourcePatch {
					continue
				}
				patches = append(patches, patchLabel)
			}
			if len(patches) == 0 {
				continue
			}
			patchArgs := []string{"-p0"}
			if strip := intValueInBlock(block, "patch_strip"); strip != "" {
				patchArgs = []string{"-p" + strip}
			}
			existing := result[modulePath]
			existing.Patches = append(existing.Patches, patches...)
			existing.PatchArgs = patchArgs
			result[modulePath] = existing
		}
	}
	return result, nil
}

func rootBazelRepoName(rootDir string, rootModulePath string) (string, error) {
	content, err := os.ReadFile(filepath.Join(rootDir, "MODULE.bazel"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err == nil {
		if repoName := stringValueInBlock(string(content), "repo_name"); repoName != "" {
			return repoName, nil
		}
		if name := stringValueInBlock(string(content), "name"); name != "" {
			return name, nil
		}
	}
	return bazelRepoNameFromModulePath(rootModulePath), nil
}

func goDepsModuleFiles(rootDir string) ([]string, error) {
	seen := map[string]bool{}
	var files []string
	add := func(path string) {
		if path == "" || seen[path] {
			return
		}
		if _, err := os.Stat(path); err == nil {
			seen[path] = true
			files = append(files, path)
		}
	}
	moduleFile := filepath.Join(rootDir, "MODULE.bazel")
	add(moduleFile)
	content, err := os.ReadFile(moduleFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		includeRe := regexp.MustCompile(`include\(\s*"//([^:"]+):([^"]+)"\s*\)`)
		for _, match := range includeRe.FindAllStringSubmatch(string(content), -1) {
			add(filepath.Join(rootDir, filepath.FromSlash(match[1]), match[2]))
		}
	}
	add(filepath.Join(rootDir, "deps", "go_deps.MODULE.bazel"))
	sort.Strings(files)
	return files, nil
}

func goModuleOverrideBlocks(content string) []string {
	content = stripStarlarkLineComments(content)
	re := regexp.MustCompile("(?s)go_deps\\.module_override\\((.*?)\\)")
	matches := re.FindAllStringSubmatch(content, -1)
	blocks := make([]string, 0, len(matches))
	for _, match := range matches {
		blocks = append(blocks, match[1])
	}
	return blocks
}

func stripStarlarkLineComments(content string) string {
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "#"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}

func stringValueInBlock(block string, name string) string {
	re := regexp.MustCompile(`(?m)` + regexp.QuoteMeta(name) + `\s*=\s*"([^"]+)"`)
	match := re.FindStringSubmatch(block)
	if match == nil {
		return ""
	}
	return match[1]
}

func intValueInBlock(block string, name string) string {
	re := regexp.MustCompile(`(?m)` + regexp.QuoteMeta(name) + `\s*=\s*([0-9]+)`)
	match := re.FindStringSubmatch(block)
	if match == nil {
		return ""
	}
	return match[1]
}

func stringListValueInBlock(block string, name string) []string {
	listRe := regexp.MustCompile(`(?s)` + regexp.QuoteMeta(name) + `\s*=\s*\[(.*?)\]`)
	listMatch := listRe.FindStringSubmatch(block)
	if listMatch == nil {
		return nil
	}
	stringRe := regexp.MustCompile(`"([^"]+)"`)
	var values []string
	for _, match := range stringRe.FindAllStringSubmatch(listMatch[1], -1) {
		values = append(values, match[1])
	}
	return values
}

func normalizeRootPatchLabel(label string, rootRepoName string) (string, bool) {
	if strings.HasPrefix(label, "//") {
		return label, true
	}
	prefix := "@" + rootRepoName + "//"
	if strings.HasPrefix(label, prefix) {
		return "//" + strings.TrimPrefix(label, prefix), true
	}
	return "", false
}

func isGoSourcePatch(rootDir string, label string) (bool, error) {
	dir, file, ok := parseRootFileLabel(label)
	if !ok {
		return false, nil
	}
	content, err := os.ReadFile(filepath.Join(rootDir, filepath.FromSlash(dir), file))
	if err != nil {
		return false, err
	}
	diffRe := regexp.MustCompile("(?m)^diff --git a/([^\\r\\n ]+) b/([^\\r\\n ]+)$")
	matches := diffRe.FindAllStringSubmatch(string(content), -1)
	if len(matches) == 0 {
		return false, nil
	}
	for _, match := range matches {
		if !isGoSourcePatchPath(match[1]) || !isGoSourcePatchPath(match[2]) {
			return false, nil
		}
	}
	return true, nil
}

func isGoSourcePatchPath(path string) bool {
	base := filepath.Base(path)
	if base == "BUILD" || base == "BUILD.bazel" || base == "WORKSPACE" || base == "WORKSPACE.bazel" || strings.HasSuffix(path, ".bzl") {
		return false
	}
	switch filepath.Ext(path) {
	case ".go", ".c", ".h", ".s", ".S", ".cc", ".cpp", ".hpp", ".proto", ".mod", ".sum":
		return true
	default:
		return false
	}
}

func bazelRepoNameFromModulePath(modulePath string) string {
	parts := strings.Split(modulePath, "/")
	if len(parts) == 0 {
		return sanitizeBuckName(modulePath)
	}
	domainParts := strings.Split(parts[0], ".")
	for i, j := 0, len(domainParts)-1; i < j; i, j = i+1, j-1 {
		domainParts[i], domainParts[j] = domainParts[j], domainParts[i]
	}
	allParts := append(domainParts, parts[1:]...)
	return strings.Trim(invalidBuckNameChar.ReplaceAllString(strings.Join(allParts, "_"), "_"), "_")
}

func queryGoModPackages(workDir string, patterns []string) ([]*GoModPackage, error) {
	args := append([]string{"list", "-e", "-deps", "-json"}, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = workDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create go list stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create go list stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start go list: %w", err)
	}

	var pkgs []*GoModPackage
	dec := json.NewDecoder(stdout)
	for {
		var pkg GoModPackage
		if err := dec.Decode(&pkg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to decode go list output: %w", err)
		}
		pkgs = append(pkgs, &pkg)
	}

	stderrBytes, readErr := io.ReadAll(stderr)
	waitErr := cmd.Wait()
	if readErr != nil {
		return nil, fmt.Errorf("failed to read go list stderr: %w", readErr)
	}
	if waitErr != nil {
		return nil, fmt.Errorf("go list failed: %w\n%s", waitErr, string(stderrBytes))
	}
	return pkgs, nil
}

type moduleDownload struct {
	Path    string
	Version string
	Zip     string
	Dir     string
	Error   string
}

func queryModuleZipSHA256(workDir string) (map[string]string, error) {
	cmd := exec.Command("go", "mod", "download", "-json", "all")
	cmd.Dir = workDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create go mod download stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create go mod download stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start go mod download: %w", err)
	}

	checksums := map[string]string{}
	dec := json.NewDecoder(stdout)
	for {
		var mod moduleDownload
		if err := dec.Decode(&mod); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to decode go mod download output: %w", err)
		}
		if mod.Error != "" || mod.Zip == "" {
			continue
		}
		sha, err := fileSHA256(mod.Zip)
		if err != nil {
			return nil, err
		}
		checksums[mod.Path+"@"+mod.Version] = sha
	}

	stderrBytes, readErr := io.ReadAll(stderr)
	waitErr := cmd.Wait()
	if readErr != nil {
		return nil, fmt.Errorf("failed to read go mod download stderr: %w", readErr)
	}
	if waitErr != nil {
		return nil, fmt.Errorf("go mod download failed: %w\n%s", waitErr, string(stderrBytes))
	}
	return checksums, nil
}

func (s *goModState) addGRPCToolPackage(workDir string) error {
	version, err := goSumVersion(workDir, protocGenGoGRPCImport)
	if err != nil {
		return err
	}
	mod, err := downloadModule(workDir, protocGenGoGRPCImport, version)
	if err != nil {
		return err
	}
	if mod.Dir == "" || mod.Zip == "" {
		return fmt.Errorf("downloaded %s@%s without module dir or zip", protocGenGoGRPCImport, version)
	}
	sha, err := fileSHA256(mod.Zip)
	if err != nil {
		return err
	}
	s.ZipSHA256[mod.Path+"@"+mod.Version] = sha
	files, err := goSourceFiles(mod.Dir)
	if err != nil {
		return err
	}
	pkg := &GoModPackage{
		Dir:        mod.Dir,
		ImportPath: protocGenGoGRPCImport,
		Name:       "main",
		Module: &Module{
			Path:    mod.Path,
			Version: mod.Version,
			Dir:     mod.Dir,
		},
		Imports: []string{
			"google.golang.org/protobuf/compiler/protogen",
			"google.golang.org/protobuf/reflect/protoreflect",
			"google.golang.org/protobuf/types/descriptorpb",
			"google.golang.org/protobuf/types/pluginpb",
		},
		GoFiles: files,
	}
	s.Packages[pkg.ImportPath] = pkg
	s.FilesByDir[pkg.ImportPath] = files
	s.Labels[pkg.ImportPath] = s.labelForPackage(pkg)
	return nil
}

func goSumVersion(workDir string, modulePath string) (string, error) {
	content, err := os.ReadFile(filepath.Join(workDir, "go.sum"))
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile("(?m)^" + regexp.QuoteMeta(modulePath) + " (v[^ ]+) h1:")
	match := re.FindStringSubmatch(string(content))
	if match == nil {
		return "", fmt.Errorf("missing %s module version in go.sum", modulePath)
	}
	return match[1], nil
}

func downloadModule(workDir string, modulePath string, version string) (*moduleDownload, error) {
	cmd := exec.Command("go", "mod", "download", "-json", modulePath+"@"+version)
	cmd.Dir = workDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to download %s@%s: %w\n%s", modulePath, version, err, string(out))
	}
	var mod moduleDownload
	if err := json.Unmarshal(out, &mod); err != nil {
		return nil, fmt.Errorf("failed to decode downloaded module %s@%s: %w", modulePath, version, err)
	}
	if mod.Error != "" {
		return nil, fmt.Errorf("failed to download %s@%s: %s", modulePath, version, mod.Error)
	}
	return &mod, nil
}

func goSourceFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, name)
	}
	sort.Strings(files)
	return files, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open module zip %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to hash module zip %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func shouldSkipGoModPackage(pkg *GoModPackage) bool {
	return pkg == nil || pkg.Standard || pkg.Goroot || pkg.ImportPath == "" || pkg.Module == nil
}

func packageFiles(pkg *GoModPackage) []string {
	seen := map[string]bool{}
	var files []string
	for _, list := range [][]string{pkg.GoFiles, pkg.CgoFiles, pkg.HFiles, pkg.CFiles, pkg.CXXFiles, pkg.SFiles, pkg.SysoFiles} {
		for _, file := range list {
			if file == "" || seen[file] {
				continue
			}
			seen[file] = true
			files = append(files, file)
		}
	}
	sort.Strings(files)
	return files
}

func (s *goModState) isRootPackage(pkg *GoModPackage) bool {
	return pkg.Module != nil && pkg.Module.Path == s.RootModule.Path
}

func (s *goModState) labelForPackage(pkg *GoModPackage) string {
	if s.isRootPackage(pkg) {
		name := rootTargetName(pkg.ImportPath)
		rel, err := filepath.Rel(s.RootDir, pkg.Dir)
		if err != nil || rel == "." {
			return "//:" + name
		}
		return "//" + filepath.ToSlash(rel) + ":" + name
	}
	return "//third_party/go:" + s.externalTargetName(pkg.ImportPath)
}

func rootTargetName(importPath string) string {
	base := TargetNameFromImportPath(importPath)
	if base == "." || base == "/" || base == "" {
		base = "go_default"
	}
	return sanitizeBuckName(base)
}

func (s *goModState) externalTargetName(importPath string) string {
	if name, ok := s.TargetNames[importPath]; ok {
		return name
	}
	name := rootTargetName(importPath)
	for otherImport, otherName := range s.TargetNames {
		if otherImport != importPath && otherName == name {
			name = sanitizeBuckName(importPath)
			break
		}
	}
	for otherImport, otherName := range s.TargetNames {
		if otherImport != importPath && otherName == name {
			name = sanitizeBuckName(importPath + "_" + shortHash(importPath))
			break
		}
	}
	s.TargetNames[importPath] = name
	return name
}

func (s *goModState) depsForPackage(pkg *GoModPackage) []string {
	seen := map[string]bool{}
	var deps []string
	for _, imp := range pkg.Imports {
		label, ok := s.Labels[imp]
		if !ok || label == s.Labels[pkg.ImportPath] || seen[label] {
			continue
		}
		seen[label] = true
		deps = append(deps, label)
	}
	sort.Strings(deps)
	return deps
}

func (s *goModState) resolveProtoDeps() {
	for _, target := range s.ProtoTargets {
		seen := map[string]bool{}
		var deps []string
		for _, importPath := range target.DepImportPaths {
			label, ok := s.Labels[importPath]
			if !ok || label == protoTargetLabel(target) || seen[label] {
				continue
			}
			seen[label] = true
			deps = append(deps, label)
		}
		sort.Strings(deps)
		target.Deps = deps
	}
}

func (s *goModState) writeRootPackageFiles() error {
	byDir := map[string][]*goModTarget{}
	protoByDir := map[string][]*goModProtoTarget{}
	for _, pkg := range sortedPackages(s.Packages) {
		if !s.isRootPackage(pkg) {
			continue
		}
		rel, err := filepath.Rel(s.RootDir, pkg.Dir)
		if err != nil {
			return fmt.Errorf("failed to relativize package dir %s: %w", pkg.Dir, err)
		}
		target := &goModTarget{
			Name:               rootTargetName(pkg.ImportPath),
			Rule:               goRuleForPackage(pkg),
			ImportPath:         pkg.ImportPath,
			Srcs:               s.FilesByDir[pkg.ImportPath],
			EmbedSrcs:          embedFilesForPackage(pkg),
			GeneratedEmbedSrcs: missingLiteralEmbeds(pkg),
			Deps:               s.depsForPackage(pkg),
		}
		byDir[rel] = append(byDir[rel], target)
	}
	for _, target := range s.ProtoTargets {
		protoByDir[target.Dir] = append(protoByDir[target.Dir], target)
	}
	for dir := range protoByDir {
		if _, ok := byDir[dir]; !ok {
			byDir[dir] = nil
		}
	}

	for rel, targets := range byDir {
		dir := s.RootDir
		if rel != "." {
			dir = filepath.Join(s.RootDir, rel)
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, GoModBuckFileName), []byte(renderGoModBuck(nil, targets, protoByDir[rel])), 0644); err != nil {
			return err
		}
	}
	return nil
}

func (s *goModState) writePatchBuildFiles() error {
	filesByDir := map[string][]string{}
	for _, modulePatch := range s.ModulePatches {
		for _, label := range modulePatch.Patches {
			dir, file, ok := parseRootFileLabel(label)
			if !ok {
				continue
			}
			filesByDir[dir] = appendIfMissing(filesByDir[dir], file)
		}
	}
	for dir, files := range filesByDir {
		sort.Strings(files)
		absDir := filepath.Join(s.RootDir, filepath.FromSlash(dir))
		if err := os.MkdirAll(absDir, 0755); err != nil {
			return err
		}
		buildPath := filepath.Join(absDir, GoModBuckFileName)
		if _, err := os.Stat(buildPath); err == nil {
			content, err := os.ReadFile(buildPath)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(string(content), "# @generated by gobuckify gomod; do not edit.") {
				continue
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.WriteFile(buildPath, []byte(renderExportFilesBuck(files)), 0644); err != nil {
			return err
		}
	}
	return nil
}

func renderExportFilesBuck(files []string) string {
	var b strings.Builder
	b.WriteString("# @generated by gobuckify gomod; do not edit.\n\n")
	for _, file := range files {
		b.WriteString("export_file(\n")
		fmt.Fprintf(&b, "    name = %q,\n", file)
		b.WriteString("    visibility = [\"PUBLIC\"],\n")
		b.WriteString(")\n\n")
	}
	return b.String()
}

func parseRootFileLabel(label string) (string, string, bool) {
	if !strings.HasPrefix(label, "//") {
		return "", "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(label, "//"), ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (s *goModState) writeThirdPartyFile() error {
	var targets []*goModTarget
	for _, pkg := range sortedPackages(s.Packages) {
		if s.isRootPackage(pkg) {
			continue
		}
		archive, pkgRel, err := s.archiveForPackage(pkg)
		if err != nil {
			return err
		}
		var srcs []string
		for _, file := range s.FilesByDir[pkg.ImportPath] {
			srcs = append(srcs, fmt.Sprintf(":%s[%s]", archive.Name, filepath.ToSlash(filepath.Join(pkgRel, file))))
		}
		var embedSrcs []string
		for _, file := range existingRelativeFiles(pkg.Dir, pkg.EmbedFiles) {
			embedSrcs = append(embedSrcs, fmt.Sprintf(":%s[%s]", archive.Name, filepath.ToSlash(filepath.Join(pkgRel, file))))
		}
		targets = append(targets, &goModTarget{
			Name:        s.externalTargetName(pkg.ImportPath),
			Rule:        goRuleForPackage(pkg),
			ImportPath:  pkg.ImportPath,
			PackageRoot: filepath.ToSlash(filepath.Join(archive.Out, pkgRel)),
			Srcs:        srcs,
			EmbedSrcs:   embedSrcs,
			Deps:        s.depsForPackage(pkg),
		})
	}
	if len(targets) == 0 {
		return nil
	}
	dir := filepath.Join(s.RootDir, "third_party", "go")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	archives := make([]*goModArchive, 0, len(s.Archives))
	for _, archive := range s.Archives {
		sort.Strings(archive.SubTargets)
		archives = append(archives, archive)
	}
	sort.Slice(archives, func(i, j int) bool { return archives[i].Name < archives[j].Name })
	return os.WriteFile(filepath.Join(dir, GoModBuckFileName), []byte(renderGoModBuck(archives, targets, nil)), 0644)
}

func (s *goModState) archiveForPackage(pkg *GoModPackage) (*goModArchive, string, error) {
	mod := effectiveModule(pkg.Module)
	if mod.Version == "" {
		return nil, "", fmt.Errorf("module %s for package %s has no version; local replaces are not supported by gomod generation yet", mod.Path, pkg.ImportPath)
	}
	key := mod.Path + "@" + mod.Version
	archive, ok := s.Archives[key]
	if !ok {
		name := sanitizeBuckName("gomod_" + mod.Path + "_" + mod.Version)
		sha256, ok := s.ZipSHA256[key]
		if !ok {
			return nil, "", fmt.Errorf("missing downloaded zip checksum for module %s", key)
		}
		archive = &goModArchive{
			Name:        name,
			Out:         name,
			URL:         fmt.Sprintf("https://proxy.golang.org/%s/@v/%s.zip", escapeModulePath(mod.Path), mod.Version),
			StripPrefix: fmt.Sprintf("%s@%s", mod.Path, mod.Version),
			SHA256:      sha256,
			Type:        "zip",
		}
		s.applyModulePatches(archive, pkg.Module)
		s.Archives[key] = archive
	}
	modDir := mod.Dir
	if modDir == "" {
		modDir = pkg.Module.Dir
	}
	pkgRel, err := filepath.Rel(modDir, pkg.Dir)
	if err != nil {
		return nil, "", err
	}
	for _, file := range append(s.FilesByDir[pkg.ImportPath], existingRelativeFiles(pkg.Dir, pkg.EmbedFiles)...) {
		archive.SubTargets = appendIfMissing(archive.SubTargets, filepath.ToSlash(filepath.Join(pkgRel, file)))
	}
	return archive, filepath.ToSlash(pkgRel), nil
}

func (s *goModState) attachExternalProtoSources() error {
	for _, target := range s.ProtoTargets {
		seen := stringSet{}
		for _, protoPath := range target.ProtoSrcs {
			seen.Add(protoPath)
		}
		for _, protoPath := range append([]string{}, target.ProtoSrcs...) {
			if err := s.attachImportedExternalProtoSource(target, protoPath, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *goModState) attachImportedExternalProtoSource(target *goModProtoTarget, protoPath string, seen stringSet) error {
	if filepath.IsAbs(protoPath) {
		return nil
	}
	path := filepath.Join(s.RootDir, filepath.FromSlash(protoPath))
	for _, importedProto := range parseProtoImports(path) {
		if err := s.attachExternalProtoSource(target, importedProto, seen); err != nil {
			return err
		}
	}
	return nil
}

func (s *goModState) attachExternalProtoSource(target *goModProtoTarget, protoPath string, seen stringSet) error {
	source, ok := externalProtoSources[protoPath]
	if !ok {
		return nil
	}
	if seen[protoPath] {
		return nil
	}
	archive, err := s.externalProtoArchive(source)
	if err != nil {
		return err
	}
	archive.SubTargets = appendIfMissing(archive.SubTargets, source.SourcePath)
	label := fmt.Sprintf(":%s[%s]", archive.Name, source.SourcePath)
	if len(target.ProtoSrcLabels) == 0 {
		target.ProtoSrcLabels = localProtoFiles(target.Dir, target.ProtoSrcs)
	}
	target.ProtoSrcs = append(target.ProtoSrcs, protoPath)
	target.ProtoSrcLabels = append(target.ProtoSrcLabels, label)
	target.Archives = appendArchiveIfMissing(target.Archives, archive)
	seen.Add(protoPath)
	for _, dep := range externalProtoDeps[protoPath] {
		if err := s.attachExternalProtoSource(target, dep, seen); err != nil {
			return err
		}
	}
	return nil
}

func (s *goModState) externalProtoArchive(source externalProtoSource) (*goModArchive, error) {
	if source.Archive != nil {
		archive, ok := s.ProtoArchives[source.Archive.Name]
		if !ok {
			copy := *source.Archive
			copy.SubTargets = nil
			archive = &copy
			s.ProtoArchives[archive.Name] = archive
		}
		return archive, nil
	}
	mod := s.moduleForPath(source.ModulePath)
	if mod == nil || mod.Version == "" {
		return nil, fmt.Errorf("missing module version for external proto source %s", source.ModulePath)
	}
	key := mod.Path + "@" + mod.Version
	archive, ok := s.ProtoArchives[key]
	if !ok {
		sha256, ok := s.ZipSHA256[key]
		if !ok {
			return nil, fmt.Errorf("missing downloaded zip checksum for external proto module %s", key)
		}
		name := sanitizeBuckName("proto_" + mod.Path + "_" + mod.Version)
		archive = &goModArchive{
			Name:        name,
			Out:         name,
			URL:         fmt.Sprintf("https://proxy.golang.org/%s/@v/%s.zip", escapeModulePath(mod.Path), mod.Version),
			StripPrefix: fmt.Sprintf("%s@%s", mod.Path, mod.Version),
			SHA256:      sha256,
			Type:        "zip",
		}
		s.applyModulePatches(archive, mod)
		s.ProtoArchives[key] = archive
	}
	return archive, nil
}

func (s *goModState) moduleForPath(modulePath string) *Module {
	for _, pkg := range s.Packages {
		if pkg.Module == nil {
			continue
		}
		mod := effectiveModule(pkg.Module)
		if mod.Path == modulePath {
			return mod
		}
	}
	return nil
}

func (s *goModState) aliasReplaceModulePatches() {
	for _, pkg := range s.Packages {
		if pkg.Module == nil || pkg.Module.Replace == nil {
			continue
		}
		modulePatch, ok := s.ModulePatches[pkg.Module.Path]
		if ok {
			if _, exists := s.ModulePatches[pkg.Module.Replace.Path]; !exists {
				s.ModulePatches[pkg.Module.Replace.Path] = modulePatch
			}
		}
	}
}

func (s *goModState) applyModulePatches(archive *goModArchive, mod *Module) {
	if archive == nil || mod == nil {
		return
	}
	s.applyModulePatch(archive, mod.Path)
	if mod.Replace != nil {
		s.applyModulePatch(archive, mod.Replace.Path)
	}
}

func (s *goModState) applyModulePatch(archive *goModArchive, modulePath string) {
	modulePatch, ok := s.ModulePatches[modulePath]
	if !ok || len(modulePatch.Patches) == 0 {
		return
	}
	for _, patch := range modulePatch.Patches {
		archive.Patches = appendIfMissing(archive.Patches, patch)
	}
	archive.PatchArgs = append([]string{}, modulePatch.PatchArgs...)
}

func effectiveModule(mod *Module) *Module {
	if mod.Replace != nil {
		return mod.Replace
	}
	return mod
}

func appendArchiveIfMissing(archives []*goModArchive, archive *goModArchive) []*goModArchive {
	for _, existing := range archives {
		if existing == archive || existing.Name == archive.Name {
			return archives
		}
	}
	return append(archives, archive)
}

func goRuleForPackage(pkg *GoModPackage) string {
	if pkg.Name == "main" {
		return "go_binary"
	}
	return "go_library"
}

func renderGoModBuck(archives []*goModArchive, targets []*goModTarget, protoTargets []*goModProtoTarget) string {
	var b strings.Builder
	b.WriteString("# @generated by gobuckify gomod; do not edit.\n\n")
	if len(protoTargets) > 0 {
		archives = append(archives, protoArchives(protoTargets)...)
	}
	if len(protoTargets) > 0 {
		b.WriteString("load(\"@prelude//go:go_proto_library.bzl\", \"go_proto_compiler\", \"go_proto_library\")\n\n")
	}
	if hasGeneratedEmbeds(targets) {
		b.WriteString("load(\"@prelude//go:gomod_embed.bzl\", \"gomod_embed_file\")\n\n")
	}
	if len(protoTargets) > 0 {
		for _, src := range localExportedProtoFiles(protoTargets) {
			b.WriteString("export_file(\n")
			fmt.Fprintf(&b, "    name = %q,\n", src)
			b.WriteString("    visibility = [\"PUBLIC\"],\n")
			b.WriteString(")\n\n")
		}
	}
	for _, compiler := range protoCompilerTargets(protoTargets) {
		b.WriteString("go_proto_compiler(\n")
		fmt.Fprintf(&b, "    name = %q,\n", compiler.Name)
		fmt.Fprintf(&b, "    plugin_name = %q,\n", compiler.PluginName)
		fmt.Fprintf(&b, "    plugin = %q,\n", compiler.Plugin)
		if compiler.Suffix != ".pb.go" {
			fmt.Fprintf(&b, "    suffix = %q,\n", compiler.Suffix)
		}
		writeStringList(&b, "options", compiler.Options)
		b.WriteString("    visibility = [\"PUBLIC\"],\n")
		b.WriteString(")\n\n")
	}
	for _, archive := range archives {
		b.WriteString("http_archive(\n")
		fmt.Fprintf(&b, "    name = %q,\n", archive.Name)
		fmt.Fprintf(&b, "    out = %q,\n", archive.Out)
		fmt.Fprintf(&b, "    strip_prefix = %q,\n", archive.StripPrefix)
		fmt.Fprintf(&b, "    sha256 = %q,\n", archive.SHA256)
		if archive.Type != "" {
			fmt.Fprintf(&b, "    type = %q,\n", archive.Type)
		}
		if len(archive.Patches) > 0 {
			writeStringListPreserveOrder(&b, "patches", archive.Patches)
			writeStringListPreserveOrder(&b, "patch_args", archive.PatchArgs)
		}
		fmt.Fprintf(&b, "    urls = [%q],\n", archive.URL)
		if len(archive.SubTargets) > 0 {
			b.WriteString("    sub_targets = [\n")
			for _, subTarget := range archive.SubTargets {
				fmt.Fprintf(&b, "        %q,\n", subTarget)
			}
			b.WriteString("    ],\n")
		}
		b.WriteString("    visibility = [],\n")
		b.WriteString(")\n\n")
	}
	for _, target := range targets {
		for _, embedName := range sortedStringKeys(target.GeneratedEmbedSrcs) {
			genruleName := target.GeneratedEmbedSrcs[embedName]
			b.WriteString("gomod_embed_file(\n")
			fmt.Fprintf(&b, "    name = %q,\n", genruleName)
			fmt.Fprintf(&b, "    out = %q,\n", embedName)
			b.WriteString(")\n\n")
		}
	}
	for _, target := range targets {
		fmt.Fprintf(&b, "%s(\n", target.Rule)
		fmt.Fprintf(&b, "    name = %q,\n", target.Name)
		fmt.Fprintf(&b, "    import_path = %q,\n", target.ImportPath)
		if target.PackageRoot != "" {
			fmt.Fprintf(&b, "    package_root = %q,\n", target.PackageRoot)
		}
		writeStringList(&b, "srcs", target.Srcs)
		writeEmbedSrcs(&b, target)
		writeStringList(&b, "deps", target.Deps)
		b.WriteString("    visibility = [\"PUBLIC\"],\n")
		b.WriteString(")\n\n")
	}
	for _, target := range protoTargets {
		b.WriteString("go_proto_library(\n")
		fmt.Fprintf(&b, "    name = %q,\n", target.Name)
		fmt.Fprintf(&b, "    import_path = %q,\n", target.ImportPath)
		writeProtoSourceLists(&b, "srcs", "src_paths", target.Dir, target.Srcs, target.SrcLabels)
		writeProtoSourceLists(&b, "proto_srcs", "proto_src_paths", target.Dir, target.ProtoSrcs, target.ProtoSrcLabels)
		writeStringDict(&b, "import_mappings", target.ImportMappings)
		writeStringListPreserveOrder(&b, "compilers", protoCompilersForTarget(target))
		writeStringList(&b, "deps", target.Deps)
		if target.GRPC {
			writeStringListPreserveOrder(&b, "grpc_src_paths", target.GRPCSrcs)
		}
		if target.VTProto {
			writeStringListPreserveOrder(&b, "vtproto_src_paths", target.VTProtoSrcs)
		}
		b.WriteString("    visibility = [\"PUBLIC\"],\n")
		b.WriteString(")\n\n")
	}
	return b.String()
}

func protoCompilerTargets(protoTargets []*goModProtoTarget) []*goModProtoCompiler {
	if len(protoTargets) == 0 {
		return nil
	}
	needGRPC := false
	needVTProto := false
	for _, target := range protoTargets {
		needGRPC = needGRPC || target.GRPC
		needVTProto = needVTProto || target.VTProto
	}
	compilers := []*goModProtoCompiler{{
		Name:       "go_proto",
		PluginName: "go",
		Plugin:     "//third_party/go:protoc-gen-go",
		Suffix:     ".pb.go",
	}}
	if needGRPC {
		compilers = append(compilers, &goModProtoCompiler{
			Name:       "go_grpc_v2",
			PluginName: "go-grpc",
			Plugin:     "//third_party/go:protoc-gen-go-grpc",
			Suffix:     "_grpc.pb.go",
			Options:    []string{"require_unimplemented_servers=false"},
		})
	}
	if needVTProto {
		compilers = append(compilers, &goModProtoCompiler{
			Name:       "go_vtproto",
			PluginName: "go-vtproto",
			Plugin:     "//third_party/go:protoc-gen-go-vtproto",
			Suffix:     "_vtproto.pb.go",
			Options:    []string{"features=marshal+unmarshal+size+pool+clone"},
		})
	}
	return compilers
}

func protoCompilersForTarget(target *goModProtoTarget) []string {
	compilers := []string{":go_proto"}
	if target.GRPC {
		compilers = append(compilers, ":go_grpc_v2")
	}
	if target.VTProto {
		compilers = append(compilers, ":go_vtproto")
	}
	return compilers
}

func hasGeneratedEmbeds(targets []*goModTarget) bool {
	for _, target := range targets {
		if len(target.GeneratedEmbedSrcs) > 0 {
			return true
		}
	}
	return false
}

func protoArchives(protoTargets []*goModProtoTarget) []*goModArchive {
	seen := map[string]bool{}
	var archives []*goModArchive
	for _, target := range protoTargets {
		for _, archive := range target.Archives {
			if archive == nil || seen[archive.Name] {
				continue
			}
			seen[archive.Name] = true
			archives = append(archives, archive)
		}
	}
	sort.Slice(archives, func(i, j int) bool { return archives[i].Name < archives[j].Name })
	return archives
}

func localExportedProtoFiles(protoTargets []*goModProtoTarget) []string {
	seen := map[string]bool{}
	var files []string
	for _, target := range protoTargets {
		if len(target.SrcLabels) != 0 {
			continue
		}
		for _, src := range localProtoFiles(target.Dir, target.Srcs) {
			if strings.HasPrefix(src, ":") || strings.HasPrefix(src, "//") || seen[src] {
				continue
			}
			seen[src] = true
			files = append(files, src)
		}
	}
	sort.Strings(files)
	return files
}

func protoSourceLabels(dir string, paths []string, labels []string) []string {
	if len(labels) != 0 {
		if len(labels) != len(paths) {
			panic(fmt.Sprintf("proto source labels length (%d) must match paths length (%d)", len(labels), len(paths)))
		}
		resolved := append([]string{}, labels...)
		for i, label := range resolved {
			if label == "" {
				resolved[i] = localProtoFile(dir, paths[i])
			}
		}
		return resolved
	}
	return localProtoFiles(dir, paths)
}

func writeProtoSourceLists(b *strings.Builder, srcsName, pathsName, dir string, paths []string, labels []string) {
	if len(paths) == 0 {
		return
	}
	resolvedLabels := protoSourceLabels(dir, paths, labels)
	type protoSource struct {
		label string
		path  string
	}
	sources := make([]protoSource, 0, len(paths))
	for i, path := range paths {
		sources = append(sources, protoSource{
			label: resolvedLabels[i],
			path:  path,
		})
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].path == sources[j].path {
			return sources[i].label < sources[j].label
		}
		return sources[i].path < sources[j].path
	})
	sortedLabels := make([]string, 0, len(sources))
	sortedPaths := make([]string, 0, len(sources))
	for _, source := range sources {
		sortedLabels = append(sortedLabels, source.label)
		sortedPaths = append(sortedPaths, source.path)
	}
	writeStringListPreserveOrder(b, srcsName, sortedLabels)
	writeStringListPreserveOrder(b, pathsName, sortedPaths)
}

func localProtoFiles(dir string, values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, localProtoFile(dir, value))
	}
	return result
}

func localProtoFile(dir, value string) string {
	prefix := dir + "/"
	if strings.HasPrefix(value, ":") || strings.HasPrefix(value, "//") {
		return value
	}
	fileDir := filepath.ToSlash(filepath.Dir(value))
	if fileDir == "." {
		fileDir = ""
	}
	if fileDir == dir {
		return strings.TrimPrefix(value, prefix)
	}
	base := filepath.Base(value)
	if fileDir == "" {
		return "//:" + base
	}
	return "//" + fileDir + ":" + base
}

func sortedStringKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func writeEmbedSrcs(b *strings.Builder, target *goModTarget) {
	if len(target.GeneratedEmbedSrcs) == 0 {
		writeStringList(b, "embed_srcs", target.EmbedSrcs)
		return
	}
	values := map[string]string{}
	for _, src := range target.EmbedSrcs {
		values[src] = src
	}
	for name, genruleName := range target.GeneratedEmbedSrcs {
		values[name] = ":" + genruleName
	}
	writeStringDict(b, "embed_srcs", values)
}

func writeStringList(b *strings.Builder, name string, values []string) {
	if len(values) == 0 {
		return
	}
	sort.Strings(values)
	writeStringListPreserveOrder(b, name, values)
}

func writeStringListPreserveOrder(b *strings.Builder, name string, values []string) {
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(b, "    %s = [\n", name)
	for _, value := range values {
		fmt.Fprintf(b, "        %q,\n", value)
	}
	b.WriteString("    ],\n")
}

func writeStringDict(b *strings.Builder, name string, values map[string]string) {
	if len(values) == 0 {
		return
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fmt.Fprintf(b, "    %s = {\n", name)
	for _, key := range keys {
		fmt.Fprintf(b, "        %q: %q,\n", key, values[key])
	}
	b.WriteString("    },\n")
}

func sortedPackages(pkgs map[string]*GoModPackage) []*GoModPackage {
	keys := make([]string, 0, len(pkgs))
	for key := range pkgs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*GoModPackage, 0, len(keys))
	for _, key := range keys {
		result = append(result, pkgs[key])
	}
	return result
}

func existingRelativeFiles(dir string, files []string) []string {
	var result []string
	for _, file := range files {
		if file == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, file)); err == nil {
			result = append(result, file)
		}
	}
	sort.Strings(result)
	return result
}

func embedFilesForPackage(pkg *GoModPackage) []string {
	seen := map[string]bool{}
	var files []string
	for _, file := range existingRelativeFiles(pkg.Dir, pkg.EmbedFiles) {
		seen[file] = true
		files = append(files, file)
	}
	for _, pattern := range allEmbedPatterns(pkg) {
		if pattern == "" || strings.ContainsAny(pattern, "*?[") || seen[pattern] {
			continue
		}
		if _, err := os.Stat(filepath.Join(pkg.Dir, pattern)); err == nil {
			seen[pattern] = true
			files = append(files, pattern)
		}
	}
	sort.Strings(files)
	return files
}

func missingLiteralEmbeds(pkg *GoModPackage) map[string]string {
	generated := map[string]string{}
	for _, pattern := range allEmbedPatterns(pkg) {
		if pattern == "" || strings.ContainsAny(pattern, "*?[") {
			continue
		}
		if _, err := os.Stat(filepath.Join(pkg.Dir, pattern)); err == nil {
			continue
		}
		generated[pattern] = sanitizeBuckName(rootTargetName(pkg.ImportPath) + "_" + pattern + "_embed")
	}
	if len(generated) == 0 {
		return nil
	}
	return generated
}

func allEmbedPatterns(pkg *GoModPackage) []string {
	seen := map[string]bool{}
	var patterns []string
	for _, pattern := range pkg.EmbedPatterns {
		if !seen[pattern] {
			seen[pattern] = true
			patterns = append(patterns, pattern)
		}
	}
	for _, pattern := range parsedGoEmbedPatterns(pkg) {
		if !seen[pattern] {
			seen[pattern] = true
			patterns = append(patterns, pattern)
		}
	}
	return patterns
}

func parsedGoEmbedPatterns(pkg *GoModPackage) []string {
	re := regexp.MustCompile("(?m)^\\s*//go:embed\\s+(.+)$")
	var patterns []string
	for _, src := range append(pkg.GoFiles, pkg.CgoFiles...) {
		content, err := os.ReadFile(filepath.Join(pkg.Dir, src))
		if err != nil {
			continue
		}
		for _, match := range re.FindAllStringSubmatch(string(content), -1) {
			for _, field := range strings.Fields(match[1]) {
				field = strings.Trim(field, "\\\"")
				if field != "" {
					patterns = append(patterns, field)
				}
			}
		}
	}
	return patterns
}

func appendIfMissing(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

var invalidBuckNameChar = regexp.MustCompile("[^A-Za-z0-9_.-]+")

func sanitizeBuckName(value string) string {
	value = invalidBuckNameChar.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "target"
	}
	return value
}

func escapeModulePath(path string) string {
	var b strings.Builder
	for _, r := range path {
		if r >= 'A' && r <= 'Z' {
			b.WriteByte('!')
			b.WriteRune(r + ('a' - 'A'))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func shortHash(value string) string {
	var h uint32 = 2166136261
	for _, c := range []byte(value) {
		h ^= uint32(c)
		h *= 16777619
	}
	return fmt.Sprintf("%08x", h)
}
