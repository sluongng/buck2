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
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	protocGenGoImport        = "google.golang.org/protobuf/cmd/protoc-gen-go"
	protocGenGoGRPCImport    = "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	protocGenGoVTProtoImport = "github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto"
)

var protoRuntimeImports = []string{
	"google.golang.org/protobuf/proto",
	"google.golang.org/protobuf/reflect/protoreflect",
	"google.golang.org/protobuf/runtime/protoimpl",
}

var grpcRuntimeImports = []string{
	"google.golang.org/grpc",
	"google.golang.org/grpc/codes",
	"google.golang.org/grpc/status",
}

var vtprotoRuntimeImports = []string{
	"github.com/planetscale/vtprotobuf/protohelpers",
	"github.com/planetscale/vtprotobuf/types/known/anypb",
	"github.com/planetscale/vtprotobuf/types/known/durationpb",
	"github.com/planetscale/vtprotobuf/types/known/emptypb",
	"github.com/planetscale/vtprotobuf/types/known/fieldmaskpb",
	"github.com/planetscale/vtprotobuf/types/known/structpb",
	"github.com/planetscale/vtprotobuf/types/known/timestamppb",
	"github.com/planetscale/vtprotobuf/types/known/wrapperspb",
}

var wellKnownProtoMappings = map[string]string{
	"github.com/planetscale/vtprotobuf/vtproto/ext.proto": "github.com/planetscale/vtprotobuf/vtproto",
	"google/api/annotations.proto":                        "google.golang.org/genproto/googleapis/api/annotations",
	"google/api/client.proto":                             "google.golang.org/genproto/googleapis/api/annotations",
	"google/api/field_behavior.proto":                     "google.golang.org/genproto/googleapis/api/annotations",
	"google/bytestream/bytestream.proto":                  "google.golang.org/genproto/googleapis/bytestream",
	"google/longrunning/operations.proto":                 "google.golang.org/genproto/googleapis/longrunning",
	"google/protobuf/any.proto":                           "google.golang.org/protobuf/types/known/anypb",
	"google/protobuf/descriptor.proto":                    "google.golang.org/protobuf/types/descriptorpb",
	"google/protobuf/duration.proto":                      "google.golang.org/protobuf/types/known/durationpb",
	"google/protobuf/empty.proto":                         "google.golang.org/protobuf/types/known/emptypb",
	"google/protobuf/field_mask.proto":                    "google.golang.org/protobuf/types/known/fieldmaskpb",
	"google/protobuf/struct.proto":                        "google.golang.org/protobuf/types/known/structpb",
	"google/protobuf/timestamp.proto":                     "google.golang.org/protobuf/types/known/timestamppb",
	"google/protobuf/wrappers.proto":                      "google.golang.org/protobuf/types/known/wrapperspb",
	"google/rpc/status.proto":                             "google.golang.org/genproto/googleapis/rpc/status",
	"io/prometheus/client/metrics.proto":                  "github.com/prometheus/client_model/go",
	"kythe/proto/analysis.proto":                          "kythe.io/kythe/proto/analysis_go_proto",
	"kythe/proto/common.proto":                            "kythe.io/kythe/proto/common_go_proto",
	"kythe/proto/explore.proto":                           "kythe.io/kythe/proto/explore_go_proto",
	"kythe/proto/filetree.proto":                          "kythe.io/kythe/proto/filetree_go_proto",
	"kythe/proto/graph.proto":                             "kythe.io/kythe/proto/graph_go_proto",
	"kythe/proto/identifier.proto":                        "kythe.io/kythe/proto/identifier_go_proto",
	"kythe/proto/link.proto":                              "kythe.io/kythe/proto/link_go_proto",
	"kythe/proto/status_service.proto":                    "kythe.io/kythe/proto/status_service_go_proto",
	"kythe/proto/storage.proto":                           "kythe.io/kythe/proto/storage_go_proto",
	"kythe/proto/xref.proto":                              "kythe.io/kythe/proto/xref_go_proto",
	"validate/validate.proto":                             "github.com/envoyproxy/protoc-gen-validate/validate",
}

type externalProtoSource struct {
	ModulePath  string
	SourcePath  string
	ArchiveName string
	Archive     *goModArchive
}

var externalProtoSources = map[string]externalProtoSource{
	"github.com/planetscale/vtprotobuf/vtproto/ext.proto": {
		ModulePath: "github.com/planetscale/vtprotobuf",
		SourcePath: "include/github.com/planetscale/vtprotobuf/vtproto/ext.proto",
	},
	"io/prometheus/client/metrics.proto": {
		ModulePath: "github.com/prometheus/client_model",
		SourcePath: "io/prometheus/client/metrics.proto",
	},
	"validate/validate.proto": {
		ModulePath: "github.com/envoyproxy/protoc-gen-validate",
		SourcePath: "validate/validate.proto",
	},
}

var googleapisProtoSource = externalProtoSource{
	Archive: &goModArchive{
		Name:        "googleapis_proto",
		Out:         "googleapis_proto",
		URL:         "https://github.com/googleapis/googleapis/archive/27ffde22e73154260f8eaa6115420b7bd895db2f.zip",
		SHA256:      "1dc5a09537d5cc679d1227e8598bc270d62c1877f3c5f0aa7f8e46c925fc864d",
		StripPrefix: "googleapis-27ffde22e73154260f8eaa6115420b7bd895db2f",
		Type:        "zip",
	},
}

var googleapisProtoFiles = []string{
	"google/api/annotations.proto",
	"google/api/client.proto",
	"google/api/field_behavior.proto",
	"google/api/http.proto",
	"google/api/launch_stage.proto",
	"google/bytestream/bytestream.proto",
	"google/longrunning/operations.proto",
	"google/rpc/status.proto",
}

func init() {
	for _, protoPath := range googleapisProtoFiles {
		source := googleapisProtoSource
		source.SourcePath = protoPath
		externalProtoSources[protoPath] = source
	}
	for protoPath := range wellKnownProtoMappings {
		if strings.HasPrefix(protoPath, "kythe/proto/") {
			externalProtoSources[protoPath] = externalProtoSource{
				ModulePath: "github.com/buildbuddy-io/kythe",
				SourcePath: protoPath,
			}
		}
	}
}

var externalProtoDeps = map[string][]string{
	"google/api/annotations.proto": {
		"google/api/http.proto",
	},
	"google/api/client.proto": {
		"google/api/launch_stage.proto",
	},
	"google/bytestream/bytestream.proto": {
		"google/api/annotations.proto",
		"google/api/client.proto",
		"google/api/field_behavior.proto",
	},
	"google/longrunning/operations.proto": {
		"google/api/annotations.proto",
		"google/api/client.proto",
		"google/api/field_behavior.proto",
		"google/rpc/status.proto",
	},
}

type goModProtoTarget struct {
	Name           string
	Dir            string
	ImportPath     string
	SrcLabels      []string
	Srcs           []string
	ProtoSrcLabels []string
	ProtoSrcs      []string
	ImportMappings map[string]string
	DepImportPaths []string
	Deps           []string
	Archives       []*goModArchive
	GRPC           bool
	GRPCSrcs       []string
	VTProto        bool
	VTProtoSrcs    []string
}

type parsedProtoLibrary struct {
	Name string
	Dir  string
	Srcs []string
	Deps []string
}

type parsedGoProtoLibrary struct {
	Name       string
	Dir        string
	ImportPath string
	Proto      string
	Deps       []string
	Mode       string
	Compilers  []string
}

type goProtoScan struct {
	Targets             []*goModProtoTarget
	ToolPatterns        []string
	ExtraPackageImports []string
	NeedGRPCTool        bool
}

func scanGoProtoTargets(rootDir string) (*goProtoScan, error) {
	protoLibraries := map[string]*parsedProtoLibrary{}
	goProtoLibraries := map[string]*parsedGoProtoLibrary{}
	aliases := map[string]string{}
	var buildFiles []string
	if err := filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && shouldSkipProtoScanDir(rootDir, path, d.Name()) {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		base := d.Name()
		if base == "BUILD" || base == "BUILD.bazel" {
			buildFiles = append(buildFiles, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(buildFiles)

	for _, buildFile := range buildFiles {
		content, err := os.ReadFile(buildFile)
		if err != nil {
			return nil, err
		}
		buildContent := stripStarlarkLineComments(string(content))
		relDir, err := filepath.Rel(rootDir, filepath.Dir(buildFile))
		if err != nil {
			return nil, err
		}
		if relDir == "." {
			relDir = ""
		}
		relDir = filepath.ToSlash(relDir)
		for _, block := range extractCallBlocks(buildContent, "proto_library") {
			lib := &parsedProtoLibrary{
				Name: parseStringAttr(block, "name"),
				Dir:  relDir,
				Srcs: parseStringListAttr(block, "srcs"),
				Deps: parseStringListAttr(block, "deps"),
			}
			if lib.Name != "" {
				protoLibraries[protoKey(relDir, lib.Name)] = lib
			}
		}
		for _, block := range extractCallBlocks(buildContent, "go_proto_library") {
			lib := &parsedGoProtoLibrary{
				Name:       parseStringAttr(block, "name"),
				Dir:        relDir,
				ImportPath: firstNonEmpty(parseStringAttr(block, "importpath"), parseStringAttr(block, "import_path")),
				Proto:      parseStringAttr(block, "proto"),
				Deps:       parseStringListAttr(block, "deps"),
				Mode:       parseStringAttr(block, "mode"),
				Compilers:  parseStringListAttr(block, "compilers"),
			}
			if lib.Name != "" && lib.ImportPath != "" && lib.Proto != "" {
				goProtoLibraries[protoKey(relDir, lib.Name)] = lib
			}
		}
		for _, block := range extractCallBlocks(buildContent, "alias") {
			name := parseStringAttr(block, "name")
			actual := parseStringAttr(block, "actual")
			if name != "" && actual != "" {
				aliases[protoKey(relDir, name)] = actual
			}
		}
	}

	if len(goProtoLibraries) == 0 {
		return &goProtoScan{}, nil
	}

	protoImportMappings := map[string]string{}
	for _, goProto := range goProtoLibraries {
		protoLib := protoLibraries[resolveLocalLabelWithAliases(goProto.Dir, goProto.Proto, aliases)]
		if protoLib == nil {
			continue
		}
		for _, src := range protoLib.Srcs {
			protoImportMappings[relFile(protoLib.Dir, src)] = goProto.ImportPath
		}
	}
	for protoPath, importPath := range wellKnownProtoMappings {
		protoImportMappings[protoPath] = importPath
	}

	var targets []*goModProtoTarget
	extraImports := stringSet{}
	needGRPC := false
	needVTProto := false
	for _, key := range sortedGoProtoKeys(goProtoLibraries) {
		goProto := goProtoLibraries[key]
		protoLib := protoLibraries[resolveLocalLabelWithAliases(goProto.Dir, goProto.Proto, aliases)]
		if protoLib == nil {
			if target := knownExternalProtoTarget(goProto, resolveAliasActual(goProto.Dir, goProto.Proto, aliases)); target != nil {
				needGRPC = needGRPC || target.GRPC
				needVTProto = needVTProto || target.VTProto
				target.ImportMappings = protoImportMappings
				for protoPath, importPath := range targetSpecificImportMappings(target) {
					target.ImportMappings[protoPath] = importPath
				}
				for _, dep := range target.DepImportPaths {
					extraImports.Add(dep)
				}
				targets = append(targets, target)
			}
			continue
		}
		protoSrcs := collectProtoSrcs(protoLib, protoLibraries, aliases)
		directSrcs := relFiles(protoLib.Dir, protoLib.Srcs)
		grpcSrcs := protoFilesMatching(rootDir, directSrcs, regexp.MustCompile("(?m)^\\s*service\\s+[A-Za-z_]"))
		grpc := goProtoWantsGRPC(goProto) && len(grpcSrcs) > 0
		vtprotoSrcs := protoFilesMatching(rootDir, directSrcs, regexp.MustCompile("(?m)^\\s*message\\s+[A-Za-z_]"))
		vtproto := goProto.Mode != "services_only" && len(vtprotoSrcs) > 0
		needGRPC = needGRPC || grpc
		needVTProto = needVTProto || vtproto
		depSet := stringSet{}
		for _, dep := range depImportsForGoProto(rootDir, goProto, protoLib, protoLibraries, goProtoLibraries, protoImportMappings, aliases) {
			depSet.Add(dep)
		}
		if vtproto {
			for _, dep := range vtprotoRuntimeImports {
				depSet.Add(dep)
			}
		}
		deps := depSet.Sorted()
		for _, dep := range deps {
			extraImports.Add(dep)
		}
		for _, dep := range protoRuntimeImports {
			extraImports.Add(dep)
		}
		if grpc {
			for _, dep := range grpcRuntimeImports {
				extraImports.Add(dep)
			}
		}
		if vtproto {
			for _, dep := range vtprotoRuntimeImports {
				extraImports.Add(dep)
			}
		}
		targets = append(targets, &goModProtoTarget{
			Name:           sanitizeBuckName(goProto.Name),
			Dir:            goProto.Dir,
			ImportPath:     goProto.ImportPath,
			Srcs:           directSrcs,
			ProtoSrcs:      protoSrcs,
			ImportMappings: protoImportMappings,
			DepImportPaths: deps,
			GRPC:           grpc,
			GRPCSrcs:       grpcSrcs,
			VTProto:        vtproto,
			VTProtoSrcs:    vtprotoSrcs,
		})
	}

	toolPatterns := []string{protocGenGoImport}
	if needVTProto {
		toolPatterns = append(toolPatterns, protocGenGoVTProtoImport)
	}
	return &goProtoScan{
		Targets:             targets,
		ToolPatterns:        toolPatterns,
		ExtraPackageImports: extraImports.Sorted(),
		NeedGRPCTool:        needGRPC,
	}, nil
}

func shouldSkipProtoScanDir(rootDir, path, name string) bool {
	if path == rootDir {
		return false
	}
	switch name {
	case ".git", "buck-out", "node_modules", "third_party", "vendor":
		return true
	}
	return strings.HasPrefix(name, "bazel-")
}

func sortedGoProtoKeys(m map[string]*parsedGoProtoLibrary) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func goProtoWantsGRPC(goProto *parsedGoProtoLibrary) bool {
	if goProto.Mode == "services_only" || goProto.Mode == "messages_and_services" {
		return true
	}
	for _, compiler := range goProto.Compilers {
		if strings.Contains(compiler, "go_grpc") {
			return true
		}
	}
	return false
}

func collectProtoSrcs(root *parsedProtoLibrary, libs map[string]*parsedProtoLibrary, aliases map[string]string) []string {
	seenLibs := map[string]bool{}
	seenFiles := stringSet{}
	var visit func(*parsedProtoLibrary)
	visit = func(lib *parsedProtoLibrary) {
		if lib == nil {
			return
		}
		key := protoKey(lib.Dir, lib.Name)
		if seenLibs[key] {
			return
		}
		seenLibs[key] = true
		for _, src := range lib.Srcs {
			seenFiles.Add(relFile(lib.Dir, src))
		}
		for _, dep := range lib.Deps {
			visit(libs[resolveLocalLabelWithAliases(lib.Dir, dep, aliases)])
		}
	}
	visit(root)
	return seenFiles.Sorted()
}

func depImportsForGoProto(rootDir string, goProto *parsedGoProtoLibrary, protoLib *parsedProtoLibrary, protoLibraries map[string]*parsedProtoLibrary, goProtoLibraries map[string]*parsedGoProtoLibrary, protoImportMappings map[string]string, aliases map[string]string) []string {
	deps := stringSet{}
	for _, dep := range goProto.Deps {
		key := resolveLocalLabelWithAliases(goProto.Dir, dep, aliases)
		if local := goProtoLibraries[key]; local != nil {
			deps.Add(local.ImportPath)
			continue
		}
		if importPath := externalBazelGoImport(dep); importPath != "" {
			deps.Add(importPath)
		}
	}
	for _, src := range collectProtoSrcs(protoLib, protoLibraries, aliases) {
		for _, importedProto := range parseProtoImports(filepath.Join(rootDir, filepath.FromSlash(src))) {
			if importPath, ok := protoImportMappings[importedProto]; ok && importPath != goProto.ImportPath {
				deps.Add(importPath)
			}
		}
	}
	for _, dep := range protoRuntimeImports {
		deps.Add(dep)
	}
	if goProtoWantsGRPC(goProto) {
		for _, dep := range grpcRuntimeImports {
			deps.Add(dep)
		}
	}
	return deps.Sorted()
}

func knownExternalProtoTarget(goProto *parsedGoProtoLibrary, actual string) *goModProtoTarget {
	if actual != "@bazel_worker_api//:worker_protocol_proto" {
		return nil
	}
	deps := stringSet{}
	for _, dep := range protoRuntimeImports {
		deps.Add(dep)
	}
	grpc := false
	if grpc {
		for _, dep := range grpcRuntimeImports {
			deps.Add(dep)
		}
	}
	for _, dep := range vtprotoRuntimeImports {
		deps.Add(dep)
	}
	return &goModProtoTarget{
		Name:           sanitizeBuckName(goProto.Name),
		Dir:            goProto.Dir,
		ImportPath:     goProto.ImportPath,
		SrcLabels:      []string{":bazel_worker_api[worker_protocol.proto]"},
		Srcs:           []string{"bazel_worker_api/worker_protocol.proto"},
		ProtoSrcLabels: []string{":bazel_worker_api[worker_protocol.proto]"},
		ProtoSrcs:      []string{"bazel_worker_api/worker_protocol.proto"},
		DepImportPaths: deps.Sorted(),
		Archives: []*goModArchive{{
			Name:        "bazel_worker_api",
			Out:         "bazel_worker_api",
			URL:         "https://github.com/bazelbuild/bazel-worker-api/releases/download/v0.0.10/bazel-worker-api-v0.0.10.tar.gz",
			SHA256:      "0476fe27251cd3234b69737f8bc231cfe9912becdd620e07e2d73c87bcc7e40a",
			StripPrefix: "bazel-worker-api-0.0.10/proto",
			SubTargets:  []string{"worker_protocol.proto"},
		}},
		GRPC:        grpc,
		VTProto:     true,
		VTProtoSrcs: []string{"bazel_worker_api/worker_protocol.proto"},
	}
}

func targetSpecificImportMappings(target *goModProtoTarget) map[string]string {
	if target.ImportPath == "github.com/buildbuddy-io/buildbuddy/proto/worker" {
		return map[string]string{
			"bazel_worker_api/worker_protocol.proto": target.ImportPath,
		}
	}
	return nil
}

func parseProtoImports(path string) []string {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	re := regexp.MustCompile("(?m)^\\s*import\\s+(?:public\\s+|weak\\s+)?\\\"([^\\\"]+)\\\"")
	matches := re.FindAllStringSubmatch(string(content), -1)
	imports := make([]string, 0, len(matches))
	for _, match := range matches {
		imports = append(imports, match[1])
	}
	return imports
}

func protoFilesHaveMessage(rootDir string, srcs []string) bool {
	return len(protoFilesMatching(rootDir, srcs, regexp.MustCompile("(?m)^\\s*message\\s+[A-Za-z_]"))) > 0
}

func protoFilesHaveService(rootDir string, srcs []string) bool {
	return len(protoFilesMatching(rootDir, srcs, regexp.MustCompile("(?m)^\\s*service\\s+[A-Za-z_]"))) > 0
}

func protoFilesMatching(rootDir string, srcs []string, re *regexp.Regexp) []string {
	var matching []string
	for _, src := range srcs {
		content, err := os.ReadFile(filepath.Join(rootDir, filepath.FromSlash(src)))
		if err == nil && re.Match(content) {
			matching = append(matching, src)
		}
	}
	return matching
}

func externalBazelGoImport(label string) string {
	if strings.HasPrefix(label, "@org_golang_google_genproto_googleapis_rpc//") {
		return "google.golang.org/genproto/googleapis/rpc/" + strings.TrimPrefix(label, "@org_golang_google_genproto_googleapis_rpc//")
	}
	if strings.HasPrefix(label, "@org_golang_google_genproto_googleapis_api//") {
		return "google.golang.org/genproto/googleapis/api/" + strings.TrimPrefix(label, "@org_golang_google_genproto_googleapis_api//")
	}
	if strings.HasPrefix(label, "@com_google_googleapis//google/") {
		pkg := strings.TrimPrefix(label, "@com_google_googleapis//google/")
		pkg = strings.TrimSuffix(pkg, "_proto")
		return "google.golang.org/genproto/googleapis/" + strings.ReplaceAll(pkg, ":", "/")
	}
	return ""
}

func relFiles(dir string, srcs []string) []string {
	result := make([]string, 0, len(srcs))
	for _, src := range srcs {
		result = append(result, relFile(dir, src))
	}
	sort.Strings(result)
	return result
}

func relFile(dir, src string) string {
	if dir == "" {
		return filepath.ToSlash(src)
	}
	return filepath.ToSlash(filepath.Join(dir, src))
}

func protoKey(dir, name string) string {
	if dir == "" {
		return ":" + name
	}
	return "//" + dir + ":" + name
}

func resolveLocalLabel(currentDir, label string) string {
	if strings.HasPrefix(label, ":") {
		return protoKey(currentDir, strings.TrimPrefix(label, ":"))
	}
	if strings.HasPrefix(label, "//") && !strings.HasPrefix(label, "@") {
		return label
	}
	return ""
}

func resolveLocalLabelWithAliases(currentDir, label string, aliases map[string]string) string {
	key := resolveLocalLabel(currentDir, label)
	seen := map[string]bool{}
	for key != "" && !seen[key] {
		seen[key] = true
		actual := aliases[key]
		if actual == "" {
			return key
		}
		next := resolveLocalLabel(protoKeyDir(key), actual)
		if next == "" {
			return key
		}
		key = next
	}
	return key
}

func resolveAliasActual(currentDir, label string, aliases map[string]string) string {
	key := resolveLocalLabel(currentDir, label)
	seen := map[string]bool{}
	for key != "" && !seen[key] {
		seen[key] = true
		actual := aliases[key]
		if actual == "" {
			return ""
		}
		if next := resolveLocalLabel(protoKeyDir(key), actual); next != "" {
			key = next
			continue
		}
		return actual
	}
	return ""
}

func protoKeyDir(key string) string {
	if strings.HasPrefix(key, "//") {
		pkgAndName := strings.TrimPrefix(key, "//")
		if idx := strings.LastIndex(pkgAndName, ":"); idx >= 0 {
			return pkgAndName[:idx]
		}
	}
	return ""
}

func protoTargetLabel(target *goModProtoTarget) string {
	if target.Dir == "" {
		return "//:" + target.Name
	}
	return "//" + target.Dir + ":" + target.Name
}

func extractCallBlocks(content, name string) []string {
	var blocks []string
	for i := 0; i < len(content); {
		if isStarlarkQuote(content[i]) {
			i = skipStarlarkStringLiteral(content, i)
			continue
		}
		if content[i] == '#' {
			for i < len(content) && content[i] != '\n' {
				i++
			}
			continue
		}
		if !callNameAt(content, name, i) {
			i++
			continue
		}
		open := i + len(name)
		for open < len(content) && isStarlarkWhitespace(content[open]) {
			open++
		}
		end := matchingParen(content, open)
		if end < 0 {
			break
		}
		blocks = append(blocks, content[open+1:end])
		i = end + 1
	}
	return blocks
}

func callNameAt(content, name string, index int) bool {
	if !strings.HasPrefix(content[index:], name) {
		return false
	}
	if index > 0 && (isStarlarkIdentifierChar(content[index-1]) || content[index-1] == '.') {
		return false
	}
	afterName := index + len(name)
	if afterName < len(content) && isStarlarkIdentifierChar(content[afterName]) {
		return false
	}
	for afterName < len(content) && isStarlarkWhitespace(content[afterName]) {
		afterName++
	}
	return afterName < len(content) && content[afterName] == '('
}

func isStarlarkIdentifierChar(c byte) bool {
	return c == '_' || ('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z')
}

func isStarlarkWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func matchingParen(content string, open int) int {
	return matchingStarlarkDelimiter(content, open, '(', ')')
}

func matchingStarlarkDelimiter(content string, open int, openChar byte, closeChar byte) int {
	depth := 0
	for i := open; i < len(content); i++ {
		c := content[i]
		if isStarlarkQuote(c) {
			i = skipStarlarkStringLiteral(content, i) - 1
			continue
		}
		if c == '#' {
			for i < len(content) && content[i] != '\n' {
				i++
			}
			continue
		}
		switch c {
		case openChar:
			depth++
		case closeChar:
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseStringAttr(block, name string) string {
	expr, ok := findStarlarkAttrExpr(block, name)
	if !ok {
		return ""
	}
	value, ok := parseStarlarkStringExpr(expr)
	if !ok {
		return ""
	}
	return value
}

func parseStringListAttr(block, name string) []string {
	expr, ok := findStarlarkAttrExpr(block, name)
	if !ok {
		return nil
	}
	return parseStarlarkStringListExpr(expr)
}

func findStarlarkAttrExpr(block, name string) (string, bool) {
	parenDepth := 0
	bracketDepth := 0
	braceDepth := 0
	for i := 0; i < len(block); i++ {
		c := block[i]
		if isStarlarkQuote(c) {
			i = skipStarlarkStringLiteral(block, i) - 1
			continue
		}
		if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 && strings.HasPrefix(block[i:], name) {
			nameEnd := i + len(name)
			if (i == 0 || !isStarlarkIdentifierChar(block[i-1])) && (nameEnd == len(block) || !isStarlarkIdentifierChar(block[nameEnd])) {
				j := nameEnd
				for j < len(block) && isStarlarkWhitespace(block[j]) {
					j++
				}
				if j < len(block) && block[j] == '=' {
					valueStart := j + 1
					for valueStart < len(block) && isStarlarkWhitespace(block[valueStart]) {
						valueStart++
					}
					valueEnd := findStarlarkExprEnd(block, valueStart)
					return strings.TrimSpace(block[valueStart:valueEnd]), true
				}
			}
		}
		switch c {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		}
	}
	return "", false
}

func findStarlarkExprEnd(content string, start int) int {
	parenDepth := 0
	bracketDepth := 0
	braceDepth := 0
	for i := start; i < len(content); i++ {
		c := content[i]
		if isStarlarkQuote(c) {
			i = skipStarlarkStringLiteral(content, i) - 1
			continue
		}
		switch c {
		case '(':
			parenDepth++
		case ')':
			if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
				return i
			}
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case ',':
			if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
				return i
			}
		}
	}
	return len(content)
}

func parseStarlarkStringExpr(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if len(expr) == 0 || !isStarlarkQuote(expr[0]) {
		return "", false
	}
	value, end, ok := parseStarlarkStringLiteral(expr, 0)
	if !ok || strings.TrimSpace(expr[end:]) != "" {
		return "", false
	}
	return value, true
}

func parseStarlarkStringListExpr(expr string) []string {
	var values []string
	for i := 0; i < len(expr); {
		for i < len(expr) && isStarlarkWhitespace(expr[i]) {
			i++
		}
		if i >= len(expr) || expr[i] != '[' {
			break
		}
		end := matchingStarlarkDelimiter(expr, i, '[', ']')
		if end < 0 {
			break
		}
		values = append(values, parseStarlarkStringListItems(expr[i+1:end])...)
		i = end + 1
		for i < len(expr) && isStarlarkWhitespace(expr[i]) {
			i++
		}
		if i >= len(expr) || expr[i] != '+' {
			break
		}
		i++
	}
	return values
}

func parseStarlarkStringListItems(content string) []string {
	var values []string
	parenDepth := 0
	bracketDepth := 0
	braceDepth := 0
	for i := 0; i < len(content); i++ {
		c := content[i]
		if isStarlarkQuote(c) {
			value, end, ok := parseStarlarkStringLiteral(content, i)
			if !ok {
				break
			}
			if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
				values = append(values, value)
			}
			i = end - 1
			continue
		}
		switch c {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		}
	}
	return values
}

func parseStarlarkStringLiteral(content string, start int) (string, int, bool) {
	if start >= len(content) || !isStarlarkQuote(content[start]) {
		return "", start, false
	}
	end := skipStarlarkStringLiteral(content, start)
	if end > len(content) {
		return "", start, false
	}
	quote := content[start]
	triple := start+2 < len(content) && content[start+1] == quote && content[start+2] == quote
	bodyStart := start + 1
	bodyEnd := end - 1
	if triple {
		bodyStart = start + 3
		bodyEnd = end - 3
	}
	return unescapeStarlarkString(content[bodyStart:bodyEnd]), end, true
}

func unescapeStarlarkString(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 == len(s) {
			out.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 't':
			out.WriteByte('\t')
		default:
			out.WriteByte(s[i])
		}
	}
	return out.String()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type stringSet map[string]bool

func (s stringSet) Add(value string) {
	if value != "" {
		s[value] = true
	}
}

func (s stringSet) Sorted() []string {
	values := make([]string, 0, len(s))
	for value := range s {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
