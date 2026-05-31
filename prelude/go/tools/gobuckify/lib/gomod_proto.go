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
	"fmt"
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
		relDir, err := filepath.Rel(rootDir, filepath.Dir(buildFile))
		if err != nil {
			return nil, err
		}
		if relDir == "." {
			relDir = ""
		}
		relDir = filepath.ToSlash(relDir)
		for _, block := range extractCallBlocks(string(content), "proto_library") {
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
		for _, block := range extractCallBlocks(string(content), "go_proto_library") {
			lib := &parsedGoProtoLibrary{
				Name:       parseStringAttr(block, "name"),
				Dir:        relDir,
				ImportPath: parseStringAttr(block, "importpath"),
				Proto:      parseStringAttr(block, "proto"),
				Deps:       parseStringListAttr(block, "deps"),
				Mode:       parseStringAttr(block, "mode"),
				Compilers:  parseStringListAttr(block, "compilers"),
			}
			if lib.Name != "" && lib.ImportPath != "" && lib.Proto != "" {
				goProtoLibraries[protoKey(relDir, lib.Name)] = lib
			}
		}
		for _, block := range extractCallBlocks(string(content), "alias") {
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
		protoLib := protoLibraries[resolveLocalLabel(goProto.Dir, goProto.Proto)]
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
		protoLib := protoLibraries[resolveLocalLabel(goProto.Dir, goProto.Proto)]
		if protoLib == nil {
			if target := knownExternalProtoTarget(goProto, aliases[resolveLocalLabel(goProto.Dir, goProto.Proto)]); target != nil {
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
		protoSrcs := collectProtoSrcs(protoLib, protoLibraries)
		directSrcs := relFiles(protoLib.Dir, protoLib.Srcs)
		grpcSrcs := protoFilesMatching(rootDir, directSrcs, regexp.MustCompile("(?m)^\\s*service\\s+[A-Za-z_]"))
		grpc := goProtoWantsGRPC(goProto) && len(grpcSrcs) > 0
		vtprotoSrcs := protoFilesMatching(rootDir, directSrcs, regexp.MustCompile("(?m)^\\s*message\\s+[A-Za-z_]"))
		vtproto := goProto.Mode != "services_only" && len(vtprotoSrcs) > 0
		needGRPC = needGRPC || grpc
		needVTProto = needVTProto || vtproto
		depSet := stringSet{}
		for _, dep := range depImportsForGoProto(rootDir, goProto, protoLib, protoLibraries, goProtoLibraries, protoImportMappings) {
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

func collectProtoSrcs(root *parsedProtoLibrary, libs map[string]*parsedProtoLibrary) []string {
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
			visit(libs[resolveLocalLabel(lib.Dir, dep)])
		}
	}
	visit(root)
	return seenFiles.Sorted()
}

func depImportsForGoProto(rootDir string, goProto *parsedGoProtoLibrary, protoLib *parsedProtoLibrary, protoLibraries map[string]*parsedProtoLibrary, goProtoLibraries map[string]*parsedGoProtoLibrary, protoImportMappings map[string]string) []string {
	deps := stringSet{}
	for _, dep := range goProto.Deps {
		key := resolveLocalLabel(goProto.Dir, dep)
		if local := goProtoLibraries[key]; local != nil {
			deps.Add(local.ImportPath)
			continue
		}
		if importPath := externalBazelGoImport(dep); importPath != "" {
			deps.Add(importPath)
		}
	}
	for _, src := range collectProtoSrcs(protoLib, protoLibraries) {
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

func protoTargetLabel(target *goModProtoTarget) string {
	if target.Dir == "" {
		return "//:" + target.Name
	}
	return "//" + target.Dir + ":" + target.Name
}

func extractCallBlocks(content, name string) []string {
	needle := name + "("
	var blocks []string
	for offset := 0; ; {
		idx := strings.Index(content[offset:], needle)
		if idx < 0 {
			break
		}
		start := offset + idx + len(name)
		end := matchingParen(content, start)
		if end < 0 {
			break
		}
		blocks = append(blocks, content[start+1:end])
		offset = end + 1
	}
	return blocks
}

func matchingParen(content string, open int) int {
	depth := 0
	inString := false
	escaped := false
	for i := open; i < len(content); i++ {
		c := content[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseStringAttr(block, name string) string {
	re := regexp.MustCompile(fmt.Sprintf("(?m)\\b%s\\s*=\\s*\\\"([^\\\"]*)\\\"", regexp.QuoteMeta(name)))
	match := re.FindStringSubmatch(block)
	if match == nil {
		return ""
	}
	return match[1]
}

func parseStringListAttr(block, name string) []string {
	re := regexp.MustCompile(fmt.Sprintf("(?s)\\b%s\\s*=\\s*\\[(.*?)\\]", regexp.QuoteMeta(name)))
	match := re.FindStringSubmatch(block)
	if match == nil {
		return nil
	}
	itemRe := regexp.MustCompile("\\\"([^\\\"]+)\\\"")
	items := itemRe.FindAllStringSubmatch(match[1], -1)
	values := make([]string, 0, len(items))
	for _, item := range items {
		values = append(values, item[1])
	}
	return values
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
