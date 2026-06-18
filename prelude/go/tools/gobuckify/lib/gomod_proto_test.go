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
	"testing"
)

func TestScanGoProtoTargetsSingleQuotedAttrsAndComments(t *testing.T) {
	dir := t.TempDir()
	build := "# go_proto_library(name = \"commented\", importpath = \"bad\", proto = \":missing\")\n" +
		"proto_library(\n" +
		"    name = 'api_proto',\n" +
		"    srcs = ['api.proto'],\n" +
		")\n\n" +
		"go_proto_library(\n" +
		"    name = 'api_go_proto',\n" +
		"    import_path = 'example.com/root/api',\n" +
		"    proto = ':api_proto',\n" +
		")\n"
	if err := os.WriteFile(filepath.Join(dir, "BUILD.bazel"), []byte(build), 0644); err != nil {
		t.Fatal(err)
	}
	proto := "syntax = \"proto3\";\npackage api;\nmessage Request { string name = 1; }\n"
	if err := os.WriteFile(filepath.Join(dir, "api.proto"), []byte(proto), 0644); err != nil {
		t.Fatal(err)
	}

	scan, err := scanGoProtoTargets(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Targets) != 1 {
		t.Fatalf("targets = %#v", scan.Targets)
	}
	target := scan.Targets[0]
	if target.Name != "api_go_proto" || target.ImportPath != "example.com/root/api" {
		t.Fatalf("target = %#v", target)
	}
	if len(target.Srcs) != 1 || target.Srcs[0] != "api.proto" {
		t.Fatalf("srcs = %#v", target.Srcs)
	}
	if !target.VTProto {
		t.Fatalf("VTProto = false, want true")
	}
}

func TestScanGoProtoTargetsStarlarkListsAndAliases(t *testing.T) {
	dir := t.TempDir()
	build := "proto_library(\n" +
		"    name = \"common_proto\",\n" +
		"    srcs = [\"common.proto\"],\n" +
		")\n\n" +
		"alias(\n" +
		"    name = \"common_alias\",\n" +
		"    actual = \":common_proto\",\n" +
		")\n\n" +
		"proto_library(\n" +
		"    name = \"api_proto\",\n" +
		"    srcs = [\"api.proto\"] + [\"extra.proto\"],\n" +
		"    deps = [\":common_alias\"],\n" +
		")\n\n" +
		"alias(\n" +
		"    name = \"api_alias\",\n" +
		"    actual = \":api_proto\",\n" +
		")\n\n" +
		"go_proto_library(\n" +
		"    name = \"api_go_proto\",\n" +
		"    importpath = \"example.com/root/api#v1\",\n" +
		"    proto = \":api_alias\",\n" +
		")\n"
	if err := os.WriteFile(filepath.Join(dir, "BUILD.bazel"), []byte(build), 0644); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"api.proto":    "syntax = \"proto3\";\npackage api;\nimport \"common.proto\";\nmessage Request { string name = 1; }\n",
		"common.proto": "syntax = \"proto3\";\npackage api;\nmessage Common { string name = 1; }\n",
		"extra.proto":  "syntax = \"proto3\";\npackage api;\nmessage Extra { string name = 1; }\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	scan, err := scanGoProtoTargets(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Targets) != 1 {
		t.Fatalf("targets = %#v", scan.Targets)
	}
	target := scan.Targets[0]
	if target.ImportPath != "example.com/root/api#v1" {
		t.Fatalf("ImportPath = %q", target.ImportPath)
	}
	if got, want := target.Srcs, []string{"api.proto", "extra.proto"}; !sameStrings(got, want) {
		t.Fatalf("Srcs = %#v, want %#v", got, want)
	}
	if got, want := target.ProtoSrcs, []string{"api.proto", "common.proto", "extra.proto"}; !sameStrings(got, want) {
		t.Fatalf("ProtoSrcs = %#v, want %#v", got, want)
	}
}

func sameStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestScanGoProtoTargetsIgnoresStringsAndPrefixedNames(t *testing.T) {
	dir := t.TempDir()
	build := "proto_library(\n" +
		"    name = 'api_proto',\n" +
		"    srcs = ['api.proto'],\n" +
		")\n\n" +
		"custom_go_proto_library(\n" +
		"    name = 'bad_go_proto',\n" +
		"    import_path = 'example.com/root/bad',\n" +
		"    proto = ':api_proto',\n" +
		")\n\n" +
		"text = \"go_proto_library(name = 'bad_string', import_path = 'example.com/root/string', proto = ':api_proto')\"\n\n" +
		"go_proto_library (\n" +
		"    name = 'api_go_proto',\n" +
		"    import_path = 'example.com/root/api',\n" +
		"    proto = ':api_proto',\n" +
		")\n"
	if err := os.WriteFile(filepath.Join(dir, "BUILD.bazel"), []byte(build), 0644); err != nil {
		t.Fatal(err)
	}
	proto := "syntax = \"proto3\";\npackage api;\nmessage Request { string name = 1; }\n"
	if err := os.WriteFile(filepath.Join(dir, "api.proto"), []byte(proto), 0644); err != nil {
		t.Fatal(err)
	}

	scan, err := scanGoProtoTargets(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Targets) != 1 {
		t.Fatalf("targets = %#v", scan.Targets)
	}
	target := scan.Targets[0]
	if target.Name != "api_go_proto" || target.ImportPath != "example.com/root/api" {
		t.Fatalf("target = %#v", target)
	}
}
