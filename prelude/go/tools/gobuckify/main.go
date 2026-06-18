/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	lib "go/tools/gobuckify/lib"
)

func main() {
	if len(os.Args[1:]) < 1 {
		fmt.Println("Usage: gobuckify <path-third-party-dir> | gobuckify gomod <module-dir> [patterns...]")
		os.Exit(1)
	}

	if os.Args[1] == "gomod" {
		if len(os.Args[2:]) < 1 {
			fmt.Println("Usage: gobuckify gomod <module-dir> [patterns...]")
			os.Exit(1)
		}
		moduleDir := os.Args[2]
		patterns := os.Args[3:]
		if err := lib.GenerateGoModBuckFiles(moduleDir, patterns); err != nil {
			slog.Error("Error generating gomod BUCK files", "err", err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args[1:]) != 1 {
		fmt.Println("Usage: gobuckify <path-third-party-dir> | gobuckify gomod <module-dir> [patterns...]")
		os.Exit(1)
	}

	thirdPartyDir := os.Args[1]

	goMod := filepath.Join(thirdPartyDir, "go.mod")
	rootModuleName, err := lib.ReadModuleName(goMod)
	if err != nil {
		slog.Error("Error reading go.mod", "err", err)
		os.Exit(1)
	}

	cfg, err := lib.FromJSON(filepath.Join(thirdPartyDir, "gobuckify.json"))
	if err != nil {
		slog.Error("Error reading config gobuckify.json", "err", err)
		os.Exit(1)
	}

	result, err := lib.CollectPackages(cfg, thirdPartyDir, rootModuleName)
	if err != nil {
		slog.Error("Error collecting packages", "err", err)
		os.Exit(1)
	}

	slog.Info("Rendering BUCK files")
	if err := lib.RenderBuckFiles(cfg, thirdPartyDir, result.BuckTargets); err != nil {
		slog.Error("Error rendering BUCK files", "err", err)
		os.Exit(1)
	}
}
