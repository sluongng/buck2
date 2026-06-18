/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

package external_test

import (
	_ "embed"
	"os"
	"strings"
	"testing"

	external "tests/prelude/go/go_test/external"
)

//go:embed external_fixture.txt
var fixture string

var testMainRan bool

func TestMain(m *testing.M) {
	testMainRan = true
	os.Exit(m.Run())
}

func TestExternalPackage(t *testing.T) {
	if !testMainRan {
		t.Fatalf("external TestMain was not called")
	}
	if got := external.Greeting(); got != "hello world" {
		t.Fatalf("Greeting() = %q", got)
	}
	if got := strings.TrimSpace(fixture); got != "external fixture" {
		t.Fatalf("fixture = %q", got)
	}
}
