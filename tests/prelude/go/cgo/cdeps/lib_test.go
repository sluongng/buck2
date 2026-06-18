/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

package cdeps

import "testing"

func TestValue(t *testing.T) {
	if got := Value(1); got != 2 {
		t.Fatalf("Value(1) = %d, want 2", got)
	}
}
