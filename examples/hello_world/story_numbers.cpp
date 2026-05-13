/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory.
 * You may select, at your option, one of the above-listed licenses.
 */

#include "story_numbers.hpp"

int doubled_checksum(std::string_view text) {
  int checksum = 0;
  for (char ch : text) {
    checksum += static_cast<unsigned char>(ch);
  }
  return checksum * 2;
}
