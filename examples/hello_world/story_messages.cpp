/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory.
 * You may select, at your option, one of the above-listed licenses.
 */

#include "story_messages.hpp"

#include "story_numbers.hpp"

#include <sstream>

std::string build_story_message(std::string_view subject) {
  std::ostringstream out;
  out << subject << " produced " << doubled_checksum(subject)
      << " diagnostic points";
  return out.str();
}
