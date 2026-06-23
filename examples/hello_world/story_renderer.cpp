/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory.
 * You may select, at your option, one of the above-listed licenses.
 */

#include "story_renderer.hpp"

#include "story_messages.hpp"

std::string render_story(std::string_view subject) {
  return "BuildBuddy view: " + build_story_message(subject);
}
