# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _platforms(ctx):
    configuration = ConfigurationInfo(
        constraints = {},
        values = {},
    )

    # This can be any Docker image. Use a pinned digest for reproducibility.
    # This one is Ubuntu 24.04: https://hub.docker.com/_/ubuntu
    image = "docker://ubuntu@sha256:cd1dba651b3080c3686ecf4e3c4220f026b521fb76978881737d24f200828b2b"
    platform = ExecutionPlatformInfo(
        label = ctx.label.raw_target(),
        configuration = configuration,
        executor_config = CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            use_limited_hybrid = True,
            remote_execution_properties = {
                "OSFamily": "Linux",
                "container-image": image,
            },
            remote_execution_use_case = "buck2-default",
            remote_output_paths = "output_paths",
        ),
    )

    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [platform])]

platforms = rule(attrs = {}, impl = _platforms)
