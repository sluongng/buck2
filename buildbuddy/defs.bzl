# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# Keep these image names in sync with buildbuddy-io/buildbuddy-toolchain.
BUILDBUDDY_UBUNTU16_04_IMAGE = "docker://gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"
BUILDBUDDY_UBUNTU20_04_IMAGE = "docker://gcr.io/flame-public/rbe-ubuntu20-04:latest"
BUILDBUDDY_UBUNTU22_04_IMAGE = "docker://gcr.io/flame-public/rbe-ubuntu22-04:latest"
BUILDBUDDY_UBUNTU24_04_IMAGE = "docker://gcr.io/flame-public/rbe-ubuntu24-04:latest"
BUILDBUDDY_DEFAULT_CONTAINER_IMAGE = BUILDBUDDY_UBUNTU24_04_IMAGE

def _configuration(ctx):
    constraints = dict()
    constraints.update(ctx.attrs.cpu_configuration[ConfigurationInfo].constraints)
    constraints.update(ctx.attrs.os_configuration[ConfigurationInfo].constraints)
    return ConfigurationInfo(constraints = constraints, values = {})

def _remote_execution_properties(ctx):
    properties = dict(ctx.attrs.remote_execution_properties)
    if ctx.attrs.os_family != None:
        properties["OSFamily"] = ctx.attrs.os_family
    if ctx.attrs.arch != None:
        properties["Arch"] = ctx.attrs.arch
    if ctx.attrs.container_image != None:
        properties["container-image"] = ctx.attrs.container_image
    if ctx.attrs.network != None:
        properties["network"] = ctx.attrs.network
    return properties

def _buildbuddy_remote_execution_platform_impl(ctx):
    configuration = _configuration(ctx)
    label = ctx.label.raw_target()
    platform = ExecutionPlatformInfo(
        label = label,
        configuration = configuration,
        executor_config = CommandExecutorConfig(
            local_enabled = ctx.attrs.local_enabled,
            remote_enabled = True,
            remote_cache_enabled = True,
            allow_cache_uploads = True,
            remote_execution_properties = _remote_execution_properties(ctx),
            remote_execution_use_case = "buck2-default",
            remote_output_paths = "output_paths",
            use_limited_hybrid = ctx.attrs.use_limited_hybrid,
            use_windows_path_separators = ctx.attrs.use_windows_path_separators,
        ),
    )

    return [
        DefaultInfo(),
        platform,
        PlatformInfo(label = str(label), configuration = configuration),
        ExecutionPlatformRegistrationInfo(platforms = [platform]),
    ]

buildbuddy_remote_execution_platform = rule(
    attrs = {
        "arch": attrs.option(attrs.string(), default = None),
        "container_image": attrs.option(attrs.string(), default = None),
        "cpu_configuration": attrs.dep(default = "prelude//cpu:x86_64", providers = [ConfigurationInfo]),
        "local_enabled": attrs.bool(default = False),
        "network": attrs.option(attrs.string(), default = None),
        "os_configuration": attrs.dep(default = "prelude//os:linux", providers = [ConfigurationInfo]),
        "os_family": attrs.option(attrs.string(), default = None),
        "remote_execution_properties": attrs.dict(key = attrs.string(), value = attrs.string(), default = {}),
        "use_limited_hybrid": attrs.bool(default = False),
        "use_windows_path_separators": attrs.bool(default = False),
    },
    impl = _buildbuddy_remote_execution_platform_impl,
)

def _buildbuddy_remote_execution_platforms_impl(ctx):
    return [
        DefaultInfo(),
        ExecutionPlatformRegistrationInfo(
            platforms = [platform[ExecutionPlatformInfo] for platform in ctx.attrs.platforms],
        ),
    ]

buildbuddy_remote_execution_platforms = rule(
    attrs = {
        "platforms": attrs.list(attrs.dep(providers = [ExecutionPlatformInfo])),
    },
    impl = _buildbuddy_remote_execution_platforms_impl,
)

def buildbuddy_linux_x86_64_rbe(name, visibility = None, container_image = BUILDBUDDY_DEFAULT_CONTAINER_IMAGE):
    kwargs = {}
    if visibility != None:
        kwargs["visibility"] = visibility
    buildbuddy_remote_execution_platform(
        name = name,
        arch = "amd64",
        container_image = container_image,
        cpu_configuration = "prelude//cpu:x86_64",
        network = "external",
        os_configuration = "prelude//os:linux",
        os_family = "Linux",
        **kwargs
    )
