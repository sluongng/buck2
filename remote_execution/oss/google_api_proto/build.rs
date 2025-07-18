use std::env;
use std::io;

fn main() -> io::Result<()> {
    let proto_files = &[
        "google/api/annotations.proto",
        "google/api/client.proto",
        "google/api/field_behavior.proto",
        "google/api/http.proto",
        "google/bytestream/bytestream.proto",
        "google/longrunning/operations.proto",
        "google/rpc/code.proto",
        "google/rpc/status.proto",
    ];

    let includes = if let Ok(path) = env::var("BUCK_PROTO_SRCS") {
        vec![path]
    } else {
        vec!["./proto/".to_owned()]
    };

    let builder = buck2_protoc_dev::configure();
    unsafe { builder.setup_protoc() }
        .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
        .field_attribute(
            "google.longrunning.Operation.metadata",
            "#[serde(with = \"crate::serialize_option_any\")]",
        )
        .field_attribute(
            "google.longrunning.Operation.result.response",
            "#[serde(with = \"crate::serialize_any\")]",
        )
        .field_attribute(
            "google.longrunning.WaitOperationRequest.timeout",
            "#[serde(with = \"::buck2_data::serialize_duration_as_micros\")]",
        )
        .field_attribute(
            "google.api.MethodSettings.LongRunning.initial_poll_delay",
            "#[serde(with = \"::buck2_data::serialize_duration_as_micros\")]",
        )
        .field_attribute(
            "google.api.MethodSettings.LongRunning.max_poll_delay",
            "#[serde(with = \"::buck2_data::serialize_duration_as_micros\")]",
        )
        .field_attribute(
            "google.api.MethodSettings.LongRunning.total_poll_timeout",
            "#[serde(with = \"::buck2_data::serialize_duration_as_micros\")]",
        )
        .field_attribute(
            "google.rpc.Status.details",
            "#[serde(with = \"crate::serialize_vec_any\")]",
        )
        .compile(proto_files, &includes)
}
