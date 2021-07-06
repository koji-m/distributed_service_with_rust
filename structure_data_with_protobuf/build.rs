extern crate protoc_rust;

fn main() {
    protoc_rust::Codegen::new()
        .out_dir("src/api/v1")
        .inputs(&["protos/log.proto"])
        .include("protos")
        .run()
        .expect("protoc");
}
