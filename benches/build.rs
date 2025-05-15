fn main() {
    prost_build::compile_protos(&["proto/test_record.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
