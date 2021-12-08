extern crate prost_build;

use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/udf.proto"], &["src/"])?;
    Ok(())
}
