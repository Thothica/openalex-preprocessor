use std::io::Read;

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

const OPENALEX_WORKS_DIRECTORY: &str = "openalex-snapshot-works/";
// const OUTPUT_DIRECTORY: &str = "processed-data/";

#[derive(Deserialize, Debug, Serialize)]
struct WorkObject {
    open_access: OpenAcess,
    oa_url: Option<String>,
}

#[derive(Deserialize, Debug, Serialize)]
struct OpenAcess {
    is_oa: bool,
}

// fn process_object() {}

fn main() {
    // let mut handles = vec![];

    for entry in WalkDir::new(OPENALEX_WORKS_DIRECTORY) {
        let path = entry.unwrap();
        let bytes = match std::fs::read(path.path()) {
            Ok(data) => data,
            Err(_) => continue,
        };

        println!("{:?}", path.path());

        let mut gz = GzDecoder::new(&bytes[..]);
        let mut contents = String::new();
        gz.read_to_string(&mut contents).unwrap();

        for content in contents.lines() {
            let obj: WorkObject = serde_json::from_str(content).unwrap();
            println!("{}", obj.open_access.is_oa)
        }
    }
}
