use std::{
    fs::File,
    io::{self, BufRead},
    path::Path,
    sync::Arc,
};

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use walkdir::WalkDir;

const OPENALEX_WORKS_DIRECTORY: &str = "openalex-snapshot-works/";
// const OUTPUT_DIRECTORY: &str = "processed-data/";

#[derive(Deserialize, Debug, Serialize)]
struct WorkObject {
    open_access: OpenAcess,
    cited_by_count: u32,
    title: String,
    primary_topic: Topic,
    publication_year: u32,
    publication_date: Option<String>,
}

#[derive(Deserialize, Debug, Serialize)]
struct OpenAcess {
    is_oa: bool,
    oa_url: String,
    oa_status: String,
}

#[derive(Deserialize, Debug, Serialize)]
struct Topic {
    id: String,
    domain: Domain,
}

#[derive(Deserialize, Debug, Serialize)]
struct Domain {
    display_name: String,
}

impl WorkObject {
    fn is_useful(&self) -> bool {
        if !self.open_access.is_oa && self.open_access.oa_status != "gold" {
            return false;
        }
        if self.primary_topic.domain.display_name != "Social Sciences" {
            return false;
        }

        true
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<flate2::read::GzDecoder<File>>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let gz = GzDecoder::new(file);
    Ok(io::BufReader::new(gz).lines())
}

#[tokio::main]
async fn main() {
    let mut handles = vec![];

    let count = Arc::new(Mutex::new(0));

    for entry in WalkDir::new(OPENALEX_WORKS_DIRECTORY) {
        let path = entry.unwrap();

        if let Ok(contents) = read_lines(path.path()) {
            for content in contents.flatten() {
                let obj: WorkObject = match serde_json::from_str(&content) {
                    Ok(data) => data,
                    Err(_err) => {
                        continue;
                    }
                };

                let count = Arc::clone(&count);
                let handle = tokio::spawn(async move {
                    if obj.is_useful() {
                        let mut count = count.lock().await;
                        *count += 1;
                    }
                });

                handles.push(handle);
            }
        }
    }

    for handle in handles {
        let _ = handle.await;
    }

    let count = count.lock().await;
    println!("Useful objects: {}", count)
}
