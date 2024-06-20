use std::{
    collections::BinaryHeap,
    fs::File,
    io::{self, BufRead},
    path::Path,
};

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

const OPENALEX_WORKS_DIRECTORY: &str = "openalex-snapshot-works/";
// const OUTPUT_DIRECTORY: &str = "processed-data/";

#[derive(Deserialize, Debug, Serialize)]
struct WorkObject {
    open_access: OpenAcess,
    cited_by_count: u32,
    title: String,
    primary_topic: Topic,
    language: String,
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
        if self.language != "en" {
            return false;
        }
        if self.primary_topic.domain.display_name != "Social Sciences" {
            return false;
        }

        true
    }
}

impl PartialEq for WorkObject {
    fn eq(&self, other: &Self) -> bool {
        self.cited_by_count == other.cited_by_count
    }
}

impl Eq for WorkObject {}

impl PartialOrd for WorkObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.cmp(self))
    }
}

impl Ord for WorkObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.cited_by_count.cmp(&self.cited_by_count)
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

fn main() {
    let mut max_heap: BinaryHeap<WorkObject> = BinaryHeap::new();
    let mut greatest = 0;

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
                if obj.is_useful() && obj.cited_by_count > 0 {
                    if obj.cited_by_count > greatest {
                        greatest = obj.cited_by_count;
                    }
                    max_heap.push(obj);
                }
            }
        }
    }

    let total = max_heap.len();
    let best = max_heap.peek().unwrap().cited_by_count;
    println!("Useful objects: {}\nbest one: {}", total, best);
    println!("Greatest: {}", greatest);
}
