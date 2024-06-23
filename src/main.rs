use std::{
    collections::BinaryHeap,
    fs::File,
    io::{self, BufRead, Write},
    path::Path,
};

use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

const OPENALEX_WORKS_DIRECTORY: &str = "openalex-snapshot-works/";
const OUTPUT_FILE: &str = "best_works.jsonl.gz";
const TOTAL_OBJECTS: u32 = 1_050_000;

#[derive(Deserialize, Debug, Serialize)]
struct WorkObject {
    id: String,
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
        if !self.open_access.is_oa {
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
    println!("processing directory: {}", OPENALEX_WORKS_DIRECTORY);

    for entry in WalkDir::new(OPENALEX_WORKS_DIRECTORY) {
        let path = entry.unwrap();
        if let Ok(contents) = read_lines(path.path()) {
            for content in contents.flatten() {
                let obj: WorkObject = match serde_json::from_str(&content) {
                    Ok(data) => data,
                    Err(_) => {
                        continue;
                    }
                };
                if obj.is_useful() && obj.cited_by_count > 0 {
                    max_heap.push(obj);
                }
            }
        }
    }

    let total = max_heap.len();
    let best = max_heap.peek().unwrap().cited_by_count;
    println!(
        "Reading complete\nRead {} objetcs\nhighest_citation: {}",
        total, best
    );

    let output = File::create(OUTPUT_FILE).unwrap();
    let mut encoder = GzEncoder::new(output, Compression::default());

    println!("Writing objects in {}", OUTPUT_FILE);

    for _ in 0..TOTAL_OBJECTS {
        let obj = match max_heap.pop() {
            Some(data) => data,
            None => break,
        };
        let json = serde_json::to_string(&obj).unwrap() + "\n";
        encoder.write_all(json.as_bytes()).unwrap();
    }

    encoder.try_finish().unwrap();
}
