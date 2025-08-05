use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::thread;
use std::time::Instant;
use crossbeam::channel;

#[derive(Debug, Clone)]
struct TempStats {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
}

impl TempStats {
    fn new(temp: f64) -> Self {
        TempStats {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    fn update(&mut self, temp: f64) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.count += 1;
    }

    fn mean(&self) -> f64 {
        self.sum / self.count as f64
    }
}

fn process_lines(lines: &[Vec<u8>]) -> BTreeMap<Vec<u8>, TempStats> {
    let mut city_stats: BTreeMap<Vec<u8>, TempStats> = BTreeMap::new();

    for line in lines {
        if let Some(pos) = line.iter().position(|&b| b == b';') {
            let (city, temp_bytes) = line.split_at(pos);
            let temp_str = std::str::from_utf8(&temp_bytes[1..]); // skip ';'

            if let Ok(temp_str) = temp_str {
                if let Ok(temp) = temp_str.trim().parse::<f64>() {
                    city_stats
                        .entry(city.to_vec()) // only convert once
                        .and_modify(|s| s.update(temp))
                        .or_insert_with(|| TempStats::new(temp));
                }
            }
        }
    }

    city_stats
}


fn main() -> io::Result<()> {
    let start = Instant::now();

    let file = File::open("../data/weather_stations.csv")?;
    let reader = BufReader::new(file);

    let num_threads = 8;
    let batch_size = 100_000;

    let (sender, receiver) = channel::unbounded();
    let mut handles = vec![];
    let mut buffer = Vec::with_capacity(batch_size);
    let mut line_stream = reader.split(b'\n');

    while let Some(line_result) = line_stream.next() {
        if let Ok(line) = line_result {
            buffer.push(line);
            if buffer.len() >= batch_size {
                let batch = std::mem::take(&mut buffer);
                let thread_sender = sender.clone();
                let handle = thread::spawn(move || {
                    let result = process_lines(&batch);
                    thread_sender.send(result).expect("Failed to send result");
                });
                handles.push(handle);
            }
        }
    }

    // Handle remaining lines
    if !buffer.is_empty() {
        let thread_sender = sender.clone();
        let batch = std::mem::take(&mut buffer);
        let handle = thread::spawn(move || {
            let result = process_lines(&batch);
            thread_sender.send(result).expect("Failed to send result");
        });
        handles.push(handle);
    }

    drop(sender); // Close sender

    let partial_maps: Vec<_> = receiver.iter().collect();
    let final_map = merge_maps(partial_maps);

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let duration = start.elapsed();
    println!("Execution time: {:.2?}", duration);

    Ok(())
}



fn merge_maps(mut maps: Vec<BTreeMap<Vec<u8>, TempStats>>) -> BTreeMap<Vec<u8>, TempStats> {
    while maps.len() > 1 {
        let mut next_round = vec![];
        let mut handles = vec![];

        for chunk in maps.chunks(2) {
            if chunk.len() == 2 {
                let mut left = chunk[0].clone();
                let right = chunk[1].clone();

                let handle = thread::spawn(move || {
                    for (city, stats) in right {
                        left.entry(city)
                            .and_modify(|s| {
                                s.min = s.min.min(stats.min);
                                s.max = s.max.max(stats.max);
                                s.sum += stats.sum;
                                s.count += stats.count;
                            })
                            .or_insert(stats);
                    }
                    left
                });

                handles.push(handle);
            } else {
                // Unpaired map (odd count), move to next round directly
                next_round.push(chunk[0].clone());
            }
        }

        // Collect merged results
        for handle in handles {
            next_round.push(handle.join().expect("Merge thread failed"));
        }

        maps = next_round;
    }

    maps.pop().unwrap_or_default()
}
