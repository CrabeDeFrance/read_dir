use std::{
    collections::{BTreeMap, VecDeque},
    env,
    fs::File,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant, UNIX_EPOCH},
};

use futures_util::StreamExt;
use inotify::{Inotify, WatchMask};
use tokio::runtime::Runtime;

fn create_files(rx: std::sync::mpsc::Receiver<()>, dir: String) {
    let mut count = 1;
    loop {
        match rx.try_recv() {
            Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                println!("Terminating.");
                break;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }

        let mut file = File::create(format!("{dir}/file{count}.txt")).unwrap();
        file.write_all(b"Hello, world!").unwrap();
        count += 1;
    }
}

fn read_dir(dir: &String, max: usize) -> std::io::Result<()> {
    let mut count = 0;

    loop {
        for entry in std::fs::read_dir(dir)? {
            count += 1;
            let _path = entry?.path();

            if count == max {
                break;
            }
        }

        if count == max {
            break;
        }
    }

    Ok(())
}

fn read_dir_sorted(dir: &String, max: usize) -> std::io::Result<()> {
    // btreemap to order files by date
    let mut ordered_files: BTreeMap<u128, VecDeque<PathBuf>> = BTreeMap::new();

    let mut count = 0;

    loop {
        for entry in std::fs::read_dir(dir)? {
            count += 1;
            let path = entry?.path();

            // insert files, automatically ordered by key (date)
            let modified_date = match std::fs::metadata(&path) {
                Ok(metadata) => metadata.modified(),
                Err(e) => {
                    println!("Can't get metadata for file {path:?}: {e}");
                    continue;
                }
            };

            let duration = match modified_date {
                Ok(t) => t.duration_since(UNIX_EPOCH),
                Err(e) => {
                    println!("Can't get modified time for file {path:?}: {e}");
                    continue;
                }
            };

            let duration = match duration {
                Ok(duration) => duration,
                Err(e) => {
                    println!("Can't get time duration for file {path:?}: {e}");
                    continue;
                }
            };

            let duration_nano = duration.as_nanos();
            if let Some(row) = ordered_files.get_mut(&duration_nano) {
                row.push_front(path);
            } else {
                let mut v = VecDeque::new();
                v.push_front(path);
                ordered_files.insert(duration_nano, v);
            }

            if count == max {
                break;
            }
        }

        if count == max {
            break;
        }
    }

    Ok(())
}

fn read_inotify(dir: &String, max: usize) {
    let mut inotify = Inotify::init().expect("Error while initializing inotify instance");
    inotify
        .watches()
        .add(dir, WatchMask::CLOSE_WRITE)
        .expect("Failed to add file watch");

    let mut buffer = [0; 8096];
    let mut count = 0;

    loop {
        let events = inotify
            .read_events_blocking(&mut buffer)
            .expect("Error while reading events");

        for event in events {
            // Handle event
            if let Some(_filename) = event.name {
                count += 1;

                if count == max {
                    break;
                }
            }
        }

        if count == max {
            break;
        }
    }
}

fn read_inotify_async(dir: &String, max: usize) {
    let inotify = Inotify::init().expect("Error while initializing inotify instance");
    inotify
        .watches()
        .add(dir, WatchMask::CLOSE_WRITE)
        .expect("Failed to add file watch");

    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        // Read events that were added with `Watches::add` above.
        let mut buffer = [0; 1024];

        // les différents types d'événements pour tokio_select : inotify, signal, timer
        let mut stream = inotify.into_event_stream(&mut buffer).unwrap();

        let mut count = 0;

        loop {
            tokio::select! {
                _event = stream.next() => {
                    count += 1;
                    if count == max {
                        break;
                    }
                },
            }
        }
    });
}

fn read_dir_tokio(dir: &String, max: usize) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut read_dir = tokio::fs::read_dir(dir).await.unwrap();

        let mut count = 0;

        loop {
            tokio::select! {
                event = read_dir.next_entry() => {
                    if let Ok(Some(_file)) = event {
                        count += 1;
                        if count == max {
                            break;
                        }
                    }
                },
            }
        }
    });
}

fn main() {
    let max_files = 200_000;
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("invalid number of arguments");
    }

    let dir: String = args[1].clone();

    if std::fs::metadata(&dir).is_ok() {
        panic!("Error: path {dir} exists");
    }

    std::fs::create_dir_all(&dir).unwrap();

    let thread_dir = dir.clone();
    let (tx, rx) = std::sync::mpsc::channel();

    // thread to create a lot of files continuously
    let gen_thread = std::thread::spawn(|| {
        create_files(rx, thread_dir);
    });

    // wait 1s to create some initial files
    std::thread::sleep(Duration::from_secs(1));

    // readdir sync unsorted
    let now = Instant::now();
    read_dir(&dir, max_files).unwrap();
    let elapsed = now.elapsed();

    println!(
        "read_dir duration: {}.{:0>3}s",
        elapsed.as_secs(),
        elapsed.as_millis()
    );

    // readdir sync sorted
    let now = Instant::now();
    read_dir_sorted(&dir, max_files).unwrap();
    let elapsed = now.elapsed();

    println!(
        "read_dir_sorted duration: {}.{:0>3}s",
        elapsed.as_secs(),
        elapsed.as_millis()
    );

    // readdir async (tokio)
    let now = Instant::now();
    read_dir_tokio(&dir, max_files);
    let elapsed = now.elapsed();

    println!(
        "readdir tokio duration: {}.{:0>3}s",
        elapsed.as_secs(),
        elapsed.as_millis()
    );

    // sync inotify
    let now = Instant::now();
    read_inotify(&dir, max_files);
    let elapsed = now.elapsed();

    println!(
        "inotify duration: {}.{:0>3}s",
        elapsed.as_secs(),
        elapsed.as_millis()
    );

    // async / tokio inotify
    let now = Instant::now();
    read_inotify_async(&dir, max_files);
    let elapsed = now.elapsed();

    println!(
        "inotify async duration: {}.{:0>3}s",
        elapsed.as_secs(),
        elapsed.as_millis()
    );

    let _ = tx.send(());
    gen_thread.join().unwrap();

    std::fs::remove_dir_all(&dir).unwrap();
}
