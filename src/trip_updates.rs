use gtfs_realtime::FeedMessage;
use helpers::download_file;
use helpers::elapsed_ms;
use std::time;
use protobuf;
use postgres::Connection;
use std::thread;

pub fn run(db: &Connection) {
    let wait_time = time::Duration::from_millis(10_000);

    loop {
        do_run(db);
        thread::sleep(wait_time);
    }
}

fn do_run(db: &Connection) {
    if let Some(data) = download_file("https://s3.amazonaws.com/mbta-gtfs-s3/VehiclePositions.pb") {
        let now = time::Instant::now();

        if let Ok(message) = protobuf::parse_from_bytes::<FeedMessage>(&data) {
            process_trip_updates(db, &message);
            println!("Processed TripUpdates in {:?} ms", elapsed_ms(&now));
        }
    }
}

fn process_trip_updates(_db: &Connection, _msg: &FeedMessage) {

}

