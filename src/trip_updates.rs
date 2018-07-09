use chrono::prelude::*;
use gtfs_realtime::FeedMessage;
use helpers::download_file;
use helpers::elapsed_ms;
use std::time;
use protobuf;
use postgres::Connection;
use std::thread;

pub fn run(db: &Connection) {
    let wait_time = time::Duration::from_millis(60_000);

    loop {
        do_run(db);
        thread::sleep(wait_time);
    }
}

fn do_run(db: &Connection) {
    if let Some(data) = download_file("https://s3.amazonaws.com/mbta-gtfs-s3/TripUpdates.pb") {
        let now = time::Instant::now();

        if let Ok(message) = protobuf::parse_from_bytes::<FeedMessage>(&data) {
            process_trip_updates(db, &message);
            println!("Processed TripUpdates in {:?} ms", elapsed_ms(&now));
        }
    }
}

fn process_trip_updates(db: &Connection, msg: &FeedMessage) {
    let file_at = &Utc::now();
    let mut predictions: Vec<Prediction> = vec![];

    let stmt = db.prepare("
        INSERT into predictions
        (file_at, trip_id, vehicle_id, stop_id, stop_sequence, predicted_arrive_at, predicted_depart_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    ").unwrap();

    for entity in msg.get_entity() {
        if !entity.has_trip_update() { continue; }
        let trip_update = entity.get_trip_update();
        let trip_descriptor = trip_update.get_trip();

        if !trip_descriptor.has_trip_id() { continue; }
        let trip_id = trip_descriptor.get_trip_id();

        if !trip_update.has_vehicle() { continue; }
        let vehicle = trip_update.get_vehicle();

        if !vehicle.has_id() { continue; }
        let vehicle_id = vehicle.get_id();

        for update in trip_update.get_stop_time_update() {
            if !(update.has_stop_id() && update.has_stop_sequence()) { continue; }
            let stop_id = update.get_stop_id();
            let stop_sequence = update.get_stop_sequence() as i32;

            let arrive_time = if update.has_arrival() && update.get_arrival().has_time() {
                let time = update.get_arrival().get_time();
                Some(Utc.timestamp(time, 0))
            } else {
                None
            };

            let depart_time = if update.has_departure() && update.get_departure().has_time() {
                let time = update.get_departure().get_time();
                Some(Utc.timestamp(time, 0))
            } else {
                None
            };

            let prediction = Prediction {
                file_at: &file_at,
                trip_id: &trip_id,
                vehicle_id: &vehicle_id,
                stop_id: &stop_id,
                stop_sequence: stop_sequence,
                predicted_arrive_at: arrive_time,
                predicted_depart_at: depart_time,
            };

            if let Err(e) = stmt.execute(&[&file_at, &trip_id, &vehicle_id, &stop_id, &stop_sequence, &arrive_time, &depart_time]) {
                println!("Could not insert prediction: {:?}", e);
            }

            predictions.push(prediction);
        }
    }

    println!("Predictions length: {:?}", predictions.len());
}

struct Prediction<'a, 'b> {
    file_at: &'b DateTime<Utc>,
    trip_id: &'a str,
    vehicle_id: &'a str,
    stop_id: &'a str,
    stop_sequence: i32,
    predicted_arrive_at: Option<DateTime<Utc>>,
    predicted_depart_at: Option<DateTime<Utc>>,
}
