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

    for entity in msg.get_entity() {
        if !entity.has_trip_update() { continue; }
        let trip_update = entity.get_trip_update();
        let trip_descriptor = trip_update.get_trip();

        if !(trip_descriptor.has_trip_id() && trip_descriptor.has_direction_id()) { continue; }
        let trip_id = trip_descriptor.get_trip_id();
        let direction_id = trip_descriptor.get_direction_id() as i32;

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
                direction_id: direction_id,
                predicted_arrive_at: arrive_time,
                predicted_depart_at: depart_time,
            };

            predictions.push(prediction);
        }
    }

    let copy_string = generate_copy_string(predictions);
    let stmt = db.prepare("COPY predictions (file_at, trip_id, vehicle_id, stop_id, stop_sequence, direction_id, predicted_arrive_at, predicted_depart_at) FROM STDIN").unwrap();

    match stmt.copy_in(&[], &mut copy_string.as_bytes()) {
        Ok(n) => println!("Added {:?} predictions", n),
        Err(e) => println!("Could not add predictions: {:?}", e)
    }

    let stmt = db.execute("
        UPDATE predictions p
        SET nth_at_stop = p2.rank
        FROM (
            SELECT
                file_at, trip_id, vehicle_id, stop_id,
                rank() OVER (PARTITION BY file_at, stop_id ORDER BY LEAST(predicted_arrive_at, predicted_depart_at) ASC)
            FROM predictions
            WHERE file_at = $1
        ) AS p2
        WHERE p.file_at = p2.file_at
          AND p.trip_id = p2.trip_id
          AND p.vehicle_id = p2.vehicle_id
          AND p.stop_id = p2.stop_id
        ;
    ", &[&file_at]);

    match stmt {
        Ok(n) => println!("Updated {:?} predictions with nth_at_stop", n),
        Err(e) => println!("Error updating: {:?}", e)
    }
}

struct Prediction<'a, 'b> {
    file_at: &'b DateTime<Utc>,
    trip_id: &'a str,
    vehicle_id: &'a str,
    stop_id: &'a str,
    stop_sequence: i32,
    direction_id: i32,
    predicted_arrive_at: Option<DateTime<Utc>>,
    predicted_depart_at: Option<DateTime<Utc>>,
}

fn generate_copy_string(predictions: Vec<Prediction>) -> String {
    let mut copy_string = String::with_capacity(1_000);

    for p in predictions {
        copy_string.push_str(&p.file_at.to_rfc3339());
        copy_string.push_str("\t");
        copy_string.push_str(p.trip_id);
        copy_string.push_str("\t");
        copy_string.push_str(p.vehicle_id);
        copy_string.push_str("\t");
        copy_string.push_str(p.stop_id);
        copy_string.push_str("\t");
        copy_string.push_str(&p.stop_sequence.to_string());
        copy_string.push_str("\t");
        copy_string.push_str(&p.direction_id.to_string());
        copy_string.push_str("\t");
        match p.predicted_arrive_at {
            Some(dt) => copy_string.push_str(&dt.to_rfc3339()),
            None => copy_string.push_str("\\N")
        }
        copy_string.push_str("\t");
        match p.predicted_depart_at {
            Some(dt) => copy_string.push_str(&dt.to_rfc3339()),
            None => copy_string.push_str("\\N")
        }
        copy_string.push_str("\n");
    }

    copy_string
}
