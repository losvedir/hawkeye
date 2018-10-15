use chrono::prelude::*;
use gtfs_realtime::*;
use helpers::download_file;
use helpers::elapsed_ms;
use std::time;
use serde_json;
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
    if let Some(json) = download_file("https://s3.amazonaws.com/mbta-gtfs-s3/rtr/TripUpdates_enhanced.json") {
        let now = time::Instant::now();

        if let Ok(message) = serde_json::from_str::<FeedMessage>(&json) {
            process_trip_updates(db, &message);
            println!("Processed TripUpdates in {:?} ms", elapsed_ms(&now));
        }
    }
}

fn process_trip_updates(db: &Connection, msg: &FeedMessage) {
    let file_at = &Utc::now();
    let mut predictions: Vec<Prediction> = vec![];

    for entity in &msg.entity {
        if let FeedEntity{
            trip_update: Some(TripUpdate{
                trip: TripDescriptor{
                    trip_id: Some(trip_id),
                    direction_id: Some(direction_id),
                    ..
                },
                vehicle: Some(VehicleDescriptor{
                    id: Some(vehicle_id),
                    ..
                }),
                stop_time_update: stop_time_updates,
                ..
            }),
            ..
        } = entity {
            for update in stop_time_updates {
                if let TripUpdateStopTimeUpdate{
                    stop_id: Some(stop_id),
                    stop_sequence: Some(stop_sequence),
                    arrival,
                    departure,
                    boarding_status,
                    ..
                } = update {
                    let arrive_time = match arrival {
                        Some(TripUpdateStopTimeEvent{
                            time: Some(time),
                            ..
                        }) => Some(Utc.timestamp(*time, 0)),
                        _ => None
                    };

                    let depart_time = match departure {
                        Some(TripUpdateStopTimeEvent{
                            time: Some(time),
                            ..
                        }) => Some(Utc.timestamp(*time, 0)),
                        _ => None
                    };

                    let prediction = Prediction {
                        file_at: &file_at,
                        trip_id: &trip_id,
                        vehicle_id: &vehicle_id,
                        stop_id: &stop_id,
                        stop_sequence: *stop_sequence as i32,
                        direction_id: *direction_id as i32,
                        predicted_arrive_at: arrive_time,
                        predicted_depart_at: depart_time,
                        boarding_status: boarding_status,
                    };

                    predictions.push(prediction);
                }
            }
        }
    }

    println!("predictions count: {:?}", predictions.len());

    let copy_string = generate_copy_string(predictions);
    let stmt = db.prepare("COPY predictions (file_at, trip_id, vehicle_id, stop_id, stop_sequence, direction_id, predicted_arrive_at, predicted_depart_at, boarding_status) FROM STDIN").unwrap();

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
    boarding_status: &'a Option<String>,
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
        copy_string.push_str("\t");
        match p.boarding_status {
            Some(bs) => copy_string.push_str(&bs),
            None => copy_string.push_str("\\N")
        }
        copy_string.push_str("\n");
    }

    copy_string
}
