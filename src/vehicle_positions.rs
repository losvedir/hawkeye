use chrono::prelude::*;
use gtfs_realtime::FeedMessage;
use gtfs_realtime::VehiclePosition_VehicleStopStatus as VehicleStatus;
use helpers::download_file;
use helpers::elapsed_ms;
use postgres::Connection;
use protobuf;
use std::collections::HashMap;
use std::thread;
use std::time;

type VehiclePositionMap = HashMap<String, (String, VehicleStatus)>;

pub fn run(db: &Connection) {
    let two_sec = time::Duration::from_millis(2000);
    let mut positions = HashMap::new();

    loop {
        positions = do_run(&db, positions);
        thread::sleep(two_sec);
    }
}

fn do_run(db: &Connection, old_positions: VehiclePositionMap) -> VehiclePositionMap {
    if let Some(data) = download_file("https://s3.amazonaws.com/mbta-gtfs-s3/VehiclePositions.pb") {
        let now = time::Instant::now();

        if let Ok(message) = protobuf::parse_from_bytes::<FeedMessage>(&data) {
            let new_positions = process_vehicle_positions(db, &message, old_positions);
            println!("Processed VehiclePositions in {:?} ms.", elapsed_ms(&now));
            return new_positions;
        }

    }

    return old_positions;
}

fn process_vehicle_positions(db: &Connection, msg: &FeedMessage, old_positions: VehiclePositionMap) -> VehiclePositionMap {
    let mut new_positions: VehiclePositionMap = HashMap::new();

    for entity in msg.get_entity() {
        if entity.has_vehicle() {
            let vehicle_position = entity.get_vehicle();
            if vehicle_position.has_vehicle() && vehicle_position.has_stop_id() && vehicle_position.has_current_status() {
                let vehicle_descriptor = vehicle_position.get_vehicle();
                let stop_id = vehicle_position.get_stop_id().to_string();
                let status = vehicle_position.get_current_status();

                if vehicle_descriptor.has_id() {
                    let vehicle_id = vehicle_descriptor.get_id();

                    if let Some((old_stop_id, old_status)) = old_positions.get(vehicle_id) {
                        record_movement(db, vehicle_id, old_stop_id, old_status, &stop_id, &status);
                    }

                    new_positions.insert(vehicle_id.to_string(), (stop_id, status));
                }
            }
        }
    }

    return new_positions;
}

fn record_movement(db: &Connection, vehicle_id: &str, old_stop_id: &str, old_status: &VehicleStatus, new_stop_id: &str, new_status: &VehicleStatus) {
    if old_stop_id == new_stop_id {
        match (old_status, new_status) {
            (VehicleStatus::INCOMING_AT, VehicleStatus::STOPPED_AT) => train_arrived(db, vehicle_id, new_stop_id),
            (VehicleStatus::IN_TRANSIT_TO, VehicleStatus::STOPPED_AT) => train_arrived(db, vehicle_id, new_stop_id),
            _ => (),
        }
    } else {
        match (old_status, new_status) {
            (VehicleStatus::STOPPED_AT, VehicleStatus::IN_TRANSIT_TO) => train_departed(db, vehicle_id, old_stop_id),
            (VehicleStatus::STOPPED_AT, VehicleStatus::INCOMING_AT) => train_departed(db, vehicle_id, old_stop_id),
            _ => (),
        }
    }
}

fn train_arrived(db: &Connection, vehicle_id: &str, stop_id: &str) {
    let res = db.execute("
        INSERT INTO vehicle_movements
        (vehicle_id, stop_id, arrived_at, departed_at)
        VALUES ($1, $2, $3, NULL)
        ON CONFLICT (vehicle_id, stop_id) DO UPDATE
        SET (vehicle_id, stop_id, arrived_at, departed_at) = ($1, $2, $3, NULL)
    ", &[&vehicle_id, &stop_id, &Utc::now()]);

    match res {
        Ok(1) => (),
        Ok(_) => println!("WARN - weird postgres result when trying to update train_arrived"),
        Err(e) => println!("WARN - could not update DB for train_arrived: {:?}", e)
    }

    let res2 = db.execute("
        UPDATE predictions
        SET actual_arrive_at = $1
        WHERE vehicle_id = $2
          AND stop_id = $3
          AND actual_arrive_at IS NULL
    ", &[&Utc::now(), &vehicle_id, &stop_id]);

    if let Err(e) = res2 {
        println!("Error updating arrival: {:?}", e);
    }
}

fn train_departed(db: &Connection, vehicle_id: &str, stop_id: &str) {
    let res = db.execute("
        UPDATE vehicle_movements
        SET departed_at = $1
        WHERE vehicle_id = $2
          AND stop_id = $3
          AND arrived_at IS NOT NULL
          AND departed_at IS NULL
    ", &[&Utc::now(), &vehicle_id, &stop_id]);

    match res {
        Ok(0) => (),
        Ok(1) => (),
        Ok(_) => println!("WARN - weird postgres result when trying to update train_departed"),
        Err(e) => println!("WARN - could not update DB for train_departed: {:?}", e)
    }

    let res2 = db.execute("
        UPDATE predictions
        SET actual_depart_at = $1
        WHERE vehicle_id = $2
          AND stop_id = $3
          AND actual_arrive_at IS NULL
    ", &[&Utc::now(), &vehicle_id, &stop_id]);

    if let Err(e) = res2 {
        println!("Error updating arrival: {:?}", e);
    }
}
