extern crate chrono;
extern crate postgres;
extern crate protobuf;
extern crate reqwest;

mod gtfs_realtime;

use chrono::prelude::*;

use gtfs_realtime::FeedMessage;
use gtfs_realtime::VehiclePosition_VehicleStopStatus as VehicleStatus;

use std::collections::HashMap;
use std::env;
use std::time;
use std::thread;

use postgres::Connection;
use postgres::TlsMode;

type VehiclePositionMap = HashMap<String, (String, VehicleStatus)>;

fn main() {
    let db = get_db();
    let two_sec = time::Duration::from_millis(2000);
    let mut positions = HashMap::new();

    loop {
        positions = run(&db, positions);
        thread::sleep(two_sec);
    }
}

fn run(db: &Connection, old_positions: VehiclePositionMap) -> VehiclePositionMap {
    let mut now = time::Instant::now();
    print!("Run loop... ");

    match reqwest::get("https://s3.amazonaws.com/mbta-gtfs-s3/VehiclePositions.pb") {
        Ok(mut veh_body) => {
            if veh_body.status().is_success() {
                print!("Fetched in {:?} ms, ", elapsed_ms(&now));
                now = time::Instant::now();
                let mut data: Vec<u8> = vec![];
                if let Ok(_) = veh_body.copy_to(&mut data) {
                    if let Ok(message) = protobuf::parse_from_bytes::<FeedMessage>(&data) {
                        let new_positions = process_data(db, &message, old_positions);
                        print!("processed in {:?} ms\n", elapsed_ms(&now));
                        return new_positions;
                    }
                }
            } else {
                println!("error getting vehicle positions, status: {:?}", veh_body.status());
            }
        },
        Err(err) => println!("error fetching veh_positions: {:?}", err),
    }

    return old_positions;
}

fn process_data(db: &Connection, msg: &FeedMessage, old_positions: VehiclePositionMap) -> VehiclePositionMap {
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
    ", &[&vehicle_id, &stop_id, &now()]);

    match res {
        Ok(1) => (),
        Ok(_) => println!("WARN - weird postgres result when trying to update train_arrived"),
        Err(e) => println!("WARN - could not update DB for train_arrived: {:?}", e)
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
    ", &[&now(), &vehicle_id, &stop_id]);

    match res {
        Ok(0) => (),
        Ok(1) => (),
        Ok(_) => println!("WARN - weird postgres result when trying to update train_departed"),
        Err(e) => println!("WARN - could not update DB for train_departed: {:?}", e)
    }
}

fn get_db() -> Connection {
    let db_url = env::var("DATABASE_URL").expect("No DATABASE_URL ENV variable.");
    let conn = Connection::connect(db_url, TlsMode::None).expect("Could not connect to given DATABASE_URL.");
    conn.execute("
        CREATE TABLE IF NOT EXISTS vehicle_movements (
            vehicle_id varchar not null,
            stop_id varchar not null,
            arrived_at timestamptz,
            departed_at timestamptz,
            primary key (vehicle_id, stop_id)
        )
    ", &[]).expect("Could not initialize DB.");

    conn
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

fn elapsed_ms(start: &time::Instant) -> u64 {
    let duration = start.elapsed();
    duration.as_secs() * 1000 + (duration.subsec_millis() as u64)
}
