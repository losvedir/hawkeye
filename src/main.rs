extern crate protobuf;
extern crate reqwest;

mod gtfs_realtime;
mod movement;

use std::{thread, time};
use gtfs_realtime::FeedMessage;
use gtfs_realtime::VehiclePosition_VehicleStopStatus as VehicleStatus;
use std::collections::HashMap;
use movement::Movement;
use std::time::SystemTime;

type VehiclePositionMap = HashMap<String, (String, VehicleStatus)>;

fn main() {
    let two_sec = time::Duration::from_millis(2000);
    let mut movements = vec![];
    let mut positions = HashMap::new();

    loop {
        positions = run(positions, &mut movements);
        thread::sleep(two_sec);
    }
}

fn run(old_positions: VehiclePositionMap, movements: &mut Vec<Movement>) -> VehiclePositionMap {
    match reqwest::get("https://s3.amazonaws.com/mbta-gtfs-s3/VehiclePositions.pb") {
        Ok(mut veh_body) => {
            if veh_body.status().is_success() {
                let mut data: Vec<u8> = vec![];
                if let Ok(_) = veh_body.copy_to(&mut data) {
                    if let Ok(message) = protobuf::parse_from_bytes::<FeedMessage>(&data) {
                        let new_positions = process_data(&message, old_positions, movements);
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

fn process_data(msg: &FeedMessage, old_positions: VehiclePositionMap, _movements: &mut Vec<Movement>) -> VehiclePositionMap {
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
                        record_movement(vehicle_id, old_stop_id, old_status, &stop_id, &status);
                    }

                    new_positions.insert(vehicle_id.to_string(), (stop_id, status));
                }
            }
        }
    }

    return new_positions;
}

fn record_movement(vehicle_id: &str, old_stop_id: &str, old_status: &VehicleStatus, new_stop_id: &str, new_status: &VehicleStatus) {
    if old_stop_id == new_stop_id {
        match (old_status, new_status) {
            (VehicleStatus::INCOMING_AT, VehicleStatus::STOPPED_AT) => train_arrived(vehicle_id, new_stop_id),
            (VehicleStatus::IN_TRANSIT_TO, VehicleStatus::STOPPED_AT) => train_arrived(vehicle_id, new_stop_id),
            _ => (),
        }
    } else {
        match (old_status, new_status) {
            (VehicleStatus::STOPPED_AT, VehicleStatus::IN_TRANSIT_TO) => train_departed(vehicle_id, old_stop_id),
            (VehicleStatus::STOPPED_AT, VehicleStatus::INCOMING_AT) => train_departed(vehicle_id, old_stop_id),
            _ => (),
        }
    }
}

fn train_arrived(vehicle_id: &str, stop_id: &str) {
    println!("{:?} arrived at {:?} at {:?}", vehicle_id, stop_id, SystemTime::now());
}

fn train_departed(vehicle_id: &str, stop_id: &str) {
    println!("{:?} departed from {:?} at {:?}", vehicle_id, stop_id, SystemTime::now());
}
