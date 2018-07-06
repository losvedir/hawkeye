extern crate protobuf;
extern crate reqwest;

mod gtfs_realtime;
mod movement;

use std::{thread, time};
use gtfs_realtime::FeedMessage;
use gtfs_realtime::VehiclePosition_VehicleStopStatus;
use std::collections::HashMap;
use movement::Movement;

type VehiclePositionMap = HashMap<String, (String, VehiclePosition_VehicleStopStatus)>;

fn main() {
    let five_sec = time::Duration::from_millis(5000);
    let mut movements = vec![];
    let mut positions = HashMap::new();

    loop {
        positions = run(positions, &mut movements);
        println!("{:?}", positions);
        thread::sleep(five_sec);
    }
}

fn run(old_positions: VehiclePositionMap, movements: &mut Vec<Movement>) -> VehiclePositionMap {
    println!("Checking!");

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

fn process_data(msg: &FeedMessage, _old_positions: VehiclePositionMap, _movements: &mut Vec<Movement>) -> VehiclePositionMap {
    let mut new_positions: VehiclePositionMap = HashMap::new();

    for entity in msg.get_entity() {
        if entity.has_vehicle() {
            let vehicle_position = entity.get_vehicle();
            if vehicle_position.has_vehicle() && vehicle_position.has_stop_id() && vehicle_position.has_current_status() {
                let vehicle_descriptor = vehicle_position.get_vehicle();
                let stop_id = vehicle_position.get_stop_id();
                let status = vehicle_position.get_current_status();

                if vehicle_descriptor.has_id() {
                    let vehicle_id = vehicle_descriptor.get_id();

                    new_positions.insert(vehicle_id.to_string(), (stop_id.to_string(), status));

                }
            }
        }
    }

    return new_positions;
}

