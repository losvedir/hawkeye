extern crate chrono;
extern crate postgres;
extern crate protobuf;
extern crate reqwest;

mod helpers;
mod gtfs_realtime;
mod trip_updates;
mod vehicle_positions;

use std::env;
use std::thread;

fn main() {
    let db_url = env::var("DATABASE_URL").expect("No DATABASE_URL ENV variable.");
    let db1 = helpers::get_db(&db_url);
    let db2 = helpers::get_db(&db_url);

    thread::spawn(move || {
        vehicle_positions::run(&db1);
    });

    trip_updates::run(&db2);
}
