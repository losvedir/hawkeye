use postgres::Connection;
use postgres::TlsMode;
use reqwest;
use std::time;

pub fn download_file(url: &str) -> Option<String> {
    let now = time::Instant::now();

    match reqwest::get(url) {
        Ok(mut body) => {
            if body.status().is_success() {
                println!("Downloaded {:?} in {:?} ms.", url, elapsed_ms(&now));
                return body.text().ok();
            } else {
                println!("Error downloading {:?}: response was {:?}.", url, body.status());
            }
        }
        Err(err) => println!("Error downloading {:?}: {:?}", url, err)
    }

    return None;
}

pub fn elapsed_ms(start: &time::Instant) -> u64 {
    let duration = start.elapsed();
    duration.as_secs() * 1000000 + (duration.subsec_micros() as u64)
}

pub fn get_db(db_url: &str) -> Connection {
    let conn = Connection::connect(db_url, TlsMode::None).expect("Could not connect to given DATABASE_URL.");
    conn.execute("
        CREATE TABLE IF NOT EXISTS vehicle_movements (
            vehicle_id varchar not null,
            stop_id varchar not null,
            arrived_at timestamptz,
            departed_at timestamptz,
            primary key (vehicle_id, stop_id)
        )
    ", &[]).expect("Could not initialize vehicle_movements DB table.");

    conn.execute("
        CREATE TABLE IF NOT EXISTS predictions (
            file_at timestamptz not null,
            trip_id varchar not null,
            vehicle_id varchar not null,
            stop_id varchar not null,
            direction_id int not null,
            stop_sequence int not null,
            predicted_arrive_at timestamptz,
            predicted_depart_at timestamptz,
            boarding_status varchar,
            nth_at_stop int,
            actual_arrive_at timestamptz,
            actual_depart_at timestamptz,
            primary key (file_at, trip_id, vehicle_id, stop_id)
        )
    ", &[]).expect("Could not initialize predictions DB table.");

    conn.execute("
        CREATE INDEX IF NOT EXISTS predictions_update_arrival
        ON predictions (vehicle_id, stop_id, actual_arrive_at)
        WHERE actual_arrive_at IS NULL
    ", &[]).expect("Could not add predictions_update_arrival index");

    conn.execute("
        CREATE INDEX IF NOT EXISTS predictions_update_departure
        ON predictions (vehicle_id, stop_id, actual_depart_at)
        WHERE actual_depart_at IS NULL
    ", &[]).expect("Could not add predictions_update_departure index");

    conn.execute("
        CREATE INDEX IF NOT EXISTS predictions_file_at_idx
        ON predictions (file_at)
    ", &[]).expect("Could not add predictions_file_at_idx");

    conn
}
