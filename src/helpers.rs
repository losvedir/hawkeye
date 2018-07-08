use postgres::Connection;
use postgres::TlsMode;
use reqwest;
use std::time;

pub fn download_file(url: &str) -> Option<Vec<u8>> {
    let now = time::Instant::now();

    match reqwest::get(url) {
        Ok(mut body) => {
            if body.status().is_success() {
                let mut data: Vec<u8> = vec![];
                if let Ok(_) = body.copy_to(&mut data) {
                    println!("Downloaded {:?} in {:?} ms, is {:?} bytes.", url, elapsed_ms(&now), data.len());
                    return Some(data);
                }
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
    duration.as_secs() * 1000 + (duration.subsec_millis() as u64)
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
    ", &[]).expect("Could not initialize DB.");

    conn
}
