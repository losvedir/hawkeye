#[derive(Debug)]
pub struct Movement {
  vehicle_id: String,
  stop_id: String,
  arrived_at: u32,
  departed_at: u32,
}
