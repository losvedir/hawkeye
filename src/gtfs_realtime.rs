#[derive(Deserialize)]
pub struct FeedMessage {
    pub header: FeedHeader,
    pub entity: Vec<FeedEntity>,
}

#[derive(Deserialize)]
pub struct FeedHeader {
    pub timestamp: Option<u64>
}

#[derive(Deserialize)]
pub struct FeedEntity {
    pub id: String,
    pub is_deleted: Option<bool>,
    pub trip_update: Option<TripUpdate>,
    pub vehicle: Option<VehiclePosition>
}

#[derive(Deserialize)]
pub struct TripUpdate {
    pub trip: TripDescriptor,
    pub vehicle: Option<VehicleDescriptor>,
    pub stop_time_update: Vec<TripUpdateStopTimeUpdate>,
    pub timestamp: Option<u64>,
}

#[derive(Deserialize)]
pub struct TripUpdateStopTimeEvent {
    pub delay: Option<i32>,
    pub time: Option<i64>,
    pub uncertainty: Option<i32>,
}

#[derive(Deserialize)]
pub struct TripUpdateStopTimeUpdate {
    pub stop_sequence: Option<u32>,
    pub stop_id: Option<String>,
    pub arrival: Option<TripUpdateStopTimeEvent>,
    pub departure: Option<TripUpdateStopTimeEvent>,
    pub schedule_relationship: Option<String>, /* Enum? */
    pub boarding_status: Option<String>,
}

#[derive(Deserialize)]
pub struct VehiclePosition {
    pub trip: Option<TripDescriptor>,
    pub vehicle: Option<VehicleDescriptor>,
    pub current_stop_sequence: Option<u32>,
    pub stop_id: Option<String>,
    pub current_status: Option<String>, /* Enum? */
    pub timestamp: Option<u64>,
}

#[derive(Deserialize)]
pub struct TripDescriptor {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<u32>,
    pub schedule_relationship: Option<String>, /* Enum? */
}

#[derive(Deserialize)]
pub struct VehicleDescriptor {
    pub id: Option<String>,
    pub label: Option<String>,
}