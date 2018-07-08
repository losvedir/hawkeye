# Hawkeye - monitoring accuracy of GTFS RT predictions

Needs a postgres database.

Run with

```
$ env DATABASE_URL=postgres://user@localhost:5432/db_name cargo run --release
```

The `vehicle_movements` table records when a train arrives at and departs from a stop. This is inferred from the VehiclePositions.pb file, when the status goes to and from STOPPED_AT, so is correct to within the resolution of how often we fetch the file (currently about every 2 - 2.5 seconds).

Note that there's no concept of trip in this table, sicne there's no concept of trip in VehiclePositions. That means, the max table size is (# of unique vehicle IDs) x (number of stops they can be at). If a vehicle visits a stop that it's been to in the past, the row in the DB will be updated.
