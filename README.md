# Hawkeye - monitoring accuracy of MBTA GTFS RT predictions

Needs a postgres database. On macOS, [Postgres.app](https://postgresapp.com/) is a great way to install and use it. I also recommend [Postico](https://eggerapps.at/postico/) as a friendly GUI to explore the tables. You will need to create a DB for hawkeye, e.g. with the `createdb` command.

Build with `$ cargo build --release` or for macOS download a precompiled [release].

Expects a `DATABASE_URL` that points to the postgres DB you set up for it.

For example, if you've compiled the app yourself, run it with:

```
$ env DATABASE_URL=postgres://user@localhost:5432/db_name target/release/hawkeye
```

The app creates and maintains two tables: `vehicle_movements` and `predictions`.

The `vehicle_movements` table records when a train arrives at and departs from a stop. This is inferred from the VehiclePositions.pb file, when the status goes to and from STOPPED_AT, so is correct to within the resolution of how often we fetch the file (currently about every 2 - 2.5 seconds).

Note that there's no concept of trip in this table, sicne there's no concept of trip in VehiclePositions. That means, the max table size is (# of unique vehicle IDs) x (number of stops they can be at). If a vehicle visits a stop that it's been to in the past, the row in the DB will be updated.

The `predictions` table stores the predictions made in the `TripUpdates.pb` file, which is downloaded once a minute. Every minute, we create a new record with `file_at` (the timestamp of when the PB file was downloaded), the `trip_id`, `vehicle_id`, `stop_id`, `stop_sequence`, and `predicted_arrive_at` and `predicted_depart_at`. Every couple seconds when the `vehicle_movements` table is updated, if a vehicle newly arrives at or departs from a stop, all of the predictions for that stop and vehicle have their `actual_arrive_at` and `actual_depart_at` timestamps updated.
