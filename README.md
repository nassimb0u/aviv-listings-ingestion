# Aviv Listings Ingestion
## Overview
A compact Spark-based ETL that ingests real-estate listings into a PostgreSQL database. The pipeline reads raw JSON/CSV files, cleans and normalizes records, and upserts them into two normalized tables: `item_category` and `listing`.

Primary implementation: `etl/ingest_listings.py` (entrypoint) and `etl/transformations.py` (transform logic).

## Tools choice
- PostgreSQL
  - Open source ACID-compliant, supports enums, domains, indexes and triggers required by the domain model. Use it to store normalized, queryable data and enforce integrity rules.
- Apache Spark (PySpark)
  - Handles large, partitioned input efficiently and performs distributed transformations. Chosen for scalability and familiar DataFrame APIs.
- psycopg2
  - Lightweight Python client for PostgreSQL with explicit control over binding and transactions. Unlike PostgreSQL JDBC driver, psycopg2 allows to perform upserts (`INSERT ... ON CONFLICT`) which is more efficient (network and concurrency wise) then a select, insert.

Together these allow parallel ETL with strong storage guarantees and DB-level enforcement of invariants.

## Data model
See `schema.sql` for full DDL. Key points:
- Enums: `listing_category`, `listing_subcategory`, `listing_transaction_type` (`SELL` / `RENT`).
- Table `item_category`
  - PK: `subcategory`
  - Columns: `subcategory`, `category`.
- Table `listing`
  - PK: `listing_id`
  - Columns: `transaction_type`, `item_subcategory` (FK → `item_category.subcategory`), `start_date`, `change_date`, `price`, `area`, `room_count`, `city`, `zipcode`, boolean flags, etc.
  - Constraints: non-negative domains, `change_date >= start_date`, zipcode format. Triggers prevent modification of `start_date` and enforce monotonic `change_date`.
- Indexes for common filters (transaction_type, change_date, price, city, zipcode, boolean flags).

## Pipeline stages
1. Read
   - `read_input` reads JSON or CSV from the configured `listings_path`.
2. Transform
   - `transform_raw_data` applies casting, enum normalization, timestamp conversion, zipcode cleaning, non-negative enforcement, build_date/change_date policies, final typing, and optional deduplication.
   - Produces `item_category_df` and `listings_df`.
   - Parameterised in config.yaml -> transform.  
3. Validate & log
   - Counts and basic presence checks are logged; empty DataFrames are skipped for writes.
   - Good to have: the use of Great Expectations to validate data and detect anomalies.

4. Write (idempotent, partitioned)
   - `write_item_category` and `write_listings` repartition DataFrames and call `foreachPartition`.
   - Each partition opens a `psycopg2` connection and upserts rows using `INSERT ... ON CONFLICT ... DO UPDATE/DO NOTHING`.
5. Finish
   - Spark context is stopped and final logs emitted.

## Idempotency & integrity
- Upserts prevent duplicate primary keys on re-run.
- DB triggers enforce business rules that cannot be violated by downstream updates (immutable `start_date`, monotonic `change_date`).
- Schema domains and checks (e.g., non-negative, zipcode pattern) ensure stored data validity.

## Configuration & running
- Main script: `etl/ingest_listings.py`.
- Config: `etl/config.yaml` (Dataset location, Spark options, transform policies, repartition sizes).
- .env: for database credentials. Required variables:

```env
# api/.env
DB_HOST=host
DB_PORT=port
DB_NAME=dbname
DB_USER=user
DB_PASSWORD=password

```
- Schema DDL: `schema.sql` — run this in PostgreSQL to create the database, types, tables, indexes, triggers, and grants.
- Requirements (for ETL): `etl/requirements.txt` (PySpark, psycopg2, etc.).

Run example:
- Ensure PostgreSQL is up and `schema.sql` applied and user has privileges.
- Ensure `etl/config.yaml` is populated with dataset location and transformation policies, .env file with DB creds.
- Execute with a Python environment where PySpark and psycopg2 are available:

  spark-submit ingest_listings.py

## Important files
- `etl/ingest_listings.py` — pipeline entrypoint, DB partition writer.
- `etl/transformations.py` — transformations and validation rules.
- `schema.sql` — full PostgreSQL schema and triggers.
- `etl/config.yaml` & `etl/config.py` — configuration parsing.

## Troubleshooting & tuning
- Spark UI (URL is logged at startup) for job/partition status and executor errors.
- If DB errors occur, inspect PostgreSQL logs and the exception raised by partition workers; triggers raise explicit errors for rule violations.
- Tune `cfg.run.repartition_listing` and `repartition_item_category` to balance Spark parallelism vs database connection/transaction capacity.

## Notes
- The pipeline favors DB-level enforcement for critical invariants; transformation policies (adjust/drop) are configurable via `config.yaml`.
