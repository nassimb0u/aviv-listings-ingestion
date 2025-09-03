# AvivListingAPI

## Overview
This FastAPI service exposes read-only endpoints for item categories and property listings. It uses SQLModel on top of SQLAlchemy for typed models and database access.

## Why these tools
- FastAPI: high-performance, automatic OpenAPI docs, and built-in validation.
- SQLModel: combines SQLAlchemy core/ORM with pydantic-style models for clear typed schemas and fast development.
- SQLAlchemy (engine): battle-tested SQL toolkit used by SQLModel for connection pooling and efficient query compilation.

## Data models
- Enums: `listing_subcategory`, `listing_category`, `listing_transaction_type`.
- ItemCategory (table):
  - `subcategory` (PK, enum)
  - `category` (enum)
  - Relationship: listings
- Listing (table):
  - `listing_id` (PK, int)
  - `transaction_type` (enum)
  - `item_subcategory` (FK -> ItemCategory.subcategory)
  - `start_date`, `change_date`, `price`, `area`, `city`, `zipcode`, etc.
- Public (response) schemas mirror table fields for safe read-only responses (`ItemCategoryPublic`, `ItemCategoriesPublic`, `ListingPublic`, `ListingsPublic`).

See `app/models.py` for full definitions.

## Endpoints
Base prefix: `/api/v1`

- GET `/item_categories/`
  - Query: `skip`, `limit`, `category`
  - Response: list of categories and total count
- GET `/item_categories/{subcategory}`
  - Returns single item category

- GET `/listings/`
  - Filtering query params: `subcategory`, `category`, `transaction_type`, `city`, `zipcode`, `min_price`, `max_price`, `min_area`, `max_area`, `min_build_year`, `max_build_year`, `has_build_year`, `has_garden`, `has_passenger_lift`, `is_new_construction`, `has_cellar`, `is_furnished`, `sort_by_change_date`, `skip`, `limit`
  - Response: paginated listings and count
- GET `/listings/{listing_id}`
  - Returns a single listing by id

For the complete reference including request/response schemas, examples and allowed enum values consult the generated OpenAPI docs (see next section).

## OpenAPI / Docs
- Swagger UI: `http://127.0.0.1:8000/docs`
- OpenAPI JSON: `http://127.0.0.1:8000/api/v1/openapi.json`

Use these for the complete API contract and full schema details.

## Running the API (local - recommended)
1) Create a Python virtual environment (recommended) and activate it (bash):

```bash
python -m venv .venv
source .venv/bin/activate
```

2) Create a `.env` file at `app/.env`. Required variables (example):

```env
# api/.env
PROJECT_NAME="Aviv Listings API"
POSTGRES_SERVER=db_server
POSTGRES_PORT=port
POSTGRES_USER=db_user
POSTGRES_PASSWORD=db_password
POSTGRES_DB=db_name
```

3) Install dependencies

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

4) Create the database and tables
- run the provided `schema.sql` in project root and the :

```bash
psql "postgresql://<user>:<pass>@<host>:<port>" -f schema.sql
```

- Run the ingestion pipeline in /etl folder (root of the repository)

5) Start the server (for local development)

- Using the included FastAPI CLI:

```bash
fastapi dev main.py
```

- Or with uvicorn (equivalent):

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

6) Open the docs in your browser (see OpenAPI / Docs above).

## API tests

To test the API run:

```console
$ bash ./scripts/test.sh
```

This will run the test suite with `pytest` and generate a code coverage report.


## Notes
- The project builds the SQLALCHEMY_DATABASE_URI from `.env` using the `psycopg` driver. Ensure your Postgres is reachable with the credentials provided.

- Assumed working directory is /api folder
