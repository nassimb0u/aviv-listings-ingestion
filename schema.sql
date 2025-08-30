drop database avivdb;
create database avivdb;
\c avivdb;

create type listing_subcategory as enum (
    'SINGLE_FAMILY_HOUSE',
    'APARTMENT',
    'EXCEPTIONAL_PROPERTY',
    'VILLA',
    'DUPLEX_OR_TRIPLEX',
    'TOWN_HOUSE',
    'FARMHOUSE',
    'MANOR',
    'CHALET',
    'SEMIDETACHED_HOUSE',
    'GITE',
    'STORAGE_PRODUCTION',
    'PLOT',
    'OFFICE',
    'PARKING',
    'TRADING',
    'MISCELLANEOUS'
);

create type listing_category as enum (
    'HOUSE',
    'APARTMENT',
    'STORAGE_PRODUCTION',
    'PLOT',
    'OFFICE',
    'PARKING',
    'TRADING',
    'MISCELLANEOUS'
);

create type listing_transaction_type as enum ('SELL', 'RENT');

create domain usmallint as smallint check (value >= 0);
create domain positive_real as real check (value >= 0);

create table item_category (
    subcategory listing_subcategory primary key,
    category listing_category not null
);

create table listing (
    listing_id bigint primary key,
    transaction_type listing_transaction_type not null,
    item_subcategory listing_subcategory not null references item_category(subcategory),
    start_date timestamp not null,
    change_date timestamp not null,
    price positive_real not null,
    area positive_real,
    site_area positive_real,
    floor smallint,
    room_count usmallint,
    balcony_count usmallint,
    terrace_count usmallint,
    has_garden boolean,
    city varchar(255) not null,
    zipcode varchar(5) not null check (zipcode ~ '^\d+$'),
    has_passenger_lift boolean,
    is_new_construction boolean,
    build_year usmallint,
    terrace_area positive_real,
    has_cellar boolean,
    is_furnished boolean,
    check (change_date >= start_date)
);

create index idx_listing_transaction_type on listing (transaction_type);
create index idx_listing_item_subcategory on listing (item_subcategory);
create index idx_listing_change_date on listing (change_date);
create index idx_listing_price on listing (price);
create index idx_listing_area on listing (area);
create index idx_listing_room_count on listing (room_count);
create index idx_listing_city on listing (city);
create index idx_listing_zipcode on listing (zipcode);
create index idx_listing_has_passenger_lift_true on listing (listing_id) where has_passenger_lift = false;
create index idx_listing_has_cellar_true on listing (listing_id) where has_cellar = true;
create index idx_listing_is_furnished_true on listing (listing_id) where is_furnished = true;

create index idx_item_category_category on item_category (category);

create or replace function prevent_start_date_update() returns trigger as $$
begin
    if new.start_date <> old.start_date then
        raise exception 'start_date cannot be modified after creation';
    end if;
    return new;
end;
$$ language plpgsql;

create trigger trg_prevent_start_date_update
before update on listing
for each row execute function prevent_start_date_update();

create or replace function enforce_change_date_increases() returns trigger as $$
begin
    if new.change_date < old.change_date then
        raise exception 'change_date can only increase';
    end if;
    return new;
end;
$$ language plpgsql;

create trigger trg_enforce_change_date_increases
before update on listing
for each row execute function enforce_change_date_increases();

grant connect on database avivdb to aviv_etl;
grant usage on schema public to aviv_etl;
grant select, insert, update, delete on all tables in schema public to aviv_etl;
