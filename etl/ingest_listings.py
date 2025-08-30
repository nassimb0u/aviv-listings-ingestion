from pyspark.sql import SparkSession
from config import load_config
from logging_utils import get_logger
from transformations import transform_raw_data
import psycopg2
from psycopg2 import sql


def init_spark(cfg):
    builder = SparkSession.builder.appName(
        cfg.spark.get("app_name", "AvivListingIngest")
    )
    if cfg.spark.get("master"):
        builder = builder.master(cfg.spark["master"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.spark.get("logging_level", "INFO"))
    return spark


def psycopg2_properties(cfg):
    return {
        "host": cfg.db["host"],
        "port": cfg.db["port"],
        "database": cfg.db["name"],
        "user": cfg.db["user"],
        "password": cfg.db["password"],
    }


def read_input(spark, cfg):
    fmt = cfg.input.get("format", "json")
    path = cfg.input["listings_path"]
    if fmt == "json":
        json_opts = cfg.input.get("json", {})
        reader = spark.read
        for k, v in json_opts.items():
            reader = reader.option(k, v)
        return reader.json(path)
    if fmt == "csv":
        csv_opts = cfg.input.get("csv", {})
        reader = spark.read
        for k, v in csv_opts.items():
            reader = reader.option(k, v)
        return reader.csv(path)

    raise ValueError(f"Unsupported input format: {fmt}")


# Browse through each partition data
def process_partition(
    partition, connection_properties, table_name, pk_names, columns=None
):
    db_conn = psycopg2.connect(**connection_properties)

    dbc_merge = db_conn.cursor()

    # Browse through each row in the partition
    for row in partition:
        # Call process row function for each record to insert or update the record
        process_row(row, dbc_merge, table_name, pk_names, columns)

    db_conn.commit()
    dbc_merge.close()
    db_conn.close()


def process_row(row, curs_merge, table_name, pk_names, columns=None):
    if columns is None:
        # Preserve deterministic order (Row.asDict() preserves insertion order in recent PySpark)
        columns = list(row.asDict().keys())

    # Build dict of values to bind
    row_dict = row.asDict()
    values = {col: row_dict.get(col) for col in columns}

    table_sql = sql.Identifier(table_name)

    col_idents = [sql.Identifier(c) for c in columns]
    pk_idents = [sql.Identifier(c) for c in pk_names]

    insert_cols_sql = sql.SQL(", ").join(col_idents)
    insert_vals_sql = sql.SQL(", ").join(sql.Placeholder(c) for c in columns)

    # Build update set list (exclude PK columns)
    update_sets = [
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
        for c in columns
        if c not in pk_names
    ]
    if update_sets:
        conflict_sql = sql.SQL("DO UPDATE SET ") + sql.SQL(", ").join(update_sets)
    else:
        conflict_sql = sql.SQL("DO NOTHING")

    query = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({vals}) " "ON CONFLICT ({pks}) {conflict}"
    ).format(
        table=table_sql,
        cols=insert_cols_sql,
        vals=insert_vals_sql,
        pks=sql.SQL(", ").join(pk_idents),
        conflict=conflict_sql,
    )

    curs_merge.execute(query, values)


def write_item_category(df, cfg, logger):
    if df.rdd.isEmpty():
        logger.info("No category rows to write.")
        return

    count = df.count()
    logger.info(f"Writing {count} item_category.")

    df.repartition(cfg.run["repartition_item_category"]).foreachPartition(
        lambda partition: process_partition(
            partition,
            psycopg2_properties(cfg),
            cfg.db["item_category_table"],
            cfg.db["item_category_pk"],
        )
    )


def write_listings(df, cfg, logger):
    count = df.count()
    logger.info(f"Writing {count} listings.")

    df.repartition(cfg.run["repartition_listing"]).foreachPartition(
        lambda partition: process_partition(
            partition,
            psycopg2_properties(cfg),
            cfg.db["listing_table"],
            cfg.db["listing_pk"],
        )
    )


if __name__ == "__main__":
    cfg = load_config()
    spark = init_spark(cfg)
    logger = get_logger(spark)

    logger.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

    logger.info("Starting listings ingestion job.")
    raw_df = read_input(spark, cfg)
    logger.info(f"Loaded raw rows: {raw_df.count()}")

    item_category_df, listings_df = transform_raw_data(raw_df, cfg, logger)

    logger.info(f"Transformed item_category rows: {item_category_df.count()}.")
    logger.info(f"Transformed listing rows: {listings_df.count()}.")

    write_item_category(item_category_df, cfg, logger)

    write_listings(listings_df, cfg, logger)
    logger.info("Ingestion job finished.")
    spark.stop()
