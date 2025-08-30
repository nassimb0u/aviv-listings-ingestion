from pyspark.sql import DataFrame, functions as F, types as T, Window


def with_timestamps(df: DataFrame, logger) -> DataFrame:
    if not {"start_date", "change_date"}.issubset(df.columns):
        logger.warn(
            "with_timestamps: Required columns start_date/change_date missing; skipping timestamp conversion."
        )
        return df
    logger.info(
        "with_timestamps: Converting start_date and change_date from epoch ms to timestamp."
    )
    return (
        df.withColumn(
            "start_date_ts", F.to_timestamp(F.from_unixtime(F.col("start_date") / 1000))
        )
        .withColumn(
            "change_date_ts",
            F.to_timestamp(F.from_unixtime(F.col("change_date") / 1000)),
        )
        .drop("start_date", "change_date")
        .withColumnRenamed("start_date_ts", "start_date")
        .withColumnRenamed("change_date_ts", "change_date")
    )


def cast_and_rename(df: DataFrame, logger) -> DataFrame:
    logger.info(
        "cast_and_rename: Casting known columns to expected types when present."
    )
    mappings = {
        "listing_id": T.LongType(),
        "transaction_type": T.StringType(),
        "item_subtype": T.StringType(),
        "item_type": T.StringType(),
        "price": T.DoubleType(),
        "area": T.DoubleType(),
        "site_area": T.DoubleType(),
        "floor": T.DoubleType(),
        "room_count": T.DoubleType(),
        "balcony_count": T.DoubleType(),
        "terrace_count": T.DoubleType(),
        "has_garden": T.BooleanType(),
        "city": T.StringType(),
        "zipcode": T.StringType(),
        "has_passenger_lift": T.BooleanType(),
        "is_new_construction": T.BooleanType(),
        "build_year": T.DoubleType(),
        "terrace_area": T.DoubleType(),
        "has_cellar": T.BooleanType(),
        "is_furnished": T.BooleanType(),
    }
    for c, t in mappings.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))
        else:
            logger.debug(f"cast_and_rename: Column {c} not present; skipping.")
    return df


def fill_subtype_from_type(df: DataFrame, logger) -> DataFrame:
    if "item_subtype" not in df.columns or "item_type" not in df.columns:
        logger.warn(
            "fill_subtype_from_type: item_subtype or item_type column missing; skipping."
        )
        return df
    logger.info(
        "fill_subtype_from_type: Filling null item_subtype values with item_type values."
    )
    return df.withColumn(
        "item_subtype",
        F.when(
            F.col("item_subtype").isNull() & F.col("item_type").isNotNull(),
            F.col("item_type"),
        ).otherwise(F.col("item_subtype")),
    )


def normalize_enum(df: DataFrame, cols, logger) -> DataFrame:
    """
    Keep only last segment after '.' for enumerated columns:
      transaction_type, item_type, item_subtype / item_subcategory
    Example: 'ITEM_TYPE.HOUSE.SINGLE_FAMILY_HOUSE' -> 'SINGLE_FAMILY_HOUSE'
    """
    present = [c for c in cols if c in df.columns]
    missing = set(cols) - set(present)
    if missing:
        logger.warn(f"normalize_enum: Missing columns skipped: {sorted(missing)}")
    if not present:
        logger.info(
            "normalize_enum: No applicable columns; returning original DataFrame."
        )
        return df
    logger.info(f"normalize_enum_suffix: Normalizing columns: {cols}")
    for c in present:
        df = df.withColumn(
            c,
            F.when(F.col(c).isNull(), F.col(c)).otherwise(
                F.substring_index(F.col(c), ".", -1)
            ),
        )
    return df


def enforce_non_negative(df: DataFrame, cols, logger) -> DataFrame:
    present = [c for c in cols if c in df.columns]
    missing = set(cols) - set(present)
    if missing:
        logger.warn(f"enforce_non_negative: Missing columns skipped: {sorted(missing)}")
    if not present:
        logger.info(
            "enforce_non_negative: No applicable columns; returning original DataFrame."
        )
        return df
    logger.info(
        f"enforce_non_negative: Enforcing non-negative constraint on columns: {present}"
    )
    expr = " AND ".join([f"({c} IS NULL OR {c} >= 0)" for c in present])
    return df.filter(expr)


def clean_zipcode(
    df: DataFrame,
    regex: str,
    drop_invalid: bool,
    normalize_two_digits_zc: bool,
    logger,
) -> DataFrame:
    if "zipcode" not in df.columns:
        logger.warn("clean_zipcode: zipcode column missing; skipping cleanup.")
        return df
    logger.info(
        f"clean_zipcode: Normalizing zipcode (drop_invalid={drop_invalid}, regex={regex}, normalize_two_digit={normalize_two_digits_zc})."
    )
    zc = F.col("zipcode")
    cleaned = df.withColumn("zipcode", F.regexp_replace(F.trim(zc), "[^0-9]", ""))

    if drop_invalid:
        cleaned = cleaned.filter(F.col("zipcode").rlike(regex))

    if normalize_two_digits_zc:
        # Expand 2-digit (e.g., '75') into French 5-digit form ('75000')
        cleaned = cleaned.withColumn(
            "zipcode",
            F.when(
                F.length("zipcode") == 2, F.concat(F.col("zipcode"), F.lit("000"))
            ).otherwise(F.col("zipcode")),
        )

    return cleaned


def adjust_build_year(df: DataFrame, policy: str, logger) -> DataFrame:
    if "build_year" not in df.columns or "start_date" not in df.columns:
        logger.warn("adjust_build_year: build_year or start_date missing; skipping.")
        return df
    logger.info(f"adjust_build_year: Applying policy '{policy}'.")
    start_year = F.year("start_date")
    if policy == "adjust":
        logger.info(
            "adjust_build_year: Applying 'adjust' policy (truncate build_year values exceeding max start year)."
        )
        logger.debug("adjust_build_year: Computing global max start year.")
        max_start_year = df.agg(
            F.max(F.year("start_date")).alias("max_start_year")
        ).collect()[0]["max_start_year"]
        if max_start_year is None:
            logger.warn(
                "adjust_build_year: No start_date values found; skipping build_year adjustment."
            )
            return df
        logger.info(
            f"adjust_build_year: Global max start year determined as {max_start_year}."
        )
        return df.withColumn("build_year", F.col("build_year").cast("int")).withColumn(
            "build_year",
            F.when(
                (F.col("build_year").isNotNull())
                & (F.col("build_year") > F.lit(max_start_year)),
                F.substring(F.col("build_year").cast("string"), 1, 4).cast("smallint"),
            ).otherwise(F.col("build_year")),
        )
    if policy == "drop":
        return df.filter(
            (F.col("build_year") <= start_year) | F.col("build_year").isNull()
        )
    logger.error(f"adjust_build_year: Unknown policy '{policy}'.")
    raise ValueError(f"Unkown adjust build year policy: {policy}")


def adjust_change_date(df: DataFrame, policy: str, logger) -> DataFrame:
    if not {"start_date", "change_date"}.issubset(df.columns):
        logger.warn("adjust_change_date: start_date or change_date missing; skipping.")
        return df
    logger.info(
        f"adjust_change_date: Enforcing change_date >= start_date with policy '{policy}'."
    )
    if policy == "adjust":
        return df.withColumn(
            "change_date",
            F.when(F.col("change_date").isNull(), F.col("start_date"))
            .when(F.col("change_date") < F.col("start_date"), F.col("start_date"))
            .otherwise(F.col("change_date")),
        )
    if policy == "drop":
        return df.filter(
            F.col("change_date").isNotNull()
            & (F.col("change_date") >= F.col("start_date"))
        )
    logger.error(f"adjust_change_date: Unknown policy '{policy}'.")
    raise ValueError(f"Unknown adjust change date policy: {policy}")


def finalize_types(df: DataFrame, logger) -> DataFrame:
    logger.info("finalize_types: Casting to final storage types (smallint/float).")
    smallint_cols = [
        "floor",
        "room_count",
        "balcony_count",
        "terrace_count",
        "build_year",
    ]
    for c in smallint_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("smallint"))
        else:
            logger.debug(f"finalize_types: smallint column {c} missing; skipping.")
    float_cols = ["price", "area", "site_area", "terrace_area"]
    for c in float_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("float"))
        else:
            logger.debug(f"finalize_types: float column {c} missing; skipping.")
    return df


def deduplicate(df: DataFrame, logger) -> DataFrame:
    if not {"listing_id", "change_date"}.issubset(df.columns):
        logger.warn("deduplicate: listing_id or change_date missing; skipping.")
        return df
    logger.info(
        "deduplicate: Keeping the latest row per listing_id (max change_date, nulls last)."
    )
    w = Window.partitionBy("listing_id").orderBy(F.col("change_date").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_item_category_df(df: DataFrame, logger) -> DataFrame:
    has_subtype = "item_subtype" in df.columns
    has_type = "item_type" in df.columns
    if not (has_subtype or has_type):
        logger.info(
            "build_item_category_df: No item_subtype or item_type columns; returning empty DataFrame."
        )
        return df.sparkSession.createDataFrame(
            [],
            T.StructType(
                [
                    T.StructField("subcategory", T.StringType(), True),
                    T.StructField("category", T.StringType(), True),
                ]
            ),
        )
    logger.info(
        "build_item_category_df: Building distinct (subcategory, category) mapping."
    )
    sub_col = F.col("item_subtype") if has_subtype else F.lit(None).cast("string")
    cat_col = F.col("item_type") if has_type else F.lit(None).cast("string")
    result = (
        df.select(sub_col.alias("subcategory"), cat_col.alias("category"))
        .filter(~F.col("subcategory").isNull())
        .dropDuplicates(["subcategory"])
    )
    return result


def select_listing_columns(df: DataFrame, logger) -> DataFrame:
    logger.info(
        "select_listing_columns: Selecting and ordering listing columns, filling missing with null."
    )
    if "item_subtype" in df.columns:
        df = df.withColumnRenamed("item_subtype", "item_subcategory")
    needed = [
        "listing_id",
        "transaction_type",
        "item_subcategory",
        "start_date",
        "change_date",
        "price",
        "area",
        "site_area",
        "floor",
        "room_count",
        "balcony_count",
        "terrace_count",
        "has_garden",
        "city",
        "zipcode",
        "has_passenger_lift",
        "is_new_construction",
        "build_year",
        "terrace_area",
        "has_cellar",
        "is_furnished",
    ]
    for c in needed:
        if c not in df.columns:
            logger.debug(f"select_listing_columns: Adding missing column {c} as null.")
            df = df.withColumn(c, F.lit(None))
    return df.select(*needed)


def transform_raw_data(df: DataFrame, cfg, logger) -> DataFrame:
    logger.info("transform_listings: Starting listings transformation pipeline.")
    df = cast_and_rename(df, logger)
    if cfg.transform.get("normalize_enum", True):
        df = normalize_enum(
            df, ["transaction_type", "item_type", "item_subtype"], logger
        )
    df = fill_subtype_from_type(df, logger)
    if cfg.transform.get("date_as_unixtime", True):
        df = with_timestamps(df, logger)
    if cfg.transform.get("enforce_non_negative", True):
        logger.info("transform_listings: enforce_non_negative enabled.")
        df = enforce_non_negative(
            df,
            [
                "price",
                "area",
                "site_area",
                "room_count",
                "balcony_count",
                "terrace_count",
                "build_year",
                "terrace_area",
            ],
            logger,
        )
    else:
        logger.info("transform_listings: enforce_non_negative disabled.")
    df = clean_zipcode(
        df,
        cfg.transform.get("zipcode_regex", "^\d+$"),
        cfg.transform.get("drop_invalid_zipcode", True),
        cfg.transform.get("normalize_two_digits_zc", True),
        logger,
    )
    df = adjust_build_year(
        df, cfg.transform.get("adjust_build_year_policy", "adjust"), logger
    )
    df = adjust_change_date(
        df, cfg.transform.get("adjust_change_date_policy", "adjust"), logger
    )
    df = finalize_types(df, logger)
    if cfg.transform.get("deduplicate", False):
        logger.info("transform_listings: Deduplication enabled.")
        df = deduplicate(df, logger)
    else:
        logger.info("transform_listings: Deduplication disabled.")
    item_category_df = build_item_category_df(df, logger)
    listings_df = select_listing_columns(df, logger)
    logger.info("transform_listings: Completed transformation pipeline.")
    return item_category_df, listings_df
