"""Spark session factory + explicit schemas (explicit schemas avoid a full extra pass for inference
on multi-GB JSON and make types deterministic)."""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType, MapType, StringType,
                               StructField, StructType)

def get_spark(app="ecommerce", mem=None):
    # Make Spark's Python worker processes use THE SAME interpreter as this script (the virtual environment's).
    # Without this, Spark may start the system "python3" and fail with PYTHON_VERSION_MISMATCH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    mem = mem or os.getenv("SPARK_DRIVER_MEM", "4g")
    spark = (SparkSession.builder.appName(app).master(os.getenv("SPARK_MASTER", "local[*]"))
            .config("spark.driver.memory", mem)
            .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTS", "16"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

GEO = StructType([StructField("city", StringType()), StructField("state", StringType()),
                  StructField("country", StringType()), StructField("ip_address", StringType())])

USERS = StructType([StructField("user_id", StringType()),
    StructField("geo_data", StructType([StructField("city", StringType()), StructField("state", StringType()),
                                        StructField("country", StringType())])),
    StructField("registration_date", StringType()), StructField("last_active", StringType())])

PRODUCTS = StructType([StructField("product_id", StringType()), StructField("name", StringType()),
    StructField("category_id", StringType()), StructField("base_price", DoubleType()),
    StructField("current_stock", LongType()), StructField("is_active", BooleanType()),
    StructField("price_history", ArrayType(StructType([StructField("price", DoubleType()),
                                                       StructField("date", StringType())]))),
    StructField("creation_date", StringType())])

CATEGORIES = StructType([StructField("category_id", StringType()), StructField("name", StringType()),
    StructField("subcategories", ArrayType(StructType([StructField("subcategory_id", StringType()),
        StructField("name", StringType()), StructField("profit_margin", DoubleType())])))])

TRANSACTIONS = StructType([StructField("transaction_id", StringType()), StructField("session_id", StringType()),
    StructField("user_id", StringType()), StructField("timestamp", StringType()),
    StructField("items", ArrayType(StructType([StructField("product_id", StringType()),
        StructField("quantity", LongType()), StructField("unit_price", DoubleType()),
        StructField("subtotal", DoubleType())]))),
    StructField("subtotal", DoubleType()), StructField("discount", DoubleType()),
    StructField("total", DoubleType()), StructField("payment_method", StringType()),
    StructField("status", StringType())])

# cart_contents is a map keyed by product_id (dynamic keys) -> MapType, not a struct
SESSIONS = StructType([StructField("session_id", StringType()), StructField("user_id", StringType()),
    StructField("start_time", StringType()), StructField("end_time", StringType()),
    StructField("duration_seconds", LongType()), StructField("geo_data", GEO),
    StructField("device_profile", StructType([StructField("type", StringType()),
        StructField("os", StringType()), StructField("browser", StringType())])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(StructType([StructField("timestamp", StringType()),
        StructField("page_type", StringType()), StructField("product_id", StringType()),
        StructField("category_id", StringType()), StructField("view_duration", LongType())]))),
    StructField("cart_contents", MapType(StringType(), StructType([StructField("quantity", LongType()),
        StructField("price", DoubleType())]))),
    StructField("conversion_status", StringType()), StructField("referrer", StringType())])

def read_json(spark, path, schema):
    # the generator writes ONE big JSON array per file (not JSON-lines) -> multiLine=true
    return spark.read.option("multiLine", "true").schema(schema).json(path)
