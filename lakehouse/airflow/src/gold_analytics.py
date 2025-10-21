#!/usr/bin/env python3
"""
Gold Layer Snowflake Builder
============================

Transforms the normalized Dublin Core documents from the silver layer into a
purposeful snowflake warehouse. Dimensions are designed for matching open
resources to courses and institutions without pre-computed bridge tables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = DataFrame = None  # type: ignore


@dataclass(frozen=True)
class SourceMetadata:
    code: str
    name: str
    full_name: str
    base_url: str
    description: str
    scraping_frequency: str


SOURCE_LOOKUP: Dict[str, SourceMetadata] = {
    "mit_ocw": SourceMetadata(
        code="mit_ocw",
        name="MIT OCW",
        full_name="MIT OpenCourseWare",
        base_url="https://ocw.mit.edu",
        description="MIT lecture materials and self-paced courses published online.",
        scraping_frequency="daily",
    ),
    "openstax": SourceMetadata(
        code="openstax",
        name="OpenStax",
        full_name="OpenStax CNX",
        base_url="https://openstax.org",
        description="Open textbooks and teaching guides from Rice University.",
        scraping_frequency="weekly",
    ),
    "otl": SourceMetadata(
        code="otl",
        name="OTL",
        full_name="Open Textbook Library",
        base_url="https://open.umn.edu",
        description="Peer-reviewed open textbooks curated by the University of Minnesota.",
        scraping_frequency="weekly",
    ),
}


class GoldSnowflakeBuilder:
    def __init__(self) -> None:
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to build the gold layer")

        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.silver_table_name = os.getenv("SILVER_TABLE", "oer_resources")
        self.silver_table = f"{self.silver_catalog}.{self.silver_database}.{self.silver_table_name}"

        self.gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
        self.gold_database = os.getenv("GOLD_DATABASE", "analytics")
        self.fact_table_name = os.getenv("GOLD_FACT_TABLE", "fact_oer_resources")

        self.spark = self._create_spark_session()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.gold_catalog}.{self.gold_database}")

    def _create_spark_session(self) -> SparkSession:
        session = (
            SparkSession.builder.appName("OER-Gold-Snowflake")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.silver_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.silver_catalog}.warehouse", f"s3a://{self.bucket}/silver/")
            .config(f"spark.sql.catalog.{self.gold_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.gold_catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.gold_catalog}.warehouse", f"s3a://{self.bucket}/gold/")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("WARN")
        return session

    def run(self) -> None:
        try:
            silver_df = self.spark.table(self.silver_table)
        except Exception:
            legacy_table = f"{self.silver_catalog}.{self.silver_database}.oer_dc_documents"
            print(f"Primary silver table {self.silver_table} unavailable, falling back to legacy table {legacy_table}")
            silver_df = self.spark.table(legacy_table)
            self.silver_table = legacy_table
        else:
            if "subjects" not in silver_df.columns or "keywords" not in silver_df.columns:
                legacy_table = f"{self.silver_catalog}.{self.silver_database}.oer_dc_documents"
                if self.silver_table != legacy_table:
                    print(
                        f"Silver table {self.silver_table} missing array columns, falling back to legacy table {legacy_table}"
                    )
                    silver_df = self.spark.table(legacy_table)
                    self.silver_table = legacy_table
        if silver_df.rdd.isEmpty():
            print("No silver data available to build gold layer")
            return

        silver_df = silver_df.withColumn("primary_subject_lc", F.lower(F.col("primary_subject")))
        silver_df.cache()

        dimensions = self._build_dimensions(silver_df)
        fact_df = self._build_fact_table(silver_df, dimensions)

        self._write_dimensions(dimensions)
        self._write_fact_table(fact_df)

        silver_df.unpersist()

    @staticmethod
    def _empty_array(data_type: T.DataType):
        """Return an empty array column of the requested type."""
        return F.array_remove(F.array(F.lit(None).cast(data_type)), F.lit(None))

    def _build_dimensions(self, silver_df: DataFrame) -> Dict[str, DataFrame]:
        dim_subject_categories = self._dim_subject_categories(silver_df)
        dim_subjects = self._dim_subjects(silver_df, dim_subject_categories)
        dim_resource_types = self._dim_resource_types(silver_df)
        dim_formats = self._dim_formats(silver_df)
        dim_publisher_types = self._dim_publisher_types(silver_df)
        dim_publishers = self._dim_publishers(silver_df, dim_publisher_types)
        dim_sources = self._dim_sources(silver_df)
        dim_languages = self._dim_languages(silver_df)
        dim_licenses = self._dim_licenses(silver_df)
        dim_institutions = self._dim_institutions(silver_df)
        dim_departments = self._dim_departments(silver_df, dim_institutions)
        dim_courses = self._dim_courses(silver_df, dim_departments, dim_subjects)

        return {
            "dim_subject_categories": dim_subject_categories,
            "dim_subjects": dim_subjects,
            "dim_resource_types": dim_resource_types,
            "dim_formats": dim_formats,
            "dim_publisher_types": dim_publisher_types,
            "dim_publishers": dim_publishers,
            "dim_sources": dim_sources,
            "dim_languages": dim_languages,
            "dim_licenses": dim_licenses,
            "dim_institutions": dim_institutions,
            "dim_departments": dim_departments,
            "dim_courses": dim_courses,
        }

    def _dim_subject_categories(self, silver_df: DataFrame) -> DataFrame:
        categories = (
            silver_df.select(
                F.col("subject_category_name").alias("category_name"),
                F.col("subject_category_code").alias("category_code"),
            )
            .where(F.col("category_name").isNotNull())
            .dropDuplicates()
        )

        if categories.rdd.isEmpty():
            categories = self.spark.createDataFrame(
                [("General Studies", "GEN")],
                schema=T.StructType(
                    [
                        T.StructField("category_name", T.StringType(), False),
                        T.StructField("category_code", T.StringType(), False),
                    ]
                ),
            )

        totals = (
            silver_df.where(F.col("subject_category_code").isNotNull())
            .groupBy("subject_category_code")
            .agg(
                F.countDistinct("primary_subject").alias("subject_count"),
                F.count("*").alias("resource_count"),
            )
        )

        window = Window.orderBy("category_name")
        joined = categories.join(
            totals,
            categories.category_code == totals.subject_category_code,
            "left",
        )
        return (
            joined.select(
                F.row_number().over(window).alias("subject_category_key"),
                F.col("category_name"),
                F.col("category_code"),
                F.lit(None).cast(T.IntegerType()).alias("parent_category_key"),
                F.lit(1).alias("hierarchy_level"),
                F.col("category_name").alias("full_path"),
                F.lit(None).cast(T.StringType()).alias("description"),
                F.coalesce(F.col("subject_count"), F.lit(0)).alias("total_subjects"),
                F.coalesce(F.col("resource_count"), F.lit(0)).alias("total_resources"),
            )
        )

    def _dim_subjects(self, silver_df: DataFrame, dim_categories: DataFrame) -> DataFrame:
        subject_base = (
            silver_df.where(F.col("primary_subject").isNotNull())
            .select(
                F.col("primary_subject").alias("subject_name"),
                F.col("subject_category_code"),
                F.col("keywords"),
                F.col("difficulty_level"),
                F.col("course_id"),
            )
        )

        aggregated = (
            subject_base.groupBy("subject_name", "subject_category_code")
            .agg(
                F.array_distinct(F.flatten(F.collect_set("keywords"))).alias("keywords"),
                F.array_distinct(F.collect_set("difficulty_level")).alias("levels"),
                F.count("*").alias("total_resources"),
                F.countDistinct("course_id").alias("total_courses"),
            )
            .fillna({"total_courses": 0})
        )

        categories = dim_categories.select("subject_category_key", "category_code")
        joined = aggregated.join(
            categories,
            aggregated.subject_category_code == categories.category_code,
            "left",
        )

        window = Window.orderBy("subject_name")
        return (
            joined.select(
                F.row_number().over(window).alias("subject_key"),
                F.col("subject_name"),
                F.upper(F.regexp_replace(F.col("subject_name"), "[^A-Za-z0-9]+", "_")).alias("subject_code"),
                F.col("subject_category_key"),
                F.when(F.col("keywords").isNotNull(), F.col("keywords")).otherwise(
                    self._empty_array(T.StringType())
                ).alias("keywords"),
                self._empty_array(T.StringType()).alias("aliases"),
                F.lit(None).cast(T.StringType()).alias("description"),
                F.when(F.col("levels").isNotNull(), F.col("levels")).otherwise(
                    self._empty_array(T.StringType())
                ).alias("suitable_for_levels"),
                F.col("total_resources"),
                F.col("total_courses"),
                F.lit(True).alias("is_active"),
                F.current_timestamp().alias("created_at"),
            )
        )

    def _dim_resource_types(self, silver_df: DataFrame) -> DataFrame:
        mapping = {
            "course": ("Text", "Curriculum", True),
            "textbook": ("Text", "Material", False),
            "learning resource": ("Text", "Material", False),
            "lecture": ("MovingImage", "Media", True),
        }

        types_df = (
            silver_df.select(F.col("resource_type"))
            .where(F.col("resource_type").isNotNull())
            .dropDuplicates()
        )

        if types_df.rdd.isEmpty():
            types_df = self.spark.createDataFrame([("Learning Resource",)], ["resource_type"])

        def resolve(type_name: str) -> tuple:
            key = (type_name or "").lower()
            return mapping.get(key, ("Text", "Material", False))

        resolve_udf = F.udf(
            resolve,
            T.StructType(
                [
                    T.StructField("dcmi_type", T.StringType()),
                    T.StructField("category", T.StringType()),
                    T.StructField("multimedia_expected", T.BooleanType()),
                ]
            ),
        )

        window = Window.orderBy("resource_type")
        enriched = types_df.withColumn("attrs", resolve_udf(F.col("resource_type")))
        return (
            enriched.select(
                F.row_number().over(window).alias("resource_type_key"),
                F.col("resource_type").alias("type_name"),
                F.col("attrs.dcmi_type"),
                F.col("attrs.category"),
                F.lit(None).cast(T.DoubleType()).alias("typical_duration_hours"),
                F.col("attrs.multimedia_expected"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_formats(self, silver_df: DataFrame) -> DataFrame:
        mime_lookup = {
            "pdf": "application/pdf",
            "html": "text/html",
            "video": "video/mp4",
            "audio": "audio/mpeg",
            "interactive": "application/json",
        }

        formats = (
            silver_df.select(
                F.col("resource_format").alias("format_name"),
                F.col("resource_format_category").alias("category"),
            )
            .where(F.col("format_name").isNotNull())
            .dropDuplicates()
        )

        if formats.rdd.isEmpty():
            formats = self.spark.createDataFrame([("unknown", "Document")], ["format_name", "category"])

        def mime_for(format_name: str) -> Optional[str]:
            if not format_name:
                return None
            return mime_lookup.get(format_name.lower())

        mime_udf = F.udf(mime_for, T.StringType())

        window = Window.orderBy("format_name")
        return (
            formats.select(
                F.row_number().over(window).alias("format_key"),
                F.col("format_name"),
                mime_udf(F.col("format_name")).alias("mime_type"),
                F.lit(True).alias("is_digital"),
                F.when(F.lower(F.col("format_name")).isin("video", "interactive"), True)
                .otherwise(False)
                .alias("requires_special_tool"),
                self._empty_array(T.StringType()).alias("suitable_for_subjects"),
                self._empty_array(T.StringType()).alias("suitable_for_levels"),
                F.col("category"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_publisher_types(self, silver_df: DataFrame) -> DataFrame:
        types_df = (
            silver_df.select(F.col("publisher_type"))
            .where(F.col("publisher_type").isNotNull())
            .dropDuplicates()
        )

        if types_df.rdd.isEmpty():
            types_df = self.spark.createDataFrame([("Platform",)], ["publisher_type"])

        def profile(ptype: str) -> tuple:
            lower = (ptype or "").lower()
            if "university" in lower:
                return True, 0.1, "University or academic publisher"
            if "non-profit" in lower:
                return True, 0.15, "Mission-driven non-profit publisher"
            if "library" in lower:
                return True, 0.12, "Library or public knowledge institution"
            return False, 0.4, "General content platform"

        profile_udf = F.udf(
            profile,
            T.StructType(
                [
                    T.StructField("peer_review_typical", T.BooleanType()),
                    T.StructField("commercial_bias", T.DoubleType()),
                    T.StructField("description", T.StringType()),
                ]
            ),
        )

        window = Window.orderBy("publisher_type")
        enriched = types_df.withColumn("profile", profile_udf(F.col("publisher_type")))
        return (
            enriched.select(
                F.row_number().over(window).alias("publisher_type_key"),
                F.col("publisher_type"),
                F.col("profile.peer_review_typical"),
                F.col("profile.commercial_bias"),
                F.col("profile.description"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_publishers(self, silver_df: DataFrame, dim_publisher_types: DataFrame) -> DataFrame:
        totals = silver_df.groupBy("publisher_name").agg(F.count("*").alias("total_resources"))
        base = (
            silver_df.select("publisher_name", "publisher_type", "source_system")
            .where(F.col("publisher_name").isNotNull())
            .dropDuplicates()
            .join(totals, "publisher_name", "left")
        )

        types_lookup = dim_publisher_types.select("publisher_type_key", "publisher_type")
        joined = base.join(types_lookup, "publisher_type", "left")

        def trust_score(ptype: str) -> float:
            if not ptype:
                return 0.6
            lower = ptype.lower()
            if "university" in lower or "library" in lower:
                return 0.95
            if "non-profit" in lower:
                return 0.9
            return 0.7

        def peer_review(ptype: str) -> str:
            if not ptype:
                return "Medium"
            lower = ptype.lower()
            if "university" in lower or "library" in lower:
                return "High"
            if "non-profit" in lower:
                return "Medium"
            return "Low"

        trust_udf = F.udf(trust_score, T.DoubleType())
        peer_udf = F.udf(peer_review, T.StringType())

        website_udf = F.udf(
            lambda name: {
                "mit opencourseware": "https://ocw.mit.edu",
                "openstax": "https://openstax.org",
                "open textbook library": "https://open.umn.edu/opentextbooks",
            }.get((name or "").lower()),
            T.StringType(),
        )

        window = Window.orderBy("publisher_name")
        return (
            joined.select(
                F.row_number().over(window).alias("publisher_key"),
                F.col("publisher_name"),
                F.col("publisher_type_key"),
                F.col("source_system"),
                trust_udf(F.col("publisher_type")).alias("trustworthiness_score"),
                peer_udf(F.col("publisher_type")).alias("peer_review_level"),
                website_udf(F.col("publisher_name")).alias("website_url"),
                F.coalesce(F.col("total_resources"), F.lit(0)).alias("total_resources"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_sources(self, silver_df: DataFrame) -> DataFrame:
        sources = (
            silver_df.select(F.col("source_system").alias("source_system_code"))
            .where(F.col("source_system").isNotNull())
            .dropDuplicates()
        )
        if sources.rdd.isEmpty():
            sources = self.spark.createDataFrame([("unknown",)], ["source_system_code"])

        def lookup(code: str) -> SourceMetadata:
            return SOURCE_LOOKUP.get(
                (code or "unknown").lower(),
                SourceMetadata(
                    code=code or "unknown",
                    name=(code or "unknown").upper(),
                    full_name=code or "Unknown Source",
                    base_url="",
                    description="Unknown content source",
                    scraping_frequency="monthly",
                ),
            )

        lookup_udf = F.udf(
            lambda code: (
                (lambda meta: (
                    meta.code,
                    meta.name,
                    meta.full_name,
                    meta.base_url,
                    meta.description,
                    meta.scraping_frequency,
                ))(lookup(code))
            ),
            T.StructType(
                [
                    T.StructField("code", T.StringType()),
                    T.StructField("name", T.StringType()),
                    T.StructField("full_name", T.StringType()),
                    T.StructField("base_url", T.StringType()),
                    T.StructField("description", T.StringType()),
                    T.StructField("scraping_frequency", T.StringType()),
                ]
            ),
        )

        window = Window.orderBy("source_system_code")
        enriched = sources.withColumn("meta", lookup_udf(F.col("source_system_code")))
        return (
            enriched.select(
                F.row_number().over(window).alias("source_system_key"),
                F.col("meta.code").alias("source_system_code"),
                F.col("meta.name").alias("source_name"),
                F.col("meta.full_name").alias("full_name"),
                F.col("meta.base_url").alias("base_url"),
                F.col("meta.description").alias("description"),
                F.lit(True).alias("is_active"),
                F.col("meta.scraping_frequency").alias("scraping_frequency"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_languages(self, silver_df: DataFrame) -> DataFrame:
        languages = (
            silver_df.select(F.col("language").alias("language_code"))
            .where(F.col("language").isNotNull())
            .dropDuplicates()
        )
        if languages.rdd.isEmpty():
            languages = self.spark.createDataFrame([("en",)], ["language_code"])

        def metadata(code: str) -> tuple:
            if not code:
                return ("Unknown", False, "Latin")
            lower = code.lower()
            if lower in {"en", "eng"}:
                return ("English", True, "Latin")
            if lower in {"vi", "vie"}:
                return ("Vietnamese", True, "Latin")
            if lower in {"es", "spa"}:
                return ("Spanish", True, "Latin")
            return (code.upper(), False, "Latin")

        meta_udf = F.udf(
            metadata,
            T.StructType(
                [
                    T.StructField("language_name", T.StringType()),
                    T.StructField("is_primary", T.BooleanType()),
                    T.StructField("script_type", T.StringType()),
                ]
            ),
        )

        window = Window.orderBy("language_code")
        enriched = languages.withColumn("meta", meta_udf(F.col("language_code")))
        return (
            enriched.select(
                F.row_number().over(window).alias("language_key"),
                F.col("language_code"),
                F.col("meta.language_name").alias("language_name"),
                F.col("meta.is_primary"),
                F.col("meta.script_type"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_licenses(self, silver_df: DataFrame) -> DataFrame:
        licenses = (
            silver_df.select("license_name", "license_url")
            .where(F.col("license_name").isNotNull())
            .dropDuplicates()
        )
        if licenses.rdd.isEmpty():
            licenses = self.spark.createDataFrame(
                [("All Rights Reserved", None)],
                ["license_name", "license_url"],
            )

        def profile(name: str) -> tuple:
            if not name:
                return (False, False, True, False, 0.2)
            lower = name.lower()
            if "cc0" in lower or "public domain" in lower:
                return (True, True, False, False, 1.0)
            if "cc by-sa" in lower:
                return (True, True, True, True, 0.95)
            if "cc by" in lower:
                return (True, True, True, False, 0.9)
            if "cc by-nc" in lower:
                return (False, True, True, False, 0.75)
            return (False, False, True, False, 0.4)

        profile_udf = F.udf(
            profile,
            T.StructType(
                [
                    T.StructField("commercial_use", T.BooleanType()),
                    T.StructField("derivative_works", T.BooleanType()),
                    T.StructField("attribution_required", T.BooleanType()),
                    T.StructField("share_alike_required", T.BooleanType()),
                    T.StructField("openness_score", T.DoubleType()),
                ]
            ),
        )

        window = Window.orderBy("license_name")
        enriched = licenses.withColumn("meta", profile_udf(F.col("license_name")))
        return (
            enriched.select(
                F.row_number().over(window).alias("license_key"),
                F.col("license_name"),
                F.col("license_url"),
                F.col("meta.commercial_use"),
                F.col("meta.derivative_works"),
                F.col("meta.attribution_required"),
                F.col("meta.share_alike_required"),
                F.col("meta.openness_score"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_institutions(self, silver_df: DataFrame) -> DataFrame:
        institutions = (
            silver_df.select(F.col("institution_name"))
            .where(F.col("institution_name").isNotNull())
            .dropDuplicates()
        )
        if institutions.rdd.isEmpty():
            institutions = self.spark.createDataFrame(
                [("Unknown Institution",)],
                ["institution_name"],
            )

        def meta(name: str) -> tuple:
            if not name:
                return ("Organization", "Unknown", None, False)
            lower = name.lower()
            if "mit" in lower:
                return ("University", "United States", "https://www.mit.edu", True)
            if "rice" in lower or "openstax" in lower:
                return ("University", "United States", "https://www.rice.edu", True)
            if "minnesota" in lower:
                return ("University", "United States", "https://twin-cities.umn.edu", True)
            return ("Organization", "Unknown", None, False)

        meta_udf = F.udf(
            meta,
            T.StructType(
                [
                    T.StructField("institution_type", T.StringType()),
                    T.StructField("country", T.StringType()),
                    T.StructField("website_url", T.StringType()),
                    T.StructField("is_accredited", T.BooleanType()),
                ]
            ),
        )

        window = Window.orderBy("institution_name")
        enriched = institutions.withColumn("meta", meta_udf(F.col("institution_name")))
        return (
            enriched.select(
                F.row_number().over(window).alias("institution_key"),
                F.col("institution_name"),
                F.col("meta.institution_type"),
                F.col("meta.country"),
                F.col("meta.website_url"),
                F.col("meta.is_accredited"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_departments(self, silver_df: DataFrame, dim_institutions: DataFrame) -> DataFrame:
        departments = (
            silver_df.select("department_name", "institution_name")
            .where(F.col("department_name").isNotNull())
            .dropDuplicates()
        )
        if departments.rdd.isEmpty():
            return self.spark.createDataFrame(
                [],
                T.StructType(
                    [
                        T.StructField("department_key", T.IntegerType(), False),
                        T.StructField("department_name", T.StringType(), True),
                        T.StructField("department_code", T.StringType(), True),
                        T.StructField("institution_key", T.IntegerType(), True),
                        T.StructField("parent_department_key", T.IntegerType(), True),
                        T.StructField("academic_field", T.StringType(), True),
                        T.StructField("total_courses", T.IntegerType(), True),
                        T.StructField("total_resources", T.IntegerType(), True),
                        T.StructField("created_at", T.TimestampType(), True),
                        T.StructField("updated_at", T.TimestampType(), True),
                    ]
                ),
            )

        totals = silver_df.groupBy("department_name").agg(
            F.countDistinct("course_id").alias("total_courses"),
            F.count("*").alias("total_resources"),
        )

        institutions = dim_institutions.select("institution_key", "institution_name")
        joined = departments.join(institutions, "institution_name", "left").join(totals, "department_name", "left")

        window = Window.orderBy("department_name")
        return (
            joined.select(
                F.row_number().over(window).alias("department_key"),
                F.col("department_name"),
                F.upper(F.regexp_replace(F.col("department_name"), "[^A-Za-z0-9]+", "_")).alias("department_code"),
                F.col("institution_key"),
                F.lit(None).cast(T.IntegerType()).alias("parent_department_key"),
                F.col("department_name").alias("academic_field"),
                F.coalesce(F.col("total_courses"), F.lit(0)).alias("total_courses"),
                F.coalesce(F.col("total_resources"), F.lit(0)).alias("total_resources"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _dim_courses(
        self,
        silver_df: DataFrame,
        dim_departments: DataFrame,
        dim_subjects: DataFrame,
    ) -> DataFrame:
        courses = (
            silver_df.select(
                "course_id",
                "course_name",
                "course_code",
                "department_name",
                "primary_subject",
                "difficulty_level",
                "target_audience",
                "subjects",
                "keywords",
            )
            .where(F.col("course_id").isNotNull())
            .dropDuplicates(["course_id"])
        )

        if courses.rdd.isEmpty():
            return self.spark.createDataFrame(
                [],
                T.StructType(
                    [
                        T.StructField("course_key", T.IntegerType(), False),
                        T.StructField("course_id", T.StringType(), True),
                        T.StructField("course_name", T.StringType(), True),
                        T.StructField("course_code", T.StringType(), True),
                        T.StructField("department_key", T.IntegerType(), True),
                        T.StructField("primary_subject_key", T.IntegerType(), True),
                        T.StructField("level", T.StringType(), True),
                        T.StructField("target_audience", T.StringType(), True),
                        T.StructField("learning_objectives", T.ArrayType(T.StringType()), True),
                        T.StructField("topics_covered", T.ArrayType(T.StringType()), True),
                        T.StructField("related_subjects", T.ArrayType(T.StringType()), True),
                        T.StructField("is_core_course", T.BooleanType(), True),
                        T.StructField("prerequisites", T.ArrayType(T.StringType()), True),
                        T.StructField("total_hours", T.IntegerType(), True),
                        T.StructField("num_modules", T.IntegerType(), True),
                        T.StructField("is_active", T.BooleanType(), True),
                        T.StructField("created_at", T.TimestampType(), True),
                        T.StructField("updated_at", T.TimestampType(), True),
                    ]
                ),
            )

        departments = dim_departments.select("department_key", "department_name")
        subjects = dim_subjects.select("subject_key", "subject_name")
        joined = (
            courses.join(departments, "department_name", "left")
            .join(subjects, courses.primary_subject == subjects.subject_name, "left")
        )

        window = Window.orderBy("course_id")
        return (
            joined.select(
                F.row_number().over(window).alias("course_key"),
                F.col("course_id"),
                F.col("course_name"),
                F.col("course_code"),
                F.col("department_key"),
                F.col("subject_key").alias("primary_subject_key"),
                F.col("difficulty_level").alias("level"),
                F.col("target_audience"),
                self._empty_array(T.StringType()).alias("learning_objectives"),
                F.when(F.col("subjects").isNotNull(), F.col("subjects")).otherwise(
                    self._empty_array(T.StringType())
                ).alias("topics_covered"),
                F.when(F.col("keywords").isNotNull(), F.col("keywords")).otherwise(
                    self._empty_array(T.StringType())
                ).alias("related_subjects"),
                F.lit(False).alias("is_core_course"),
                self._empty_array(T.StringType()).alias("prerequisites"),
                F.lit(None).cast(T.IntegerType()).alias("total_hours"),
                F.lit(None).cast(T.IntegerType()).alias("num_modules"),
                F.lit(True).alias("is_active"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )

    def _build_fact_table(self, silver_df: DataFrame, dims: Dict[str, DataFrame]) -> DataFrame:
        subject_lookup = dims["dim_subjects"].select(
            "subject_key",
            "subject_category_key",
            F.lower(F.col("subject_name")).alias("subject_name_lc"),
        )
        format_lookup = dims["dim_formats"].select(
            "format_key", F.lower(F.col("format_name")).alias("format_name_lc")
        )
        resource_type_lookup = dims["dim_resource_types"].select(
            "resource_type_key", F.lower(F.col("type_name")).alias("resource_type_lc")
        )
        publisher_lookup = dims["dim_publishers"].select(
            "publisher_key", "publisher_type_key", F.lower(F.col("publisher_name")).alias("publisher_name_lc")
        )
        source_lookup = dims["dim_sources"].select(
            "source_system_key", F.lower(F.col("source_system_code")).alias("source_system_code")
        )
        language_lookup = dims["dim_languages"].select(
            "language_key", F.lower(F.col("language_code")).alias("language_code")
        )
        license_lookup = dims["dim_licenses"].select(
            "license_key", F.lower(F.col("license_name")).alias("license_name_lc")
        )
        course_lookup = dims["dim_courses"].select("course_key", "course_id")

        joined = (
            silver_df.alias("s")
            .join(subject_lookup, F.col("s.primary_subject_lc") == subject_lookup.subject_name_lc, "left")
            .join(format_lookup, F.lower(F.col("s.resource_format")) == format_lookup.format_name_lc, "left")
            .join(
                resource_type_lookup,
                F.lower(F.col("s.resource_type")) == resource_type_lookup.resource_type_lc,
                "left",
            )
            .join(
                publisher_lookup,
                F.lower(F.col("s.publisher_name")) == publisher_lookup.publisher_name_lc,
                "left",
            )
            .join(source_lookup, F.lower(F.col("s.source_system")) == source_lookup.source_system_code, "left")
            .join(language_lookup, F.lower(F.col("s.language")) == language_lookup.language_code, "left")
            .join(
                license_lookup,
                F.lower(F.col("s.license_name")) == license_lookup.license_name_lc,
                "left",
            )
            .join(course_lookup, "course_id", "left")
        )

        processed_at = F.current_timestamp()
        window = Window.orderBy("resource_uid")
        length_bytes = F.length(F.col("description").cast("string")).cast("long")
        word_count = F.size(F.split(F.col("description"), r"\s+")).cast("int")
        keyword_count = F.size(F.col("keywords"))
        searchability = F.round(F.col("data_quality_score") + F.when(keyword_count > 5, 0.2).otherwise(0.1), 3)

        return joined.select(
            F.row_number().over(window).alias("resource_key"),
            F.col("resource_id"),
            F.col("source_system_key"),
            F.col("subject_key"),
            F.col("subject_category_key"),
            F.col("format_key"),
            F.col("resource_type_key"),
            F.col("publisher_key"),
            F.col("publisher_type_key"),
            F.col("language_key"),
            F.col("license_key"),
            F.col("course_key"),
            F.col("title"),
            F.col("description"),
            F.col("creator_names"),
            F.col("subjects").alias("all_subjects"),
            F.col("keywords"),
            F.col("difficulty_level"),
            F.col("target_audience"),
            length_bytes.alias("content_length_bytes"),
            word_count.alias("word_count"),
            F.lit(None).cast(T.IntegerType()).alias("page_count"),
            F.lit(None).cast(T.IntegerType()).alias("estimated_hours"),
            F.col("data_quality_score"),
            F.col("data_quality_score").alias("content_completeness"),
            F.when(searchability > 1.0, F.lit(1.0)).otherwise(searchability).alias("searchability_score"),
            F.col("source_url"),
            F.col("source_url").alias("resource_url"),
            F.col("ingested_at"),
            processed_at.alias("processed_at"),
            F.lit(True).alias("is_active"),
        )

    def _write_dimensions(self, dimensions: Dict[str, DataFrame]) -> None:
        for name, df in dimensions.items():
            table_fqn = f"{self.gold_catalog}.{self.gold_database}.{name}"
            df.createOrReplaceTempView(name)
            self.spark.sql(
                f"""
                CREATE OR REPLACE TABLE {table_fqn}
                USING iceberg
                AS SELECT * FROM {name}
                """
            )
            print(f"Dimension {table_fqn} refreshed with {df.count()} rows")

    def _write_fact_table(self, fact_df: DataFrame) -> None:
        table_fqn = f"{self.gold_catalog}.{self.gold_database}.{self.fact_table_name}"
        fact_df.createOrReplaceTempView("fact_oer_resources_view")
        self.spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_fqn}
            USING iceberg
            PARTITIONED BY (source_system_key, DATE(ingested_at))
            AS SELECT * FROM fact_oer_resources_view
            """
        )
        print(f"Fact table {table_fqn} refreshed with {fact_df.count()} rows")


def main() -> None:
    builder = GoldSnowflakeBuilder()
    builder.run()


if __name__ == "__main__":
    main()


class GoldAnalyticsStandalone(GoldSnowflakeBuilder):
    """Backward-compatible alias for DAG imports."""
