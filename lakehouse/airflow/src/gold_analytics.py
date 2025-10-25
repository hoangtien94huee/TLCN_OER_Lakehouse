#!/usr/bin/env python3
"""
Gold Layer Analytics Builder
============================

Transforms Silver OER resources into analytics-ready fact and dimension tables.
Focus: Program curriculum coverage - answering "How much OER is available for our programs?"

Business Questions Answerable:
1. What % of subjects in Program X have OER available?
2. Which programs have the most/least OER coverage?
3. How many OER resources match each subject?
4. Which sources (MIT, OpenStax, OTL) provide most relevant OER?
5. What languages are OER available in?
"""

from __future__ import annotations

import os
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


class GoldAnalyticsBuilder:
    """Build Gold layer analytics tables from Silver OER resources."""

    def __init__(self) -> None:
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to build the gold layer")

        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
        self.gold_database = os.getenv("GOLD_DATABASE", "analytics")

        # Silver tables
        self.silver_oer_table = f"{self.silver_catalog}.{self.silver_database}.oer_resources"
        self.reference_programs_table = f"{self.silver_catalog}.{self.silver_database}.reference_programs"
        self.reference_subjects_table = f"{self.silver_catalog}.{self.silver_database}.reference_subjects"
        self.reference_program_subject_links_table = (
            f"{self.silver_catalog}.{self.silver_database}.reference_program_subject_links"
        )
        self.reference_faculties_table = f"{self.silver_catalog}.{self.silver_database}.reference_faculties"

        # Gold tables
        self.dim_programs_table = f"{self.gold_catalog}.{self.gold_database}.dim_programs"
        self.dim_subjects_table = f"{self.gold_catalog}.{self.gold_database}.dim_subjects"
        self.dim_sources_table = f"{self.gold_catalog}.{self.gold_database}.dim_sources"
        self.dim_languages_table = f"{self.gold_catalog}.{self.gold_database}.dim_languages"
        self.dim_date_table = f"{self.gold_catalog}.{self.gold_database}.dim_date"
        self.fact_program_coverage_table = f"{self.gold_catalog}.{self.gold_database}.fact_program_coverage"
        self.fact_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.fact_oer_resources"

        self.spark = self._create_spark_session()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.gold_catalog}.{self.gold_database}")
        print(f"Gold Analytics Builder initialized: {self.gold_catalog}.{self.gold_database}")

    def _create_spark_session(self) -> SparkSession:
        session = (
            SparkSession.builder.appName("OER-Gold-Analytics")
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
        """Build all Gold layer tables."""
        print("=" * 80)
        print("Building Gold Analytics Layer")
        print("=" * 80)

        # Load source data
        oer_df = self._load_table(self.silver_oer_table, "Silver OER Resources")
        programs_df = self._load_table(self.reference_programs_table, "Programs")
        subjects_df = self._load_table(self.reference_subjects_table, "Subjects")
        program_subject_links_df = self._load_table(
            self.reference_program_subject_links_table, "Program-Subject Links"
        )
        faculties_df = self._load_table(self.reference_faculties_table, "Faculties")

        if oer_df is None:
            print("❌ No OER data available, skipping Gold layer build")
            return

        # Build dimensions
        print("\n" + "=" * 80)
        print("Building Dimensions")
        print("=" * 80)
        
        dim_programs = self._build_dim_programs(programs_df, faculties_df)
        dim_subjects = self._build_dim_subjects(subjects_df, faculties_df)
        dim_sources = self._build_dim_sources(oer_df)
        dim_languages = self._build_dim_languages(oer_df)
        dim_date = self._build_dim_date(oer_df)

        # Build facts
        print("\n" + "=" * 80)
        print("Building Fact Tables")
        print("=" * 80)
        
        fact_program_coverage = self._build_fact_program_coverage(
            oer_df, programs_df, subjects_df, program_subject_links_df
        )
        fact_oer_resources = self._build_fact_oer_resources(oer_df, dim_subjects, dim_sources, dim_languages, dim_date)

        # Write tables
        print("\n" + "=" * 80)
        print("Writing Gold Tables")
        print("=" * 80)
        
        self._write_table(dim_programs, self.dim_programs_table, "dim_programs")
        self._write_table(dim_subjects, self.dim_subjects_table, "dim_subjects")
        self._write_table(dim_sources, self.dim_sources_table, "dim_sources")
        self._write_table(dim_languages, self.dim_languages_table, "dim_languages")
        self._write_table(dim_date, self.dim_date_table, "dim_date")
        self._write_table(fact_program_coverage, self.fact_program_coverage_table, "fact_program_coverage")
        self._write_table(fact_oer_resources, self.fact_oer_resources_table, "fact_oer_resources")

        print("\n" + "=" * 80)
        print("✅ Gold Analytics Layer Build Complete!")
        print("=" * 80)

    def _load_table(self, table_name: str, description: str) -> Optional[DataFrame]:
        """Load a table from Silver catalog."""
        try:
            df = self.spark.table(table_name)
            if df.rdd.isEmpty():
                print(f"⚠️  {description}: Table empty")
                return None
            count = df.count()
            print(f"✅ {description}: Loaded {count:,} records")
            return df
        except Exception as exc:
            print(f"⚠️  {description}: Table not found ({table_name})")
            return None

    def _build_dim_programs(self, programs_df: Optional[DataFrame], faculties_df: Optional[DataFrame]) -> DataFrame:
        """
        Dimension: Programs (Curriculum/Study Programs)
        
        Business use: Group coverage by program
        """
        if programs_df is None:
            # Return empty dimension
            return self.spark.createDataFrame(
                [],
                T.StructType([
                    T.StructField("program_key", T.IntegerType(), False),
                    T.StructField("program_id", T.IntegerType(), False),
                    T.StructField("program_name", T.StringType(), True),
                    T.StructField("program_code", T.StringType(), True),
                    T.StructField("faculty_name", T.StringType(), True),
                    T.StructField("published_year", T.StringType(), True),
                ])
            )

        # Join with faculties
        if faculties_df is not None:
            dim = programs_df.join(
                faculties_df,
                programs_df.faculty_id == faculties_df.faculty_id,
                "left"
            ).select(
                F.col("program_id").alias("program_key"),
                F.col("program_id"),
                F.col("program_name"),
                F.col("program_code"),
                F.col("faculty_name"),
                F.col("published_year"),
            )
        else:
            dim = programs_df.select(
                F.col("program_id").alias("program_key"),
                F.col("program_id"),
                F.col("program_name"),
                F.col("program_code"),
                F.lit(None).cast(T.StringType()).alias("faculty_name"),
                F.col("published_year"),
            )

        return dim.dropDuplicates(["program_id"]).orderBy("program_id")

    def _build_dim_subjects(self, subjects_df: Optional[DataFrame], faculties_df: Optional[DataFrame]) -> DataFrame:
        """
        Dimension: Subjects (Curriculum Subjects)
        
        Business use: Track which subjects have OER
        """
        if subjects_df is None:
            return self.spark.createDataFrame(
                [],
                T.StructType([
                    T.StructField("subject_key", T.IntegerType(), False),
                    T.StructField("subject_id", T.IntegerType(), False),
                    T.StructField("subject_name", T.StringType(), True),
                    T.StructField("subject_name_en", T.StringType(), True),
                    T.StructField("subject_code", T.StringType(), True),
                    T.StructField("faculty_name", T.StringType(), True),
                ])
            )

        if faculties_df is not None:
            dim = subjects_df.join(
                faculties_df,
                subjects_df.faculty_id == faculties_df.faculty_id,
                "left"
            ).select(
                F.col("subject_id").alias("subject_key"),
                F.col("subject_id"),
                F.col("subject_name"),
                F.col("subject_name_en"),
                F.col("subject_code"),
                F.col("faculty_name"),
            )
        else:
            dim = subjects_df.select(
                F.col("subject_id").alias("subject_key"),
                F.col("subject_id"),
                F.col("subject_name"),
                F.col("subject_name_en"),
                F.col("subject_code"),
                F.lit(None).cast(T.StringType()).alias("faculty_name"),
            )

        return dim.dropDuplicates(["subject_id"]).orderBy("subject_id")

    def _build_dim_sources(self, oer_df: DataFrame) -> DataFrame:
        """
        Dimension: OER Sources (MIT OCW, OpenStax, OTL)
        
        Business use: Compare sources for relevance
        """
        sources = oer_df.select(
            F.col("source_system"),
            F.col("catalog_provider"),
        ).dropDuplicates()

        window = Window.orderBy("source_system")
        return sources.select(
            F.row_number().over(window).alias("source_key"),
            F.col("source_system").alias("source_code"),
            F.col("catalog_provider").alias("source_name"),
        )

    def _build_dim_languages(self, oer_df: DataFrame) -> DataFrame:
        """
        Dimension: Languages
        
        Business use: Filter OER by language
        """
        languages = oer_df.select(
            F.coalesce(F.col("language"), F.lit("unknown")).alias("language_code")
        ).dropDuplicates()

        window = Window.orderBy("language_code")
        return languages.select(
            F.row_number().over(window).alias("language_key"),
            F.col("language_code"),
        )

    def _build_dim_date(self, oer_df: DataFrame) -> DataFrame:
        """
        Dimension: Date
        
        Business use: Time-series analysis (OER added over time, freshness)
        Grain: One row per date
        """
        # Extract all dates from OER (publication, last_updated, scraped, ingested)
        dates_df = oer_df.select(
            F.to_date(F.col("publication_date")).alias("date")
        ).union(
            oer_df.select(F.to_date(F.col("last_updated_at")).alias("date"))
        ).union(
            oer_df.select(F.to_date(F.col("scraped_at")).alias("date"))
        ).union(
            oer_df.select(F.to_date(F.col("ingested_at")).alias("date"))
        ).where(F.col("date").isNotNull()).dropDuplicates()

        # Build date dimension with attributes
        dim_date = dates_df.select(
            F.col("date").alias("date_key"),
            F.col("date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.dayofmonth("date").alias("day"),
            F.dayofweek("date").alias("day_of_week"),
            F.weekofyear("date").alias("week_of_year"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "EEEE").alias("day_name"),
        ).orderBy("date")

        return dim_date

    def _build_fact_program_coverage(
        self,
        oer_df: DataFrame,
        programs_df: Optional[DataFrame],
        subjects_df: Optional[DataFrame],
        program_subject_links_df: Optional[DataFrame],
    ) -> DataFrame:
        """
        Fact Table: Program Coverage
        
        Business Questions:
        1. What % of subjects in Program X have OER?
        2. Which programs have best OER coverage?
        3. How many OER resources per program?
        
        Grain: One row per Program
        """
        if programs_df is None or subjects_df is None or program_subject_links_df is None:
            print("⚠️  Missing reference data, creating empty fact_program_coverage")
            return self.spark.createDataFrame(
                [],
                T.StructType([
                    T.StructField("program_id", T.IntegerType(), False),
                    T.StructField("program_name", T.StringType(), True),
                    T.StructField("total_subjects", T.IntegerType(), True),
                    T.StructField("subjects_with_oer", T.IntegerType(), True),
                    T.StructField("coverage_pct", T.DoubleType(), True),
                    T.StructField("total_oer_resources", T.IntegerType(), True),
                    T.StructField("avg_oer_per_subject", T.DoubleType(), True),
                ])
            )

        # Explode matched_subjects from OER to get subject-level matches
        oer_matched = oer_df.select(
            F.col("resource_uid"),
            F.explode_outer("matched_subjects").alias("matched_subject")
        ).select(
            F.col("resource_uid"),
            F.col("matched_subject.subject_id").alias("subject_id"),
        ).where(F.col("subject_id").isNotNull())

        # Count OER per subject
        oer_per_subject = oer_matched.groupBy("subject_id").agg(
            F.countDistinct("resource_uid").alias("oer_count")
        )

        # Join program → subjects → OER count
        program_subjects = program_subject_links_df.join(
            subjects_df,
            "subject_id",
            "inner"
        ).join(
            oer_per_subject,
            "subject_id",
            "left"
        ).select(
            F.col("program_id"),
            F.col("subject_id"),
            F.coalesce(F.col("oer_count"), F.lit(0)).alias("oer_count"),
        )

        # Aggregate by program
        program_coverage = program_subjects.groupBy("program_id").agg(
            F.count("subject_id").alias("total_subjects"),
            F.sum(F.when(F.col("oer_count") > 0, 1).otherwise(0)).alias("subjects_with_oer"),
            F.sum("oer_count").alias("total_oer_resources"),
        ).withColumn(
            "coverage_pct",
            F.round((F.col("subjects_with_oer") / F.col("total_subjects")) * 100, 2)
        ).withColumn(
            "avg_oer_per_subject",
            F.round(F.col("total_oer_resources") / F.col("total_subjects"), 2)
        )

        # Join program name
        fact = program_coverage.join(
            programs_df.select("program_id", "program_name"),
            "program_id",
            "left"
        ).select(
            "program_id",
            "program_name",
            "total_subjects",
            "subjects_with_oer",
            "coverage_pct",
            "total_oer_resources",
            "avg_oer_per_subject",
        ).orderBy(F.desc("coverage_pct"))

        return fact

    def _build_fact_oer_resources(
        self,
        oer_df: DataFrame,
        dim_subjects: DataFrame,
        dim_sources: DataFrame,
        dim_languages: DataFrame,
        dim_date: DataFrame,
    ) -> DataFrame:
        """
        Fact Table: OER Resources (Denormalized)
        
        Business Questions:
        1. Find all OER for subject X
        2. Filter OER by source/language
        3. View OER details for recommendations
        
        Grain: One row per OER resource
        """
        # Join with dimension keys
        fact = oer_df.alias("oer")

        # Join source dimension
        fact = fact.join(
            dim_sources.alias("src"),
            F.col("oer.source_system") == F.col("src.source_code"),
            "left"
        )

        # Join language dimension
        fact = fact.join(
            dim_languages.alias("lang"),
            F.coalesce(F.col("oer.language"), F.lit("unknown")) == F.col("lang.language_code"),
            "left"
        )

        # Join date dimensions for each date column
        fact = fact.join(
            dim_date.alias("pub_date"),
            F.to_date(F.col("oer.publication_date")) == F.col("pub_date.date"),
            "left"
        ).join(
            dim_date.alias("ing_date"),
            F.to_date(F.col("oer.ingested_at")) == F.col("ing_date.date"),
            "left"
        )

        # Select denormalized columns
        fact = fact.select(
            F.col("oer.resource_uid").alias("resource_key"),
            F.col("oer.resource_id"),
            F.col("oer.title"),
            F.col("oer.description"),
            F.col("oer.source_url"),
            F.col("src.source_key"),
            F.col("src.source_name"),
            F.col("lang.language_key"),
            F.col("lang.language_code"),
            F.col("oer.publisher_name"),
            F.col("oer.license_name"),
            F.col("oer.subjects"),
            F.col("oer.keywords"),
            F.col("oer.creator_names"),
            F.col("oer.matched_subjects"),
            F.size(F.col("oer.matched_subjects")).alias("matched_subjects_count"),
            F.col("oer.program_ids"),
            F.size(F.col("oer.program_ids")).alias("matched_programs_count"),
            F.col("oer.data_quality_score"),
            F.col("pub_date.date_key").alias("publication_date_key"),
            F.col("oer.publication_date"),
            F.col("oer.last_updated_at"),
            F.col("oer.scraped_at"),
            F.col("ing_date.date_key").alias("ingested_date_key"),
            F.col("oer.ingested_at"),
        )

        return fact.orderBy(F.desc("data_quality_score"))

    def _write_table(self, df: DataFrame, table_name: str, description: str) -> None:
        """Write DataFrame to Gold Iceberg table."""
        if df is None or df.rdd.isEmpty():
            print(f"⚠️  {description}: No data to write")
            return

        try:
            count = df.count()
            df.writeTo(table_name).using("iceberg").createOrReplace()
            print(f"✅ {description}: Wrote {count:,} records to {table_name}")
        except Exception as exc:
            print(f"❌ {description}: Failed to write - {exc}")


def main() -> None:
    builder = GoldAnalyticsBuilder()
    builder.run()


if __name__ == "__main__":
    main()
