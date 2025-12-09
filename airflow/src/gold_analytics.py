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
        self.dim_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.dim_oer_resources"
        self.bridge_oer_subjects_table = f"{self.gold_catalog}.{self.gold_database}.bridge_oer_subjects"
        self.fact_program_coverage_table = f"{self.gold_catalog}.{self.gold_database}.fact_program_coverage"
        self.fact_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.fact_oer_resources"

        self.spark = self._create_spark_session()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.gold_catalog}.{self.gold_database}")
        print(f"Gold Analytics Builder initialized: {self.gold_catalog}.{self.gold_database}")

    def _create_spark_session(self) -> SparkSession:
        # Use local JARs instead of downloading from Maven
        jars_dir = "/opt/airflow/jars"
        local_jars = ",".join([
            f"{jars_dir}/iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
            f"{jars_dir}/hadoop-aws-3.3.4.jar",
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar"
        ])
        
        print(f"[Spark] Using local JARs: {local_jars}")
        
        session = (
            SparkSession.builder.appName("OER-Gold-Analytics")
            .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))  # Use Spark cluster
            .config("spark.jars", local_jars)  # Load JARs into classpath
            .config("spark.driver.extraClassPath", local_jars)  # Add to driver classpath
            .config("spark.executor.extraClassPath", local_jars)  # Add to executor classpath
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
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))  # Reduced for local mode
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
            .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAXRESULTSIZE", "2g"))
            .config("spark.sql.debug.maxToStringFields", "500")  # Increase field limit for complex schemas
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("WARN")
        return session

    def run(self) -> None:
        """Build all Gold layer tables."""
       
        # Load source data
        oer_df = self._load_table(self.silver_oer_table, "Silver OER Resources")
        programs_df = self._load_table(self.reference_programs_table, "Programs")
        subjects_df = self._load_table(self.reference_subjects_table, "Subjects")
        program_subject_links_df = self._load_table(
            self.reference_program_subject_links_table, "Program-Subject Links"
        )
        faculties_df = self._load_table(self.reference_faculties_table, "Faculties")

        if oer_df is None:
            print("Γ¥î No OER data available, skipping Gold layer build")
            return

        # Build dimensions

        
        dim_programs = self._build_dim_programs(programs_df, faculties_df)
        dim_subjects = self._build_dim_subjects(subjects_df, faculties_df)
        dim_sources = self._build_dim_sources(oer_df)
        dim_languages = self._build_dim_languages(oer_df)
        dim_date = self._build_dim_date(oer_df)
        dim_oer_resources = self._build_dim_oer_resources(oer_df)

        # Build facts
        
        fact_program_coverage = self._build_fact_program_coverage(
            oer_df,
            programs_df,
            subjects_df,
            program_subject_links_df,
            dim_programs,
            dim_date,
        )
        fact_oer_resources = self._build_fact_oer_resources(
            oer_df, dim_sources, dim_languages, dim_date
        )
        
        # Build bridge tables (many-to-many relationships)
        # Bridge connects FACT to DIMENSION (Kimball standard)
        bridge_oer_subjects = self._build_bridge_oer_subjects(
            oer_df, fact_oer_resources, dim_subjects
        )

        # Write tables

        
        self._write_table(dim_programs, self.dim_programs_table, "dim_programs")
        self._write_table(dim_subjects, self.dim_subjects_table, "dim_subjects")
        self._write_table(dim_sources, self.dim_sources_table, "dim_sources")
        self._write_table(dim_languages, self.dim_languages_table, "dim_languages")
        self._write_table(dim_date, self.dim_date_table, "dim_date")
        self._write_table(dim_oer_resources, self.dim_oer_resources_table, "dim_oer_resources")
        self._write_table(bridge_oer_subjects, self.bridge_oer_subjects_table, "bridge_oer_subjects")
        self._write_table(fact_program_coverage, self.fact_program_coverage_table, "fact_program_coverage")
        self._write_table(fact_oer_resources, self.fact_oer_resources_table, "fact_oer_resources")

        print("\n" + "=" * 80)
        print("Gold Analytics Layer Build Complete!")
        print("=" * 80)

    def _load_table(self, table_name: str, description: str) -> Optional[DataFrame]:
        """Load a table from Silver catalog."""
        try:
            df = self.spark.table(table_name)
            if df.rdd.isEmpty():
                print(f"  {description}: Table empty")
                return None
            count = df.count()
            print(f" {description}: Loaded {count:,} records")
            
            # Verify data distribution by source_system (for OER table)
            if "oer_resources" in table_name:
                print(f"   Breakdown by source:")
                source_counts = df.groupBy("source_system").count().collect()
                for row in source_counts:
                    print(f"    • {row.source_system}: {row['count']:,} records")
            
            return df
        except Exception as exc:
            print(f"  {description}: Table not found ({table_name})")
            return None

    def _build_dim_programs(self, programs_df: Optional[DataFrame], faculties_df: Optional[DataFrame]) -> DataFrame:
        """
        Dimension: Programs (Curriculum/Study Programs)
        
        Business use: Group coverage by program
        """
        schema = T.StructType(
            [
                T.StructField("program_key", T.LongType(), False),
                T.StructField("program_id", T.IntegerType(), False),
                T.StructField("program_name", T.StringType(), True),
                T.StructField("program_code", T.StringType(), True),
                T.StructField("faculty_name", T.StringType(), True),
                T.StructField("published_year", T.StringType(), True),
                T.StructField("effective_from", T.DateType(), True),
                T.StructField("effective_to", T.DateType(), True),
                T.StructField("is_current", T.BooleanType(), False),
            ]
        )
        if programs_df is None:
            # Return empty dimension
            return self.spark.createDataFrame([], schema)

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

        dim = dim.select(
            F.abs(
                F.xxhash64(
                    F.coalesce(F.col("program_id"), F.lit(0)),
                    F.coalesce(F.col("program_code"), F.lit("")),
                )
            ).alias("program_key"),
            F.col("program_id"),
            F.col("program_name"),
            F.col("program_code"),
            F.col("faculty_name"),
            F.col("published_year"),
        ).withColumn("effective_from", F.current_date()).withColumn(
            "effective_to", F.lit(None).cast(T.DateType())
        ).withColumn("is_current", F.lit(True))

        return (
            dim.dropDuplicates(["program_id"])
            .orderBy("program_id")
            .select(schema.fieldNames())
        )

    def _build_dim_subjects(self, subjects_df: Optional[DataFrame], faculties_df: Optional[DataFrame]) -> DataFrame:
        """
        Dimension: Subjects (Curriculum Subjects)
        
        Business use: Track which subjects have OER
        """
        schema = T.StructType(
            [
                T.StructField("subject_key", T.LongType(), False),
                T.StructField("subject_id", T.IntegerType(), False),
                T.StructField("subject_name", T.StringType(), True),
                T.StructField("subject_name_en", T.StringType(), True),
                T.StructField("subject_code", T.StringType(), True),
                T.StructField("faculty_name", T.StringType(), True),
                T.StructField("effective_from", T.DateType(), True),
                T.StructField("effective_to", T.DateType(), True),
                T.StructField("is_current", T.BooleanType(), False),
            ]
        )
        if subjects_df is None:
            return self.spark.createDataFrame([], schema)

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

        dim = dim.select(
            F.abs(
                F.xxhash64(
                    F.coalesce(F.col("subject_id"), F.lit(0)),
                    F.coalesce(F.col("subject_code"), F.lit("")),
                )
            ).alias("subject_key"),
            F.col("subject_id"),
            F.col("subject_name"),
            F.col("subject_name_en"),
            F.col("subject_code"),
            F.col("faculty_name"),
        ).withColumn("effective_from", F.current_date()).withColumn(
            "effective_to", F.lit(None).cast(T.DateType())
        ).withColumn("is_current", F.lit(True))

        return (
            dim.dropDuplicates(["subject_id"])
            .orderBy("subject_id")
            .select(schema.fieldNames())
        )

    def _build_dim_sources(self, oer_df: DataFrame) -> DataFrame:
        """
        Dimension: OER Sources (MIT OCW, OpenStax, OTL)
        
        Business use: Compare sources for relevance
        """
        sources = oer_df.select(
            F.col("source_system"),
        ).dropDuplicates()

        return sources.select(
            F.abs(
                F.xxhash64(
                    F.coalesce(F.col("source_system"), F.lit("")),
                )
            ).alias("source_key"),
            F.col("source_system").alias("source_code"),
            F.col("source_system").alias("source_name"),
        )

    def _build_dim_languages(self, oer_df: DataFrame) -> DataFrame:
        """
        Dimension: Languages
        
        Business use: Filter OER by language
        """
        languages = oer_df.select(
            F.coalesce(F.col("language"), F.lit("unknown")).alias("language_code")
        ).dropDuplicates()

        return languages.select(
            F.abs(F.xxhash64(F.col("language_code"))).alias("language_key"),
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

        fallback_date = self.spark.range(1).select(F.current_date().alias("date"))
        dates_df = dates_df.union(fallback_date).dropDuplicates()

        # Build date dimension with attributes
        dim_date = dates_df.select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),  # 20251113
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

    def _build_dim_oer_resources(self, oer_df: DataFrame) -> DataFrame:
        """
        Dimension: OER Resources (Descriptive Attributes)
        
        Business use: Store all descriptive information about OER resources
        Grain: One row per OER resource
        """
        dim = oer_df.select(
            F.col("resource_uid").alias("resource_key"),
            F.col("resource_id"),
            F.col("title"),
            F.col("description"),
            F.col("source_url"),
            # Fix Unknown publisher - derive from source_system
            F.when(
                (F.col("publisher_name").isNull()) | (F.col("publisher_name") == "Unknown"),
                F.when(F.col("source_system") == "mit_ocw", "MIT OpenCourseWare")
                 .when(F.col("source_system") == "openstax", "OpenStax")
                 .when(F.col("source_system") == "open+textbook+library", "Open Textbook Library")
                 .when(F.col("source_system") == "otl", "Open Textbook Library")
                 .otherwise(F.col("source_system"))
            ).otherwise(F.col("publisher_name")).alias("publisher_name"),
            F.col("creator_names"),
            F.col("bronze_source_path"),
            F.col("license_name"),
            F.col("license_url"),
        )
        
        return dim.dropDuplicates(["resource_key"]).orderBy("resource_key")

    def _build_fact_program_coverage(
        self,
        oer_df: DataFrame,
        programs_df: Optional[DataFrame],
        subjects_df: Optional[DataFrame],
        program_subject_links_df: Optional[DataFrame],
        dim_programs: DataFrame,
        dim_date: DataFrame,
    ) -> DataFrame:
        """
        Fact Table: Program Coverage
        
        Business Questions:
        1. What % of subjects in Program X have OER?
        2. Which programs have best OER coverage?
        3. How many OER resources per program?
        
        """
        if programs_df is None or subjects_df is None or program_subject_links_df is None:
            print("[Warning] Missing reference data, creating empty fact_program_coverage")
            return self.spark.createDataFrame(
                [],
                T.StructType([
                    T.StructField("program_key", T.LongType(), False),
                    T.StructField("snapshot_date_key", T.IntegerType(), False),
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

        # Join program -> subjects -> OER count
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

        # Join with dim_programs to get program_key (surrogate key)
        dim_program_lookup = dim_programs.select(
            "program_id",
            "program_key",
        ).dropDuplicates(["program_id"])

        fact = program_coverage.join(
            dim_program_lookup,
            "program_id",
            "inner"
        )

        # Create snapshot_date_key as integer YYYYMMDD
        fact = fact.withColumn(
            "snapshot_date_key",
            F.date_format(F.current_date(), "yyyyMMdd").cast("int")
        )

        # Join with dim_date to validate snapshot_date_key exists (Kimball standard)
        fact = fact.join(
            dim_date.select("date_key").alias("snap_date"),
            F.col("snapshot_date_key") == F.col("snap_date.date_key"),
            "left"
        )

        # Select only Foreign Keys and Measures (Kimball standard - NO descriptive attributes)
        fact = fact.select(
            # Foreign Keys to Dimensions (Surrogate Keys)
            F.col("program_key"),
            F.col("snap_date.date_key").alias("snapshot_date_key"),
            # Measures (numeric facts for aggregation)
            F.col("total_subjects").cast(T.IntegerType()),
            F.col("subjects_with_oer").cast(T.IntegerType()),
            F.col("coverage_pct").cast(T.DoubleType()),
            F.col("total_oer_resources").cast(T.IntegerType()),
            F.col("avg_oer_per_subject").cast(T.DoubleType()),
        ).orderBy(F.desc("coverage_pct"))

        return fact

    def _build_fact_oer_resources(
        self,
        oer_df: DataFrame,
        dim_sources: DataFrame,
        dim_languages: DataFrame,
        dim_date: DataFrame,
    ) -> DataFrame:
        """
        Fact Table: OER Resources (Kimball Standard - Keys + Measures Only)
        
        Business Questions:
        1. How many OER resources by source/language?
        2. What's the average quality score?
        3. Count resources by matched subjects (via bridge_oer_subjects)

        """
        # Join with dimension keys
        fact = oer_df.alias("oer")

        # Join source dimension to get source_key (surrogate key)
        fact = fact.join(
            dim_sources.select("source_key", "source_code").alias("src"),
            F.col("oer.source_system") == F.col("src.source_code"),
            "left"
        )

        # Join language dimension to get language_key (surrogate key)
        fact = fact.join(
            dim_languages.select("language_key", "language_code").alias("lang"),
            F.coalesce(F.col("oer.language"), F.lit("unknown")) == F.col("lang.language_code"),
            "left"
        )

        # Create date keys for joining with dim_date (integer YYYYMMDD format)
        fact = fact.withColumn(
            "pub_date_key",
            F.when(
                F.col("oer.publication_date").isNotNull(),
                F.date_format(F.to_date(F.col("oer.publication_date")), "yyyyMMdd").cast("int")
            ).otherwise(None)
        ).withColumn(
            "ing_date_key",
            F.date_format(F.to_date(F.col("oer.ingested_at")), "yyyyMMdd").cast("int")
        )

        # Join with dim_date to validate publication_date_key exists
        fact = fact.join(
            dim_date.select("date_key").alias("pub_date"),
            F.col("pub_date_key") == F.col("pub_date.date_key"),
            "left"
        )

        # Join with dim_date to validate ingested_date_key exists
        fact = fact.join(
            dim_date.select("date_key").alias("ing_date"),
            F.col("ing_date_key") == F.col("ing_date.date_key"),
            "left"
        )

        # Select only keys and measures (Kimball standard - NO arrays, NO descriptive attributes)
        # Normalize data_quality_score to 0-1 scale (handle legacy 0-10 scale)
        fact = fact.select(
            # Primary Key (Degenerate Dimension - links to dim_oer_resources)
            F.col("oer.resource_uid").alias("resource_key"),
            
            # Foreign Keys to Dimensions (Surrogate Keys)
            F.col("src.source_key"),
            F.col("lang.language_key"),
            F.col("pub_date.date_key").alias("publication_date_key"),
            F.col("ing_date.date_key").alias("ingested_date_key"),
            
            # Measures (numeric facts) - Only meaningful aggregatable metrics
            F.size(F.col("oer.matched_subjects")).alias("matched_subjects_count"),
            # Normalize: if score > 1, it's on 0-10 scale, divide by 10
            F.when(
                F.col("oer.data_quality_score") > 1.0,
                F.round(F.col("oer.data_quality_score") / 10.0, 2)
            ).otherwise(
                F.col("oer.data_quality_score")
            ).alias("data_quality_score"),
        )

        return fact.orderBy(F.desc("data_quality_score"))

    def _build_bridge_oer_subjects(
        self,
        oer_df: DataFrame,
        fact_oer_resources: DataFrame,
        dim_subjects: DataFrame,
    ) -> DataFrame:
        """
        Bridge Table: fact_oer_resources ↔ dim_subjects (Kimball Standard)
        
     
        """
        # Get valid resource_keys from fact table (ensures referential integrity)
        valid_resource_keys = fact_oer_resources.select("resource_key").distinct()
        
        # Explode matched_subjects array to get one row per match
        bridge = oer_df.select(
            F.col("resource_uid").alias("resource_key"),
            F.explode_outer("matched_subjects").alias("matched_subject")
        ).select(
            F.col("resource_key"),
            F.col("matched_subject.subject_id").alias("subject_id"),
            F.col("matched_subject.similarity").alias("similarity_score"),
            F.col("matched_subject.matched_text").alias("matched_text"),
        ).where(F.col("subject_id").isNotNull())
        
        # Join with fact_oer_resources to ensure referential integrity (Kimball standard)
        # Only keep resource_keys that exist in the fact table
        bridge = bridge.join(
            valid_resource_keys,
            "resource_key",
            "inner"
        )
        
        # Join with dim_subjects to get subject_key (surrogate key)
        bridge = bridge.join(
            dim_subjects.select("subject_key", "subject_id").dropDuplicates(["subject_id"]),
            "subject_id",
            "inner"  # Only keep matches that exist in dim_subjects
        ).select(
            # Foreign Key to fact_oer_resources (referential integrity enforced)
            F.col("resource_key"),
            # Foreign Key to dim_subjects (surrogate key - Kimball standard)
            F.col("subject_key"),
            # Weighting factor for bridge aggregations
            F.col("similarity_score"),
            # Degenerate dimension - match context
            F.col("matched_text"),
        )
        
        return bridge.dropDuplicates(["resource_key", "subject_key"]).orderBy("resource_key", F.desc("similarity_score"))

    def _write_table(self, df: DataFrame, table_name: str, description: str) -> None:
        """Write DataFrame to Gold Iceberg table - full refresh strategy with cleanup."""
        if df is None or df.rdd.isEmpty():
            print(f" {description}: No data to write")
            return

        try:
            count = df.count()
            
            # Gold tables are FULL REFRESH (not incremental)
            # Drop existing table to ensure clean state (no stale data)
            print(f" {description}: Dropping existing table for clean refresh...")
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
            except Exception:
                # PURGE might not be supported, try without it
                try:
                    self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                except Exception as drop_err:
                    print(f" {description}: Note - could not drop table: {drop_err}")
            
            print(f" {description}: Creating fresh table with {count:,} records")
            
            # Write fresh data
            df.writeTo(table_name).using("iceberg").create()
            
            print(f" {description}: Wrote {count:,} records to {table_name}")
            
        except Exception as exc:
            print(f" {description}: Failed to write - {exc}")


def main() -> None:
    builder = GoldAnalyticsBuilder()
    builder.run()


if __name__ == "__main__":
    main()
