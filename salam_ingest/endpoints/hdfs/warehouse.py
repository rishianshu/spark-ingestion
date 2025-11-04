from __future__ import annotations

import time
import uuid
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.types import NullType
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.window import Window as W


class HiveHelper:
    """Helpers for managing Hive-compatible external tables."""

    _HIVE_TYPE_MAP = {
        "ByteType": "TINYINT",
        "ShortType": "SMALLINT",
        "IntegerType": "INT",
        "LongType": "BIGINT",
        "FloatType": "FLOAT",
        "DoubleType": "DOUBLE",
        "BinaryType": "BINARY",
        "BooleanType": "BOOLEAN",
        "StringType": "STRING",
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
    }

    @staticmethod
    def table_exists(spark: SparkSession, db: str, table_name: str) -> bool:
        return spark._jsparkSession.catalog().tableExists(db, table_name)

    @staticmethod
    def _spark_type_to_hive(dt) -> str:
        s = dt.simpleString()
        if s.startswith("decimal("):
            return "DECIMAL" + s[len("decimal") :]
        if s.startswith("array<") or s.startswith("map<") or s.startswith("struct<"):
            return "STRING"
        return HiveHelper._HIVE_TYPE_MAP.get(dt.__class__.__name__, "STRING")

    @staticmethod
    def _cols_ddl_from_df_schema(df_schema, partition_col: str = "load_date") -> str:
        cols = []
        for field in df_schema.fields:
            if field.name == partition_col:
                continue
            cols.append(f"`{field.name}` {HiveHelper._spark_type_to_hive(field.dataType)}")
        return ", ".join(cols) if cols else "`_dummy` STRING"

    @staticmethod
    def create_external_if_absent(
        spark: SparkSession,
        db: str,
        table_name: str,
        location: str,
        partitioned: bool = False,
        partition_col: str = "load_date",
    ) -> None:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        if partitioned:
            spark.sql(
                f"""
              CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table_name}` (_dummy STRING)
              PARTITIONED BY ({partition_col} STRING)
              STORED AS PARQUET
              LOCATION '{location}'
            """
            )
            try:
                spark.sql(
                    f"ALTER TABLE `{db}`.`{table_name}` REPLACE COLUMNS ({partition_col} STRING)"
                )
            except Exception:
                pass
        else:
            spark.sql(
                f"""
              CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table_name}`
              STORED AS PARQUET
              LOCATION '{location}'
              AS SELECT * FROM (SELECT 1 as __bootstrap) t WHERE 1=0
            """
            )

    @staticmethod
    def add_partition_or_msck(
        spark: SparkSession,
        db: str,
        table_name: str,
        partition_col: str,
        load_date: str,
        base_location: str,
        use_msck: bool = False,
    ) -> None:
        part_loc = f"{base_location}/{partition_col}={load_date}"
        if use_msck:
            spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table_name}`")
        else:
            spark.sql(
                f"""
              ALTER TABLE `{db}`.`{table_name}`
              ADD IF NOT EXISTS PARTITION ({partition_col}='{load_date}')
              LOCATION '{part_loc}'
            """
            )

    @staticmethod
    def set_location(spark: SparkSession, db: str, schema: str, table: str, new_location: str) -> str:
        hive_table = f"{db}.{schema}__{table}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        spark.sql(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table}
            STORED AS PARQUET
            LOCATION '{new_location}'
            AS SELECT * FROM (SELECT 1 AS _bootstrap) t LIMIT 0
        """
        )
        spark.sql(f"ALTER TABLE {hive_table} SET LOCATION '{new_location}'")
        return hive_table

    @staticmethod
    def create_partitioned_parquet_if_absent_with_schema(
        spark: SparkSession,
        db: str,
        schema: str,
        table: str,
        base_location: str,
        sample_partition_path: str,
        partition_col: str = "load_date",
    ) -> None:
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        sample_df = spark.read.parquet(sample_partition_path).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(sample_df.schema, partition_col=partition_col)
        spark.sql(
            f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          PARTITIONED BY (`{partition_col}` STRING)
          STORED AS PARQUET
          LOCATION '{base_location}'
        """
        )

    @staticmethod
    def create_unpartitioned_parquet_if_absent_with_schema(
        spark: SparkSession,
        db: str,
        schema: str,
        table: str,
        base_location: str,
    ) -> None:
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        df = spark.read.parquet(base_location).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(df.schema, partition_col="load_date")
        spark.sql(
            f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          STORED AS PARQUET
          LOCATION '{base_location}'
        """
        )


class IcebergHelper:
    """Helper routines for interacting with Apache Iceberg sinks."""

    @staticmethod
    def _identifier(schema: str, table: str) -> str:
        normalized_schema = (schema or "").strip().lower()
        normalized_table = (table or "").strip().lower()
        return f"{normalized_schema}__{normalized_table}"

    @staticmethod
    def setup_catalog(spark: SparkSession, cfg: Dict[str, Any]) -> None:
        inter = cfg["runtime"]["intermediate"]
        cat = inter["catalog"]
        if spark.conf.get(f"spark.sql.catalog.{cat}", None):
            return
        catalog_type = inter.get("catalog_type", "hadoop").lower()
        spark.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        if catalog_type == "hive":
            uri = inter.get("metastore_uri")
            if not uri:
                try:
                    uri = spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris")
                except Exception:
                    uri = None
            if not uri:
                raise ValueError(
                    "Iceberg Hive catalog requires metastore URI. Set runtime.intermediate.metastore_uri or hive.metastore.uris."
                )
            spark.conf.set(f"spark.sql.catalog.{cat}.type", "hive")
            spark.conf.set(f"spark.sql.catalog.{cat}.uri", uri)
            warehouse = inter.get("warehouse")
            if warehouse:
                spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", warehouse)
        else:
            warehouse = inter.get("warehouse")
            if not warehouse:
                raise ValueError("Hadoop catalog configuration requires runtime.intermediate.warehouse")
            spark.conf.set(f"spark.sql.catalog.{cat}.type", "hadoop")
            spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", warehouse)
        spark.conf.set(f"spark.sql.catalog.{cat}.case-sensitive", "true")

    @staticmethod
    def ensure_namespace(spark: SparkSession, cfg: Dict[str, Any]) -> None:
        inter = cfg["runtime"]["intermediate"]
        cat = inter.get("catalog")
        db = inter.get("db")
        if not cat or not db:
            return
        namespace_parts = [cat, *IcebergHelper._namespace_parts(db)]
        namespace_stmt = ".".join(
            IcebergHelper._format_part(part, force_quotes=(idx > 0))
            for idx, part in enumerate(namespace_parts)
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_stmt}")

    @staticmethod
    def _drop_legacy_table(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
    ) -> None:
        inter = cfg["runtime"]["intermediate"]
        db = inter.get("db")
        if not db:
            return
        tbl_ident = IcebergHelper._identifier(schema, table)
        try:
            spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{tbl_ident}`")
        except Exception:
            # Ignore drop failures; creation will surface any remaining errors
            pass

    @staticmethod
    def _namespace_parts(namespace: Any) -> List[str]:
        if namespace is None:
            return []
        if isinstance(namespace, (list, tuple)):
            parts: List[str] = []
            for entry in namespace:
                parts.extend(IcebergHelper._namespace_parts(entry))
            return parts
        text = str(namespace).strip().replace("`", "")
        if not text:
            return []
        return [segment for segment in text.split(".") if segment]

    @staticmethod
    def _format_part(part: str, *, force_quotes: bool = False) -> str:
        if not force_quotes and part.isidentifier() and "." not in part:
            return part
        return f"`{part}`"

    @staticmethod
    def qualified_name(
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        *,
        quoted: bool = False,
    ) -> str:
        inter = cfg["runtime"]["intermediate"]
        catalog = str(inter["catalog"])
        namespace_parts = IcebergHelper._namespace_parts(inter.get("db"))
        table_part = IcebergHelper._identifier(schema, table)
        parts = [catalog, *namespace_parts, table_part]
        formatted: List[str] = []
        for idx, part in enumerate(parts):
            force = quoted and (idx > 0 or not part.isidentifier() or "." in part)
            formatted.append(IcebergHelper._format_part(part, force_quotes=force))
        return ".".join(formatted)

    @staticmethod
    def _partition_clause(partition_col: str, partition_cfg: Optional[Dict[str, Any]]) -> str:
        spec_entries = []
        if partition_cfg and isinstance(partition_cfg, dict):
            spec_entries = partition_cfg.get("spec") or []
        if spec_entries:
            rendered = [
                IcebergHelper._render_partition_transform(entry)
                for entry in spec_entries
                if isinstance(entry, dict)
            ]
            rendered = [item for item in rendered if item]
            if rendered:
                return f" PARTITIONED BY ({', '.join(rendered)})"
        if partition_col:
            return f" PARTITIONED BY (`{partition_col}`)"
        return ""

    @staticmethod
    def _render_partition_transform(entry: Dict[str, Any], *, field_name: Optional[str] = None) -> Optional[str]:
        field = field_name or entry.get("field")
        if not field or not isinstance(field, str):
            return None
        transform_raw = entry.get("transform", "identity")
        transform = str(transform_raw).lower() if transform_raw is not None else "identity"
        base, _, suffix = transform.partition(":")
        base = base or "identity"
        needs_quotes = not str(field).isidentifier() or "." in str(field)
        column_ref = f"`{field}`" if needs_quotes else field
        if base in {"identity", ""}:
            return f"identity({column_ref})"
        if base in {"year", "years"}:
            return f"years({column_ref})"
        if base in {"month", "months"}:
            return f"months({column_ref})"
        if base in {"day", "days"}:
            return f"days({column_ref})"
        if base == "bucket":
            param = suffix or "16"
            return f"bucket({param}, {column_ref})"
        if base == "truncate":
            param = suffix or "10"
            return f"truncate({param}, {column_ref})"
        # Fallback to identity if transform is unknown (should be validated earlier)
        return f"identity({column_ref})"

    @staticmethod
    def _qualify_merge_filter(expr: str, schema: StructType) -> Optional[str]:
        if not expr:
            return None
        column_map = {field.name.lower(): field.name for field in schema.fields}
        pattern = re.compile(r"(?<![\w`\.'])([A-Za-z_][A-Za-z0-9_]*)")

        def replacer(match: re.Match[str]) -> str:
            token = match.group(1)
            lookup = column_map.get(token.lower())
            if lookup is None:
                return token
            return f"t.`{lookup}`"

        return pattern.sub(replacer, expr)

    @staticmethod
    def _current_partition_columns(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
    ) -> List[str]:
        qualified = IcebergHelper.qualified_name(cfg, schema, table, quoted=True)
        rows = spark.sql(f"DESCRIBE EXTENDED {qualified}").collect()
        columns: List[str] = []
        capture = False
        for row in rows:
            col_name = str(row[0]) if row[0] is not None else ""
            data_val = str(row[1]) if row[1] is not None else ""
            if col_name == "# Partitioning":
                capture = True
                continue
            if capture:
                if not col_name:
                    break
                if col_name.startswith("Part "):
                    cleaned = data_val.strip().strip('"')
                    if cleaned:
                        columns.append(cleaned)
        return columns

    @staticmethod
    def set_partition_spec(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        partition_cfg: Optional[Dict[str, Any]],
    ) -> bool:
        if not partition_cfg:
            return False
        spec_entries = partition_cfg.get("spec") or []
        if not spec_entries:
            return False
        IcebergHelper.setup_catalog(spark, cfg)
        full = IcebergHelper.qualified_name(cfg, schema, table, quoted=True)
        table_schema: Optional[StructType]
        try:
            table_schema = spark.table(full).schema
        except AnalysisException:
            table_schema = None
        name_map: Dict[str, str] = {}
        if table_schema is not None:
            name_map = {f.name.lower(): f.name for f in table_schema.fields}
        rendered: List[str] = []
        normalized_specs: Dict[str, str] = {}
        for entry in spec_entries:
            if not isinstance(entry, dict):
                continue
            raw_field = entry.get("field")
            if not isinstance(raw_field, str):
                continue
            actual_field = name_map.get(raw_field.lower(), raw_field)
            spec_str = IcebergHelper._render_partition_transform(entry, field_name=actual_field)
            if spec_str is None:
                continue
            rendered.append(spec_str)
            inner = spec_str.split("(", 1)[1].rsplit(")", 1)[0]
            col_arg = inner.split(",")[-1].strip().strip("`")
            normalized_specs[col_arg] = spec_str
        if not rendered:
            return False
        current_cols = IcebergHelper._current_partition_columns(spark, cfg, schema, table)

        changed = False
        for col in current_cols:
            if col not in normalized_specs:
                spark.sql(f"ALTER TABLE {full} DROP PARTITION FIELD {col}")
                changed = True

        for col, spec in normalized_specs.items():
            if col in current_cols:
                spark.sql(f"ALTER TABLE {full} REPLACE PARTITION FIELD {col} WITH {spec}")
            else:
                spark.sql(f"ALTER TABLE {full} ADD PARTITION FIELD {spec}")
            changed = True

        return changed

    @staticmethod
    def rewrite_data_files(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
    ) -> None:
        inter = cfg["runtime"]["intermediate"]
        cat = inter["catalog"]
        namespace = IcebergHelper._namespace_parts(inter.get("db"))
        tbl_ident = IcebergHelper._identifier(schema, table)
        namespace_literal = ".".join(namespace + [tbl_ident]) if namespace else tbl_ident
        spark.sql(
            f"CALL {cat}.system.rewrite_data_files(table => '{namespace_literal}')"
        )

    @staticmethod
    def ensure_table(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        df: DataFrame,
        partition_col: str,
        partition_cfg: Optional[Dict[str, Any]] = None,
        merge_filter_expr: Optional[str] = None,
    ) -> str:
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        tbl_ident = IcebergHelper._identifier(schema, table)
        full = IcebergHelper.qualified_name(cfg, schema, table, quoted=True)
        IcebergHelper.ensure_namespace(spark, cfg)
        table_is_ready = False
        try:
            spark.table(full)
            table_is_ready = True
        except Exception as exc:
            message = str(exc)
            if "Not an iceberg table" in message or "not an iceberg table" in message.lower():
                IcebergHelper._drop_legacy_table(spark, cfg, schema, table)
            else:
                lowered = message.lower()
                if "not found" in lowered or "table or view not found" in lowered or "no such table" in lowered:
                    pass
                else:
                    # For other failures we surface the exception to callers
                    raise
        if table_is_ready:
            return full
        cols = ", ".join([f"`{c}` {df.schema[c].dataType.simpleString()}" for c in df.columns])
        warehouse = inter.get("warehouse")
        location_clause = ""
        if warehouse:
            table_path = f"{warehouse.rstrip('/')}/{db}/{tbl_ident}"
            location_clause = f" LOCATION '{table_path}'"
        partition_clause = IcebergHelper._partition_clause(partition_col, partition_cfg)
        create_stmt = f"CREATE TABLE IF NOT EXISTS {full} ({cols}) USING iceberg{partition_clause}{location_clause}"
        try:
            spark.sql(create_stmt)
        except Exception as exc:
            message = str(exc)
            if "Not an iceberg table" in message or "not an iceberg table" in message.lower():
                IcebergHelper._drop_legacy_table(spark, cfg, schema, table)
                spark.sql(create_stmt)
            else:
                raise
        return full

    @staticmethod
    def dedup(df: DataFrame, pk_cols: Sequence[str], order_cols_desc: Sequence[str]) -> DataFrame:
        order_exprs = [F.col(c).desc_nulls_last() for c in order_cols_desc]
        if "_ingest_tiebreak" not in df.columns:
            df = df.withColumn("_ingest_tiebreak", F.monotonically_increasing_id())
        order_exprs.append(F.col("_ingest_tiebreak").desc())
        w = W.partitionBy(*[F.col(c) for c in pk_cols]).orderBy(*order_exprs)
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn", "_ingest_tiebreak")
        )

    @staticmethod
    def merge_upsert(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        df_src: DataFrame,
        pk_cols: Sequence[str],
        load_date: str,
        partition_col: str = "load_date",
        incr_col: Optional[str] = None,
        partition_cfg: Optional[Dict[str, Any]] = None,
        merge_filter_expr: Optional[str] = None,
    ) -> str:
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        tbl_ident = IcebergHelper._identifier(schema, table)
        tgt = IcebergHelper.qualified_name(cfg, schema, table, quoted=True)
        IcebergHelper.setup_catalog(spark, cfg)
        if partition_col not in df_src.columns:
            df_src = df_src.withColumn(partition_col, F.to_date(F.lit(load_date), "yyyy-MM-dd"))
        order_cols = [incr_col] if incr_col else []
        order_cols.append(partition_col)
        df_src = IcebergHelper.dedup(df_src, pk_cols, order_cols)
        IcebergHelper.ensure_table(spark, cfg, schema, table, df_src, partition_col, partition_cfg)
        tgt_schema: Optional[StructType] = None
        last_exc: Optional[Exception] = None
        for attempt in range(5):
            try:
                tgt_schema = spark.table(tgt).schema
                break
            except (AnalysisException, ParseException) as exc:
                last_exc = exc
                IcebergHelper.ensure_table(spark, cfg, schema, table, df_src, partition_col, partition_cfg)
                if attempt < 4:
                    time.sleep(0.5)
        if tgt_schema is None:
            if last_exc:
                raise last_exc
            raise AnalysisException(f"Unable to read schema for table {tgt}")
        tgt_cols = [f.name for f in tgt_schema.fields]
        new_fields: List[Any] = [
            field for field in df_src.schema.fields if getattr(field, "name", None) not in tgt_cols
        ]
        null_fields = [
            field for field in new_fields
            if isinstance(getattr(field, "dataType", None), NullType)
        ]
        if null_fields:
            df_src = df_src.drop(*[field.name for field in null_fields if getattr(field, "name", None)])
        new_fields = [
            field for field in new_fields
            if not isinstance(getattr(field, "dataType", None), NullType)
        ]
        if new_fields:
            IcebergHelper.add_missing_columns(spark, cfg, schema, table, new_fields)
            tgt_schema = spark.table(tgt).schema
            tgt_cols = [f.name for f in tgt_schema.fields]
        for c in tgt_cols:
            if c not in df_src.columns:
                df_src = df_src.withColumn(c, F.lit(None).cast("string"))
        df_src = df_src.select(
            [F.col(c) for c in tgt_cols if c in df_src.columns]
            + [F.col(c) for c in df_src.columns if c not in tgt_cols]
        )
        tv = f"src_{schema}_{table}_{uuid.uuid4().hex[:8]}"
        df_src.createOrReplaceTempView(tv)
        src_cols = df_src.columns
        non_pk_cols = [c for c in src_cols if c not in pk_cols and c != partition_col]
        on_clause = " AND ".join([f"t.`{c}` = s.`{c}`" for c in pk_cols])
        target_predicate = None
        if merge_filter_expr:
            target_predicate = IcebergHelper._qualify_merge_filter(merge_filter_expr, tgt_schema)
        if target_predicate:
            on_clause = f"{on_clause} AND ({target_predicate})"
        merge_sql = f"MERGE INTO {tgt} t USING {tv} s ON {on_clause} "
        if non_pk_cols:
            set_clause = ", ".join([f"t.`{c}` = s.`{c}`" for c in non_pk_cols])
            merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        insert_cols = ", ".join([f"`{c}`" for c in src_cols])
        insert_vals = ", ".join([f"s.`{c}`" for c in src_cols])
        merge_sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        spark.sql(merge_sql)
        return tgt

    @staticmethod
    def add_missing_columns(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        fields: Sequence[Any],
    ) -> None:
        tgt = IcebergHelper.qualified_name(cfg, schema, table, quoted=True)
        for field in fields:
            name = getattr(field, "name", None)
            if not name:
                continue
            dtype = getattr(field, "dataType", None)
            if hasattr(dtype, "simpleString"):
                spark_type = dtype.simpleString()
            else:
                spark_type = str(dtype)
            spark.sql(f"ALTER TABLE {tgt} ADD COLUMN `{name}` {spark_type}")


    @staticmethod
    def mirror_to_parquet_for_date(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        load_date: str,
    ) -> Dict[str, str]:
        final_cfg = cfg["runtime"]["final_parquet_mirror"]
        hive_cfg = cfg["runtime"]["hive_reg"]
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        part_col = final_cfg.get("partition_col", "load_date")
        tbl_ident = IcebergHelper._identifier(schema, table)
        ice_tbl = f"{cat}.{db}.{tbl_ident}"
        snap_df = spark.table(ice_tbl).where(f"`{part_col}` = '{load_date}'")
        append_schema = cfg["runtime"].get("append_table_schema", False)
        base = (
            f"{final_cfg['root']}/{schema}/{table}"
            if append_schema
            else f"{final_cfg['root']}/{table}"
        )
        part_path = f"{base}/{part_col}={load_date}"
        (
            snap_df.write.format(cfg["runtime"].get("write_format", "parquet"))
            .mode("overwrite")
            .option("compression", cfg["runtime"].get("compression", "snappy"))
            .save(part_path)
        )
        if hive_cfg.get("enabled", False):
            hive_db = hive_cfg["db"]
            hive_tbl = f"{schema}__{table}"
            HiveHelper.create_partitioned_parquet_if_absent_with_schema(
                spark,
                hive_db,
                schema,
                table,
                base_location=base,
                sample_partition_path=part_path,
                partition_col=part_col,
            )
            HiveHelper.add_partition_or_msck(
                spark,
                hive_db,
                hive_tbl,
                part_col,
                load_date,
                base,
                use_msck=hive_cfg.get("use_msck", False),
            )
        return {"final_parquet_path": part_path}

    @staticmethod
    def _compute_wm_ld(df: DataFrame, incr_col: str, is_int_epoch: bool = False) -> Tuple[str, str]:
        if is_int_epoch:
            agg = df.select(
                F.max(F.col(incr_col).cast("long")).alias("wm"),
                F.max(F.col("load_date")).alias("ld"),
            ).collect()[0]
        else:
            agg = df.select(
                F.max(F.col(incr_col)).alias("wm"),
                F.max(F.col("load_date")).alias("ld"),
            ).collect()[0]
        return str(agg["wm"]), str(agg["ld"])
