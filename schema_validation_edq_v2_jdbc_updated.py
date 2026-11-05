#!/usr/bin/env python3
"""Validate EDQ asset-column metadata against physical source tables using JDBC only.

Targets the current EDQ v2 layout: all metadata tables are in public in
EDQ_METADATA_DB_NAME; physical source tables are in public in EDQ_DATA_DB_NAME.
Default is dry-run. Use --write to persist parent/asset executions and one schema
DQ result per asset. Issue creation remains disabled.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
LOG = logging.getLogger("edq_schema_validation")

METADATA_DB = os.getenv("EDQ_METADATA_DB_NAME", "edq_metadata_v2")
DATA_DB = os.getenv("EDQ_DATA_DB_NAME", os.getenv("EDQ_TEST_DATA_DB", "edq_test_data"))
PHYSICAL_SCHEMA = os.getenv("EDQ_DATA_SCHEMA", os.getenv("EDQ_PHYSICAL_SCHEMA", "public"))
METADATA_SCHEMA = "public"
DRY_RUN_DEFAULT = os.getenv("EDQ_DRY_RUN", "1").strip().lower() in {"1", "true", "yes"}
JDBC_DRIVER = os.getenv("EDQ_JDBC_DRIVER", "org.postgresql.Driver")

# All EDQ v2 metadata tables are explicitly mapped to public.
TABLES = {
    "assets": ("public", "assets"),
    "columns": ("public", "asset_columns"),
    "domains": ("public", "domains"),
    "subdomains": ("public", "sub_domains"),
    "rules": ("public", "rules"),
    "assignments": ("public", "rule_assignments"),
    "executions": ("public", "executions"),
    "asset_executions": ("public", "asset_executions"),
    "results": ("public", "dq_execution_results"),
}


def env_config(prefix: str, fallback_prefix: str | None = None, default_db: str | None = None) -> dict[str, str]:
    """Read the same EDQ_* connection variables used by rule_execution_spark.py."""
    def get(suffix: str, default: str | None = None) -> str | None:
        value = os.getenv(f"{prefix}_{suffix}")
        if not value and fallback_prefix:
            value = os.getenv(f"{fallback_prefix}_{suffix}")
        return value if value else default

    missing = [suffix for suffix in ("HOST", "USER", "PASSWORD") if not get(suffix)]
    if missing:
        raise RuntimeError(
            f"Missing environment variables for {prefix}: "
            + ", ".join(f"{prefix}_{item}" for item in missing)
        )
    database = get("NAME", default_db)
    if not database:
        raise RuntimeError(f"Missing {prefix}_NAME database setting")
    return {
        "host": str(get("HOST")),
        "port": str(get("PORT", "5432")),
        "database": str(database),
        "user": str(get("USER")),
        "password": str(get("PASSWORD")),
        "sslmode": str(get("SSLMODE", "")),
    }


def spark_session():
    builder = SparkSession.builder.appName("EDQSchemaValidationJDBC").master(os.getenv("EDQ_SPARK_MASTER", "local[*]"))
    jar = os.getenv("EDQ_JDBC_JAR")
    if not jar:
        candidate = Path(__file__).resolve().parent / "lib" / "postgresql.jar"
        if candidate.exists():
            jar = str(candidate)
    if jar:
        builder = builder.config("spark.jars", jar).config("spark.driver.extraClassPath", jar)
    return builder.getOrCreate()


def jdbc_url(config: dict[str, str]) -> str:
    url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
    if config.get("sslmode"):
        url += f"?sslmode={config['sslmode']}"
    return url


def py_to_java(value):
    jvm = spark_session()._jvm
    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("boolean", value)
    if isinstance(value, int):
        return ("long", value)
    if isinstance(value, Decimal):
        return ("decimal", jvm.java.math.BigDecimal(str(value)))
    if isinstance(value, float):
        return ("double", value)
    if isinstance(value, datetime):
        # JDBC Timestamp is used for PostgreSQL timestamp/timestamptz columns.
        normalized = value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
        return ("timestamp", jvm.java.sql.Timestamp.valueOf(normalized.strftime("%Y-%m-%d %H:%M:%S.%f")))
    if isinstance(value, date):
        return ("date", jvm.java.sql.Date.valueOf(value.isoformat()))
    return ("string", str(value))


class JdbcDB:
    """Small JDBC-only helper. SQL parameters use JDBC '?' placeholders."""
    def __init__(self, prefix: str, default_db: str, fallback_prefix: str | None = None, readonly: bool = False):
        self.config = env_config(prefix, fallback_prefix, default_db)
        jvm = spark_session()._jvm
        jvm.java.lang.Class.forName(JDBC_DRIVER)
        props = jvm.java.util.Properties()
        props.setProperty("user", self.config["user"])
        props.setProperty("password", self.config["password"])
        if self.config.get("sslmode"):
            props.setProperty("sslmode", self.config["sslmode"])
        self.conn = jvm.java.sql.DriverManager.getConnection(jdbc_url(self.config), props)
        self.conn.setAutoCommit(False)
        self.readonly = readonly
        if readonly:
            # Enforce read-only at the connection level for source data.
            self.conn.setReadOnly(True)

    def _bind(self, stmt, params):
        for index, value in enumerate(params or [], start=1):
            kind, converted = py_to_java(value)
            if kind == "null":
                stmt.setObject(index, None)
            elif kind == "boolean":
                stmt.setBoolean(index, converted)
            elif kind == "long":
                stmt.setLong(index, converted)
            elif kind == "decimal":
                stmt.setBigDecimal(index, converted)
            elif kind == "double":
                stmt.setDouble(index, converted)
            elif kind == "timestamp":
                stmt.setTimestamp(index, converted)
            elif kind == "date":
                stmt.setDate(index, converted)
            else:
                stmt.setString(index, converted)

    def query(self, sql: str, params=[]) -> list[dict]:
        stmt = self.conn.prepareStatement(sql)
        try:
            self._bind(stmt, params)
            rs = stmt.executeQuery()
            try:
                meta = rs.getMetaData()
                count = meta.getColumnCount()
                names = [str(meta.getColumnLabel(i)).lower() for i in range(1, count + 1)]
                rows = []
                while rs.next():
                    row = {}
                    for i, name in enumerate(names, start=1):
                        value = rs.getObject(i)
                        if value is not None:
                            # JDBC/JayDeBeApi bridges can return either Java objects
                            # or already-converted Python primitives (e.g. str).
                            if isinstance(value, (str, int, float, bool, bytes)):
                                pass
                            else:
                                get_class = getattr(value, "getClass", None)
                                class_name = str(get_class().getName()) if callable(get_class) else ""
                                if class_name == "org.postgresql.util.PGobject":
                                    value = str(value.getValue())
                                elif class_name in ("java.lang.Long", "java.lang.Integer", "java.lang.Short"):
                                    value = int(value)
                                elif class_name in ("java.lang.Double", "java.lang.Float"):
                                    value = float(value)
                                elif class_name == "java.lang.Boolean":
                                    value = bool(value.booleanValue())
                                elif class_name.startswith("java."):
                                    value = str(value)
                        row[name] = value
                    rows.append(row)
                rs.close()
                return rows
            finally:
                # ResultSet may already be closed; close safely.
                try:
                    rs.close()
                except Exception:
                    pass
        finally:
            stmt.close()

    def execute(self, sql: str, params=[]) -> int:
        stmt = self.conn.prepareStatement(sql)
        try:
            self._bind(stmt, params)
            return int(stmt.executeUpdate())
        finally:
            stmt.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified(schema: str, table: str) -> str:
    return f"{quote(schema)}.{quote(table)}"


def columns_for(db: JdbcDB, schema: str, table: str) -> dict[str, dict]:
    rows = db.query(
        """SELECT column_name, data_type, udt_name, is_nullable, column_default,
                  ordinal_position, character_maximum_length, numeric_precision,
                  numeric_scale, is_identity
           FROM information_schema.columns
           WHERE table_schema = ? AND table_name = ?
           ORDER BY ordinal_position""", [schema, table]
    )
    if not rows:
        raise RuntimeError(f"Required table {schema}.{table} does not exist or has no visible columns.")
    result = {}
    for row in rows:
        dtype = row["data_type"]
        if dtype in ("character varying", "character") and row.get("character_maximum_length"):
            dtype = f"{dtype}({row['character_maximum_length']})"
        elif dtype == "numeric" and row.get("numeric_precision") is not None:
            dtype = f"numeric({row['numeric_precision']},{row['numeric_scale'] or 0})"
        result[row["column_name"]] = {
            "name": row["column_name"], "type": dtype, "data_type": row["data_type"],
            "udt_name": row["udt_name"], "nullable": row["is_nullable"] == "YES",
            "default": row["column_default"], "ordinal_position": int(row["ordinal_position"]),
            "length": row["character_maximum_length"], "precision": row["numeric_precision"],
            "scale": row["numeric_scale"], "identity": row["is_identity"] == "YES",
        }
    return result


def table_names(db: JdbcDB, schema: str) -> set[str]:
    rows = db.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
        [schema],
    )
    return {row["table_name"] for row in rows}


def normalize_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    v = str(value).strip().upper()
    if v in {"YES", "TRUE", "T", "1", "Y"}:
        return True
    if v in {"NO", "FALSE", "F", "0", "N"}:
        return False
    return None


def normalize_type(value):
    if value is None:
        return None
    s = re.sub(r"\s+", " ", str(value).strip().lower())
    base = re.sub(r"\s*\([^)]*\)", "", s).strip()
    aliases = {
        "int2": "smallint", "smallserial": "smallint", "int4": "integer", "serial": "integer",
        "int8": "bigint", "bigserial": "bigint", "float4": "real", "float8": "double precision",
        "bool": "boolean", "character varying": "varchar", "timestamp with time zone": "timestamptz",
        "timestamp without time zone": "timestamp", "decimal": "numeric", "double": "double precision",
    }
    return aliases.get(base, base)


def type_family(value):
    t = normalize_type(value)
    if t in {"varchar", "text", "character", "char", "bpchar", "string"}:
        return "string"
    if t in {"smallint", "integer", "bigint", "numeric", "real", "double precision", "money"}:
        return "numeric"
    if t == "date":
        return "date"
    if t in {"timestamp", "timestamptz", "time", "timetz"}:
        return "datetime"
    if t == "boolean":
        return "boolean"
    return t


def types_compatible(expected, actual):
    return type_family(expected) == type_family(actual)


def parse_metadata(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def physical_table_for(asset):
    return str(asset.get("asset_name") or "").strip().lower() or None


def declared_type_details(value):
    if value is None:
        return None, None, None
    match = re.search(r"\((\d+)(?:\s*,\s*(\d+))?\)", str(value))
    if not match:
        return None, None, None
    first = int(match.group(1))
    second = int(match.group(2)) if match.group(2) else None
    if second is not None:
        return None, first, second
    return first, None, None


def compare_asset(asset, expected_rows, physical_db: JdbcDB, physical_tables: set[str]):
    table = physical_table_for(asset)
    label = str(asset.get("asset_name") or asset.get("asset_id"))
    issues = []
    if not table or table not in physical_tables:
        return table, [{"category": "MISSING_TABLE", "column": None, "expected": table or label,
                        "actual": None, "message": f"Physical table '{table}' for asset '{label}' was not found in schema {PHYSICAL_SCHEMA}."}]

    actual_cols = list(columns_for(physical_db, PHYSICAL_SCHEMA, table).values())
    actual_by_name = {c["name"].lower(): c for c in actual_cols}
    expected_by_name = {str(r["column_name"]).lower(): r for r in expected_rows if r.get("column_name")}
    for key, row in expected_by_name.items():
        colname = str(row["column_name"])
        actual = actual_by_name.get(key)
        if actual is None:
            issues.append({"category": "MISSING_COLUMN", "column": colname, "expected": row, "actual": None,
                           "message": f"Expected column '{colname}' is missing from table '{table}'."})
            continue
        exp_type = row.get("data_type")
        act_type = str(actual["type"])
        if exp_type and not types_compatible(exp_type, act_type):
            issues.append({"category": "DATA_TYPE_MISMATCH", "column": colname, "expected": str(exp_type), "actual": act_type,
                           "message": f"Column '{colname}' type mismatch: expected '{exp_type}', actual '{act_type}'."})
        metadata = parse_metadata(row.get("metadata"))
        declared_len, declared_precision, declared_scale = declared_type_details(exp_type)
        exp_len = metadata.get("length", declared_len)
        act_len = actual.get("length")
        if exp_len is not None and act_len is not None and int(exp_len) != int(act_len) and type_family(exp_type) != type_family(act_type):
            issues.append({"category": "LENGTH_MISMATCH", "column": colname, "expected": exp_len, "actual": act_len,
                           "message": f"Column '{colname}' length mismatch: expected {exp_len}, actual {act_len}."})
        exp_prec = metadata.get("precision", declared_precision)
        act_prec = actual.get("precision")
        if exp_prec is not None and act_prec is not None and int(exp_prec) != int(act_prec) and type_family(exp_type) != type_family(act_type):
            issues.append({"category": "PRECISION_MISMATCH", "column": colname, "expected": exp_prec, "actual": act_prec,
                           "message": f"Column '{colname}' precision mismatch: expected {exp_prec}, actual {act_prec}."})
        exp_scale = metadata.get("scale", declared_scale)
        act_scale = actual.get("scale")
        if exp_scale is not None and act_scale is not None and int(exp_scale) != int(act_scale) and type_family(exp_type) != type_family(act_type):
            issues.append({"category": "SCALE_MISMATCH", "column": colname, "expected": exp_scale, "actual": act_scale,
                           "message": f"Column '{colname}' scale mismatch: expected {exp_scale}, actual {act_scale}."})
        exp_null = normalize_bool(row.get("is_nullable"))
        act_null = bool(actual.get("nullable", True))
        if exp_null is not None and exp_null != act_null:
            issues.append({"category": "NULLABILITY_MISMATCH", "column": colname, "expected": exp_null, "actual": act_null,
                           "message": f"Column '{colname}' nullability mismatch: expected nullable={exp_null}, actual nullable={act_null}."})
        pos = row.get("ordinal_position")
        if pos is not None and int(pos) != int(actual["ordinal_position"]):
            issues.append({"category": "ORDINAL_POSITION_MISMATCH", "column": colname, "expected": int(pos), "actual": int(actual["ordinal_position"]),
                           "message": f"Column '{colname}' position mismatch: expected {pos}, actual {actual['ordinal_position']}."})
    for key, actual in actual_by_name.items():
        if key not in expected_by_name:
            issues.append({"category": "UNEXPECTED_COLUMN", "column": actual["name"], "expected": None, "actual": str(actual["type"]),
                           "message": f"Unexpected physical column '{actual['name']}' exists in table '{table}'."})
    return table, issues


def insert_row(db: JdbcDB, schema: str, table: str, values: dict):
    cols = columns_for(db, schema, table)
    usable = {key: value for key, value in values.items() if key in cols}
    missing = [name for name, info in cols.items()
               if name not in usable and not info["nullable"] and info["default"] is None and not info["identity"]]
    if missing:
        raise RuntimeError(f"Cannot safely insert into {schema}.{table}; required values unknown: {missing}")
    if not usable:
        raise RuntimeError(f"No compatible insert values for {schema}.{table}")
    names = list(usable)
    placeholders = []
    params = []
    for name, value in usable.items():
        if isinstance(value, (dict, list)):
            placeholders.append("CAST(? AS jsonb)")
            params.append(json.dumps(value, default=str))
        else:
            placeholders.append("?")
            params.append(value)
    sql = f"INSERT INTO {qualified(schema, table)} ({', '.join(quote(n) for n in names)}) VALUES ({', '.join(placeholders)})"
    pk_rows = db.query(
        """SELECT kcu.column_name
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
           WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
           ORDER BY kcu.ordinal_position LIMIT 1""", [schema, table]
    )
    pk = pk_rows[0]["column_name"] if pk_rows else next((n for n in ("execution_id", "asset_execution_id", "result_id") if n in cols and n in usable), None)
    if pk:
        rows = db.query(sql + f" RETURNING {quote(pk)}", params)
        if not rows:
            raise RuntimeError(f"Insert into {schema}.{table} returned no {pk}.")
        return rows[0][pk]
    db.execute(sql, params)
    return None


def update_row(db: JdbcDB, schema: str, table: str, values: dict, where_sql: str, where_params: list):
    cols = columns_for(db, schema, table)
    usable = {key: value for key, value in values.items() if key in cols}
    if not usable:
        return 0
    sets, params = [], []
    for key, value in usable.items():
        if isinstance(value, (dict, list)):
            sets.append(f"{quote(key)}=CAST(? AS jsonb)")
            params.append(json.dumps(value, default=str))
        else:
            sets.append(f"{quote(key)}=?")
            params.append(value)
    return db.execute(f"UPDATE {qualified(schema, table)} SET {', '.join(sets)} WHERE {where_sql}", params + list(where_params))


def main():
    parser = argparse.ArgumentParser(description="Validate physical source schemas against EDQ v2 metadata using JDBC only.")
    parser.add_argument("--write", action="store_true", help="Persist execution and DQ result rows; issue insertion remains disabled.")
    parser.add_argument("--dry-run", action="store_true", help="Force read-only validation; no metadata writes.")
    parser.add_argument("--asset-names", nargs="+", help="Validate only selected asset names.")
    args = parser.parse_args()
    dry_run = True if args.dry_run else (False if args.write else DRY_RUN_DEFAULT)
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")

    print("EDQ Schema Validation (JDBC)")
    print("----------------------------")
    print(f"Metadata database: {METADATA_DB}; metadata schema: {METADATA_SCHEMA}")
    print(f"Physical database: {DATA_DB}; physical schema: {PHYSICAL_SCHEMA}")
    print("Mode: DRY RUN (no metadata writes)" if dry_run else "Mode: WRITE executions and DQ results; issue insertion disabled")

    metadata_db = physical_db = None
    parent_id = None
    passed = failed = total = 0
    overall_error = False
    try:
        metadata_db = JdbcDB("EDQ_METADATA_DB", "edq_metadata_v2")
        physical_db = JdbcDB("EDQ_DATA_DB", "edq_test_data", fallback_prefix="EDQ_TEST_DATA_DB", readonly=True)

        assets_table = TABLES["assets"]
        columns_table = TABLES["columns"]
        columns_for(metadata_db, *assets_table)
        columns_for(metadata_db, *columns_table)
        assets = metadata_db.query(f"SELECT * FROM {qualified(*assets_table)} ORDER BY {quote('asset_id')}")

        if args.asset_names:
            requested = {name.strip().lower() for name in args.asset_names}
            available = {str(a.get("asset_name") or "").strip().lower() for a in assets}
            missing = requested - available
            if missing:
                raise ValueError(f"Requested assets not found in metadata: {sorted(missing)}")
            assets = [a for a in assets if str(a.get("asset_name") or "").strip().lower() in requested]
            print(f"Selected assets: {len(assets)}")

        expected = {}
        for asset in assets:
            expected[asset["asset_id"]] = metadata_db.query(
                f"SELECT * FROM {qualified(*columns_table)} WHERE {quote('asset_id')}=? ORDER BY {quote('ordinal_position')}",
                [asset["asset_id"]],
            )

        if not dry_run:
            for key in ("executions", "asset_executions", "results"):
                columns_for(metadata_db, *TABLES[key])

        total = len(assets)
        print(f"Assets discovered (no status filter): {total}")
        if not dry_run:
            now = datetime.now(timezone.utc)
            metadata_db.rollback()
            parent_id = insert_row(metadata_db, *TABLES["executions"], {
                "execution_type": "MULTIPLE_ASSET", "status": "RUNNING", "started_at": now,
                "created_at": now, "pipeline_id": None, "triggered_by": None,
            })
            metadata_db.commit()
            print(f"Parent execution created: {parent_id}")

        physical_tables = table_names(physical_db, PHYSICAL_SCHEMA)
        for asset in assets:
            aid = asset["asset_id"]
            label = str(asset.get("asset_name") or aid)
            started = datetime.now(timezone.utc)
            try:
                table, discrepancies = compare_asset(asset, expected.get(aid, []), physical_db, physical_tables)
            except Exception as exc:
                LOG.exception("Could not validate asset_id=%s (%s)", aid, label)
                table = physical_table_for(asset)
                discrepancies = [{"category": "PROCESSING_ERROR", "column": None, "expected": None, "actual": None,
                                  "message": f"Schema inspection error: {type(exc).__name__}: {exc}"}]
            ok = not discrepancies
            if dry_run:
                if ok:
                    passed += 1
                    print(f"[PASS] {label}: no schema discrepancies")
                else:
                    failed += 1
                    print(f"[FAIL] {label}: {len(discrepancies)} discrepancies (not persisted)")
                for item in discrepancies:
                    print(f"  - {item['category']}: {item['message']}")
                continue

            ae_id = None
            try:
                metadata_db.rollback()
                ae_id = insert_row(metadata_db, *TABLES["asset_executions"], {
                    "execution_id": parent_id, "asset_id": aid, "status": "RUNNING",
                    "started_at": started, "created_at": started, "dimensions_scores": {},
                })
                end = datetime.now(timezone.utc)
                rs, rt = TABLES["results"]
                insert_row(metadata_db, rs, rt, {
                    "asset_execution_id": ae_id, "rule_code": None, "assignment_id": None,
                    "dimension": "SCHEMA", "status": "PASS" if ok else "FAIL",
                    "records_evaluated": 0, "records_passed": 0, "records_failed": len(discrepancies),
                    "failure_percentage": 0 if ok else 100,
                    "result_details": {"table": table, "discrepancies": discrepancies},
                    "started_at": started, "completed_at": end, "created_at": end,
                })
                update_row(metadata_db, *TABLES["asset_executions"], {
                    "status": "COMPLETED" if ok else "FAILED", "completed_at": end,
                    "error_message": None if ok else "; ".join(d["message"] for d in discrepancies),
                    "total_rules": 1, "passed_rules": 1 if ok else 0, "failed_rules": 0 if ok else 1,
                    "total_records": 0, "passed_records": 0, "failed_records": len(discrepancies),
                    "dimensions_scores": {"SCHEMA": 100 if ok else 0},
                }, f"{quote('asset_execution_id')}=?", [ae_id])
                metadata_db.commit()
                if ok:
                    passed += 1
                    print(f"[PASS] {label}: result persisted")
                else:
                    failed += 1
                    print(f"[FAIL] {label}: {len(discrepancies)} discrepancies persisted as DQ result only")
            except Exception as exc:
                metadata_db.rollback()
                overall_error = True
                failed += 1
                LOG.exception("Persistence failed for %s", label)
                print(f"[ERROR] {label}: persistence failed: {exc}")
                if ae_id is not None:
                    try:
                        update_row(metadata_db, *TABLES["asset_executions"], {
                            "status": "FAILED", "completed_at": datetime.now(timezone.utc), "error_message": str(exc)[:2000],
                        }, f"{quote('asset_execution_id')}=?", [ae_id])
                        metadata_db.commit()
                    except Exception:
                        metadata_db.rollback()
                        LOG.exception("Could not mark asset execution failed")

        final_status = "DRY_RUN" if dry_run else ("COMPLETED" if failed == 0 and not overall_error else "FAILED")
        if not dry_run and parent_id is not None:
            try:
                update_row(metadata_db, *TABLES["executions"], {
                    "status": final_status, "completed_at": datetime.now(timezone.utc),
                }, f"{quote('execution_id')}=?", [parent_id])
                metadata_db.commit()
            except Exception:
                metadata_db.rollback()
                overall_error = True
                LOG.exception("Could not update parent execution status")
                final_status = "FAILED"

        print("\nExecution Summary\n-----------------")
        print(f"Execution ID: {parent_id if parent_id is not None else 'N/A (dry run)'}")
        print(f"Total assets: {total}; passed: {passed}; failed: {failed}; parent status: {final_status}")
        print("Issues recorded: 0 (issue insertion intentionally disabled)")
        print("Issue management: NOT USED; CSV generation: NOT USED")
        return 0 if final_status in {"COMPLETED", "DRY_RUN"} else 1
    except Exception as exc:
        LOG.exception("EDQ schema validation could not start")
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2
    finally:
        if metadata_db is not None:
            try:
                metadata_db.close()
            except Exception:
                pass
        if physical_db is not None:
            try:
                physical_db.close()
            except Exception:
                pass
        try:
            spark_session().stop()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
