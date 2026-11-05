from __future__ import annotations



import argparse
import json
import os
import re
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType, BooleanType, DateType, DecimalType, DoubleType,
    IntegerType, LongType, ShortType, StringType, StructField, StructType,
    TimestampType,
)

load_dotenv()

SAFE_OPS = {">", ">=", "<", "<=", "=", "==", "!=", "<>"}
REQUIRED_METRIC_COLUMNS = {
    "metric_value", "passed_count", "failed_count", "total_count", "applicable_count"
}


class _Identifier:
    def __init__(self, name: str):
        if not isinstance(name, str) or not name or "\x00" in name:
            raise ValueError(f"Invalid SQL identifier: {name!r}")
        self.name = name

    def __str__(self):
        return '"' + self.name.replace('"', '""') + '"'


class _SQL:
    def __init__(self, text: str):
        self.text = text

    def format(self, *args):
        result = self.text
        for arg in args:
            result = result.replace("{}", str(arg), 1)
        if "{}" in result:
            raise ValueError("Not enough SQL identifier arguments supplied.")
        return _SQL(result)

    def join(self, values):
        return _SQL(self.text.join(str(v) for v in values))

    def __str__(self):
        return self.text


class _SQLFactory:
    @staticmethod
    def SQL(text):
        return _SQL(text)

    @staticmethod
    def Identifier(name):
        return _Identifier(name)


sql = _SQLFactory()


def identifier(name: str) -> _Identifier:
    return _Identifier(name)


def table_name(env_key: str, default: str) -> tuple[str, str]:
    value = os.getenv(env_key, default)
    parts = value.split(".")
    if len(parts) == 1:
        schema = os.getenv("EDQ_METADATA_SCHEMA", "public")
        name = parts[0]
    elif len(parts) == 2:
        schema, name = parts
    else:
        raise ValueError(f"{env_key} must be table or schema.table")
    identifier(schema)
    identifier(name)
    return schema, name


def relation(schema: str, table: str) -> _SQL:
    return sql.SQL(".").join([identifier(schema), identifier(table)])


from pathlib import Path
import os
from pyspark.sql import SparkSession

def _spark_session() -> SparkSession:
    jdbc_jar = os.getenv("EDQ_POSTGRES_JDBC_JAR")

    if not jdbc_jar:
        raise RuntimeError("EDQ_POSTGRES_JDBC_JAR is not configured")

    jar_path = Path(jdbc_jar).expanduser().resolve()

    if not jar_path.is_file():
        raise FileNotFoundError(f"JDBC JAR not found: {jar_path}")

    return (
        SparkSession.builder
        .appName("EDQ-JDBC-Executor")
        .master(os.getenv("EDQ_SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", str(jar_path))
        .getOrCreate()
    )


def _db_config(prefix: str, default_db: str) -> dict[str, str]:
    required = ["HOST", "USER", "PASSWORD"]
    missing = [f"{prefix}_{key}" for key in required if not os.getenv(f"{prefix}_{key}")]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    host = os.environ[f"{prefix}_HOST"]
    port = os.getenv(f"{prefix}_PORT", "5432")
    database = os.getenv(f"{prefix}_NAME", default_db)
    user = os.environ[f"{prefix}_USER"]
    password = os.environ[f"{prefix}_PASSWORD"]
    sslmode = os.getenv(f"{prefix}_SSLMODE")
    url = f"jdbc:postgresql://{host}:{port}/{database}"
    if sslmode:
        url += f"?sslmode={sslmode}"
    return {"url": url, "user": user, "password": password, "driver": os.getenv("EDQ_JDBC_DRIVER", "org.postgresql.Driver")}


def _java_value(value):
    """Convert Python parameters to values accepted by java.sql.PreparedStatement."""
    jvm = _spark_session()._jvm
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
        normalized = value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
        return ("timestamp", jvm.java.sql.Timestamp.valueOf(normalized.strftime("%Y-%m-%d %H:%M:%S.%f")))
    if isinstance(value, date):
        return ("date", jvm.java.sql.Date.valueOf(value.isoformat()))
    return ("string", str(value))


def _set_parameter(statement, index: int, value):
    jvm = _spark_session()._jvm
    kind, java_value = _java_value(value)
    if kind == "null":
        statement.setNull(index, jvm.java.sql.Types.NULL)
    elif kind == "boolean":
        statement.setBoolean(index, java_value)
    elif kind == "long":
        statement.setLong(index, java_value)
    elif kind == "decimal":
        statement.setBigDecimal(index, java_value)
    elif kind == "double":
        statement.setDouble(index, java_value)
    elif kind == "timestamp":
        statement.setTimestamp(index, java_value)
    elif kind == "date":
        statement.setDate(index, java_value)
    else:
        statement.setString(index, java_value)


def _from_java(value, type_name: str):
    if value is None:
        return None
    type_name = (type_name or "").lower()
    if type_name in {"json", "jsonb", "uuid", "inet", "citext", "name"}:
        try:
            return str(value.getValue())
        except Exception:
            return str(value)
    if type_name in {"int2", "int4", "int8", "smallint", "integer", "bigint", "serial", "bigserial"}:
        return int(str(value))
    if type_name in {"numeric", "decimal", "float4", "float8", "real", "double precision", "money"}:
        return Decimal(str(value))
    if type_name == "bool":
        return str(value).lower() == "true"
    if type_name in {"date"}:
        return date.fromisoformat(str(value)[:10])
    if "timestamp" in type_name:
        return datetime.fromisoformat(str(value).replace(" ", "T", 1))
    try:
        class_name = str(value.getClass().getName())
        if class_name == "org.postgresql.util.PGobject":
            return str(value.getValue())
    except Exception:
        pass
    return str(value) if type_name in {"text", "varchar", "bpchar", "char"} else value


class JdbcCursor:
    """Small DB-API-like adapter backed only by java.sql JDBC."""
    def __init__(self, connection):
        self.connection = connection
        self.result_set = None
        self.statement = None
        self.rows = []

    def execute(self, query, params=None):
        if self.result_set is not None:
            self.result_set.close()
            self.result_set = None
        if self.statement is not None:
            self.statement.close()
        query_text = str(query)
        placeholders = query_text.count("%s")
        if params is not None and placeholders != len(params):
            raise ValueError(f"SQL parameter count mismatch: query has {placeholders}, received {len(params)}")
        query_text = query_text.replace("%s", "?")
        self.statement = self.connection.jdbc.prepareStatement(query_text)
        for i, value in enumerate(params or (), start=1):
            _set_parameter(self.statement, i, value)
        has_result = self.statement.execute()
        self.rows = []
        if has_result:
            self.result_set = self.statement.getResultSet()
            metadata = self.result_set.getMetaData()
            column_count = metadata.getColumnCount()
            type_names = [str(metadata.getColumnTypeName(i)) for i in range(1, column_count + 1)]
            while self.result_set.next():
                self.rows.append(tuple(_from_java(self.result_set.getObject(i), type_names[i - 1]) for i in range(1, column_count + 1)))
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def close(self):
        if self.result_set is not None:
            self.result_set.close()
        if self.statement is not None:
            self.statement.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class JdbcConnection:
    def __init__(self, prefix: str, default_db: str):
        spark = _spark_session()
        config = _db_config(prefix, default_db)
        jvm = spark._jvm

        # Load PostgreSQL driver from Spark's classloader
        class_loader = (
            jvm.java.lang.Thread.currentThread()
            .getContextClassLoader()
        )

        driver_class = class_loader.loadClass(config["driver"])
        driver = driver_class.newInstance()

        # Pass credentials directly to the driver
        properties = jvm.java.util.Properties()
        properties.setProperty("user", config["user"])
        properties.setProperty("password", config["password"])

        # Connect directly, bypassing DriverManager
        connection = driver.connect(config["url"], properties)

        if connection is None:
            raise RuntimeError(
                f"PostgreSQL JDBC driver rejected URL: {config['url']}"
            )

        self.prefix = prefix
        self.jdbc = connection
        self.jdbc.setAutoCommit(False)

    def cursor(self):
        return JdbcCursor(self)

    def commit(self):
        self.jdbc.commit()

    def rollback(self):
        self.jdbc.rollback()

    def close(self):
        self.jdbc.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.rollback()
        finally:
            self.close()


@contextmanager
def metadata_connection():
    conn = JdbcConnection("EDQ_METADATA_DB", "edq_metadata")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def data_connection():
    conn = JdbcConnection("EDQ_DATA_DB", "edq_test_data")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_json(value: Any) -> dict:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("parameter_values must be a JSON object.")


def spark_literal(value: Any) -> str:
    """Render a scalar value as a SQL literal. SQL fragments are handled separately."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            raise ValueError("NaN/Infinity are not accepted as SQL parameter literals.")
        return str(value)
    if isinstance(value, (list, dict)):
        value = json.dumps(value, ensure_ascii=False, default=str)
    return "'" + str(value).replace("'", "''") + "'"


def quote_spark_column(value: str, available: dict[str, str], param_name: str) -> str:
    """Resolve a column name and quote it once for Spark SQL."""
    raw = str(value).strip()

    # Normalize surrounding identifier delimiters only.
    # Do not globally replace quotes inside SQL expressions or string literals.
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ('"', '`'):
        raw = raw[1:-1]

    # PostgreSQL-style escaped double quotes inside a quoted identifier.
    if value is not None and str(value).strip().startswith('"') and raw:
        raw = raw.replace('""', '"')

    actual = available.get(raw.casefold())
    if actual is None:
        raise ValueError(
            f"Parameter {param_name!r} references unknown source column {value!r}."
        )

    return "`" + actual.replace("`", "``") + "`"


def _spark_type(pg_data_type: str, udt_name: str, precision: int | None, scale: int | None):
    t = (pg_data_type or "").casefold()
    u = (udt_name or "").casefold()
    if t == "boolean":
        return BooleanType(), "native"
    if t in {"smallint"} or u == "int2":
        return ShortType(), "native"
    if t in {"integer"} or u == "int4":
        return IntegerType(), "native"
    if t in {"bigint"} or u == "int8":
        return LongType(), "native"
    if t in {"numeric", "decimal"}:
        p = max(1, min(int(precision or 38), 38))
        s = max(0, min(int(scale or 0), p))
        return DecimalType(p, s), "native"
    if t in {"real", "double precision"} or u in {"float4", "float8"}:
        return DoubleType(), "native"
    if t == "date":
        return DateType(), "native"
    if t in {"timestamp without time zone", "timestamp with time zone"}:
        return TimestampType(), "timestamp"
    if t in {"character varying", "character", "text", "USER-DEFINED".casefold()} or u in {"uuid", "json", "jsonb", "name"}:
        return StringType(), "stringify"
    if t in {"bytea"}:
        return BinaryType(), "native"
    if t == "array" or u.startswith("_"):
        return StringType(), "stringify"
    # Unsupported PostgreSQL types are exposed to Spark as strings instead of
    # relying on fragile schema inference for empty tables or heterogeneous values.
    return StringType(), "stringify"



def resolve_physical_table_name(conn, schema: str, requested_table: str) -> str:
    """Resolve a PostgreSQL table name exactly first, then case-insensitively."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
              AND table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN')
            """,
            (schema, requested_table),
        )
        exact = cur.fetchall()

        if len(exact) == 1:
            return str(exact[0][0])

        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND lower(table_name) = lower(%s)
              AND table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN')
            ORDER BY table_name
            """,
            (schema, requested_table),
        )
        matches = [str(row[0]) for row in cur.fetchall()]

    if len(matches) == 1:
        print(
            f"[INFO] Resolved source table {schema}.{requested_table} "
            f"to physical table {schema}.{matches[0]} (case-insensitive match)."
        )
        return matches[0]

    if not matches:
        raise ValueError(
            f"Source table {schema}.{requested_table} was not found. "
            "Check EDQ_DATA_DB_NAME, EDQ_DATA_SCHEMA, and the asset mapping."
        )

    raise ValueError(
        f"Ambiguous source table name {schema}.{requested_table}: "
        f"multiple case-insensitive matches found: {matches}"
    )


def fetch_table_as_spark_df(conn, spark: SparkSession, schema: str, table: str, view_name: str):
    """Read a PostgreSQL table into Spark with a regular, non-partitioned JDBC read."""
    # Resolve the physical PostgreSQL table name before discovering its columns.
    table = resolve_physical_table_name(conn, schema, table)

    with conn.cursor() as cur:
        cur.execute(
            """SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale
               FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s
               ORDER BY ordinal_position""",
            (schema, table),
        )
        column_meta = cur.fetchall()
    if not column_meta:
        raise ValueError(f"Source table {schema}.{table} was not found or has no columns.")

    selections = []
    names = {}
    for col_name, data_type, udt_name, precision, scale in column_meta:
        _, mode = _spark_type(str(data_type), str(udt_name), precision, scale)
        quoted = str(identifier(str(col_name)))
        if mode == "stringify":
            selections.append(f"CAST({quoted} AS TEXT) AS {quoted}")
        elif str(data_type).casefold() == "timestamp with time zone":
            # Normalize timestamptz to a JDBC-compatible timestamp in the source session timezone.
            selections.append(f"CAST({quoted} AS TIMESTAMP) AS {quoted}")
        else:
            selections.append(quoted)
        names[str(col_name).casefold()] = str(col_name)

    config = _db_config(conn.prefix if hasattr(conn, "prefix") else "EDQ_DATA_DB", "edq_test_data")
    query = f"(SELECT {', '.join(selections)} FROM {relation(schema, table)}) AS edq_jdbc_source"
    df = (
        spark.read.format("jdbc")
        .option("url", config["url"])
        .option("dbtable", query)
        .option("user", config["user"])
        .option("password", config["password"])
        .option("driver", config["driver"])
        .load()
    )
    df.createOrReplaceTempView(view_name)
    return df, names, df.count()


def resolve_asset(cur, assets_rel, asset_id: int):
    cur.execute(
        sql.SQL("SELECT asset_id, asset_name, status, sub_domain_id FROM {} WHERE asset_id=%s").format(assets_rel),
        (asset_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Asset {asset_id} was not found.")
    if str(row[2]).upper() != "ACTIVE":
        raise ValueError(f"Asset {asset_id} is not ACTIVE (status={row[2]}).")
    return {"asset_id": row[0], "asset_name": row[1], "status": row[2], "sub_domain_id": row[3]}


def build_spark_sql(
    template: str,
    source_view: str,
    source_columns: dict[str, str],
    target_column: str | None,
    params: dict,
    related_assets: dict[str, tuple[str, str, str, dict[str, str]]],
) -> str:
    """Render EDQ placeholders to Spark temp-view names and Spark SQL identifiers."""
    statement = template.strip().rstrip(";").strip()
    if ";" in statement:
        raise ValueError("Only one Spark SQL SELECT/CTE statement is allowed per rule.")

    target_col_sql = quote_spark_column(target_column, source_columns, "target_column") if target_column else None
    for token in ("{table}", "{asset}", "{source_table}"):
        statement = statement.replace(token, source_view)
    if "{column}" in statement:
        if target_col_sql is None:
            raise ValueError("Template placeholder {column} requires a target column.")
        statement = statement.replace("{column}", target_col_sql)

    # Cross-asset parameters resolve to registered Spark views, not PostgreSQL tables.
    for key, value in params.items():
        key_l = str(key).casefold()
        if key_l in {"source_asset", "related_asset", "parent_asset", "target_asset"}:
            resolved = related_assets.get(str(value).casefold())
            if not resolved:
                raise ValueError(f"Cannot resolve cross-asset parameter {key}={value!r} to a Spark view.")
            related_view = resolved[2]
            statement = statement.replace("{" + str(key) + "}", related_view)
            if key_l in {"related_asset", "parent_asset"}:
                statement = statement.replace("{related_table}", related_view)
            if key_l == "target_asset":
                statement = statement.replace("{target_table}", related_view)

    identifier_keys = {
        "left_column", "right_column", "source_key_column", "target_key_column",
        "source_column", "related_column", "parent_key_column",
        "primary_key_column", "column",
    }
    list_identifier_keys = {"selected_columns", "date_sequence_columns", "columns"}
    # The V08 template already wraps {format_definition} in SQL single quotes.
    # Substitute its escaped contents without adding another pair of quotes.
    raw_string_keys = {"format_definition"}
    fragment_keys = {
    "condition",
    "requirement",
    "required_condition",
    "validation_condition",
    "lookup_condition",
    "match_condition",
    "sql_condition",
    "custom_expression",
    "mandatory_condition",
    "mutual_exclusion_conditions",
    "selected_columns_condition",
    "date_sequence_condition",
    "reconciliation_condition",
    "relationship_condition",
    "source_aggregate",
    "target_aggregate",
    "sla_condition",
    "source_timestamp_column",
    "ingestion_timestamp_column",
    "arrival_timestamp_column",
    "expected_arrival",
    "arrival_condition",
    "expected_arrival_count",
    "validation_condition",
    "allowed_values",
    "primary_key_column",
    }
    for key, value in params.items():
        token = "{" + str(key) + "}"
        if token not in statement:
            continue
        key_l = str(key).casefold()
        if key_l in identifier_keys and isinstance(value, str):
            replacement = quote_spark_column(value, source_columns, str(key))
        elif key_l in list_identifier_keys and isinstance(value, list):
            replacement = ", ".join(quote_spark_column(str(v), source_columns, str(key)) for v in value)
        elif key_l in raw_string_keys:
            if not isinstance(value, str) or not value:
                raise ValueError(f"Parameter {key!r} must be a non-empty string.")
            replacement = value.replace("'", "''")
        elif key_l in fragment_keys:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"SQL fragment parameter {key!r} must be a non-empty string.")
            # These fragments are trusted rule configuration and must be reviewed as SQL.
            # Convert simple double-quoted identifiers to Spark backtick identifiers.
            replacement = re.sub(
                r'"([A-Za-z_][A-Za-z0-9_$]*)"',
                r'`\1`',
                value,
            )
            print(f"[DEBUG] SQL fragment {key!r} before conversion: {value!r}")
        else:
            replacement = spark_literal(value)
        statement = statement.replace(token, replacement)

    if "{selected_columns_condition}" in statement and isinstance(params.get("selected_columns"), list):
        cols = [quote_spark_column(str(v), source_columns, "selected_columns") for v in params["selected_columns"]]
        statement = statement.replace("{selected_columns_condition}", " OR ".join(f"{c} IS NOT NULL" for c in cols))
    if "{date_sequence_condition}" in statement and isinstance(params.get("date_sequence_columns"), list):
        cols = [quote_spark_column(str(v), source_columns, "date_sequence_columns") for v in params["date_sequence_columns"]]
        if len(cols) < 2:
            raise ValueError("date_sequence_columns requires at least two columns.")
        statement = statement.replace("{date_sequence_condition}", " AND ".join(f"{a} <= {b}" for a, b in zip(cols, cols[1:])))

    leftovers = sorted(set(re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", statement)))
    if leftovers:
        raise ValueError(f"Unresolved SQL template placeholders: {leftovers}. Check assignment parameters/template.")
    if not re.match(r"^(SELECT|WITH)\b", statement, re.I):
        raise ValueError("Rule SQL must be a read-only Spark SQL SELECT/CTE query.")
    if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE)\b", statement, re.I):
        raise ValueError("Rule SQL contains a write/DDL keyword; refusing to execute.")
    return statement


def evaluate_metric(metric, threshold, operation):
    if threshold is None:
        raise ValueError("Cannot evaluate this rule: no threshold is configured.")
    if operation is None or not str(operation).strip():
        raise ValueError("Cannot evaluate this rule: evaluation_operation is empty; configure an explicit operator.")
    if metric is None:
        return "ERROR"
    op = str(operation).strip()
    if op not in SAFE_OPS:
        raise ValueError(f"Unsupported evaluation_operation {op!r}.")
    try:
        left = Decimal(str(metric))
        right = Decimal(str(threshold))
    except (InvalidOperation, ValueError):
        left, right = metric, threshold
    checks = {
        ">": left > right, ">=": left >= right,
        "<": left < right, "<=": left <= right,
        "=": left == right, "==": left == right,
        "!=": left != right, "<>": left != right,
    }
    return "PASS" if checks[op] else "FAIL"


def _run_rule_execution_impl(
    asset_id: int,
    assignment_id: int,
    rule_code: str,
    asset_execution_id: int,
    target_column_id: int | None = None,
    parameter_values: dict | str | None = None,
    sql_expression: str | None = None,
    threshold_value: float | Decimal | None = None,
    evaluation_operation: str | None = None,
    source_schema: str | None = None,
    source_table: str | None = None,
    airflow_context: dict | None = None,
) -> dict:
    if asset_execution_id is None:
        raise ValueError("asset_execution_id is required; every rule execution must be persisted.")

    data_schema_default = os.getenv("EDQ_DATA_SCHEMA", "public")
    assignment_rel = relation(*table_name("EDQ_ASSIGNMENT_TABLE", "rules.rule_assignments"))
    rules_rel = relation(*table_name("EDQ_RULE_TABLE", "rules.rules"))
    assets_rel = relation(*table_name("EDQ_ASSET_TABLE", "assets.assets"))
    columns_rel = relation(*table_name("EDQ_ASSET_COLUMNS_TABLE", "assets.asset_columns"))
    asset_executions_rel = relation(*table_name("EDQ_ASSET_EXECUTIONS_TABLE", "execution.asset_executions"))
    results_rel = relation(*table_name("EDQ_RESULTS_TABLE", "execution.dq_execution_results"))
    issues_rel = relation(*table_name("EDQ_ISSUES_TABLE", "issues"))
    issue_management_rel = relation(*table_name("EDQ_ISSUE_MANAGEMENT_TABLE", "issue_management"))
    issue_audit_rel = relation(*table_name("EDQ_ISSUE_AUDIT_TABLE", "issue_audit"))
    users_rel = relation(*table_name("EDQ_USERS_TABLE", "users"))

    started_at = datetime.now(timezone.utc)
    with metadata_connection() as mconn:
        with mconn.cursor() as cur:
            asset = resolve_asset(cur, assets_rel, int(asset_id))
            cur.execute(
                sql.SQL("""SELECT assignment_id, asset_id, rule_code, target_column_id, target_type,
                                 parameter_values, sql_expression, threshold_value, evaluation_operation,
                                 status, dimension
                          FROM {} WHERE assignment_id=%s AND asset_id=%s AND rule_code=%s""").format(assignment_rel),
                (assignment_id, asset_id, rule_code),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("Matching rule assignment was not found.")
            assignment = dict(zip(
                ["assignment_id", "asset_id", "rule_code", "target_column_id", "target_type", "parameter_values",
                 "sql_expression", "threshold_value", "evaluation_operation", "status", "dimension"], row
            ))
            if str(assignment["status"]).upper() != "ACTIVE":
                raise ValueError(f"Assignment must be ACTIVE; status={assignment['status']}.")

            cur.execute(
                sql.SQL("""SELECT rule_code, dimension, rule_type, target_type, parameter_schema,
                                 rule_name, description, default_threshold, data_type, semantic_type,
                                 status, default_evaluation_operation, sql_query_template
                          FROM {} WHERE rule_code=%s""").format(rules_rel),
                (rule_code,),
            )
            rr = cur.fetchone()
            if not rr:
                raise ValueError(f"Rule definition {rule_code} was not found.")
            rule = dict(zip(
                ["rule_code", "dimension", "rule_type", "target_type", "parameter_schema", "rule_name",
                 "description", "default_threshold", "data_type", "semantic_type", "status",
                 "default_evaluation_operation", "sql_query_template"], rr
            ))
            if str(rule["status"]).upper() != "ACTIVE":
                raise ValueError(f"Global rule {rule_code} is not ACTIVE.")

            if target_column_id is None:
                target_column_id = assignment["target_column_id"]
            target_column_name = None
            if target_column_id is not None:
                cur.execute(
                    sql.SQL("SELECT column_name FROM {} WHERE asset_id=%s AND column_id=%s").format(columns_rel),
                    (asset_id, target_column_id),
                )
                cr = cur.fetchone()
                if not cr:
                    raise ValueError(f"target_column_id={target_column_id} does not belong to asset_id={asset_id}.")
                target_column_name = cr[0]

            cur.execute(
                sql.SQL("SELECT execution_id FROM {} WHERE asset_execution_id=%s AND asset_id=%s").format(asset_executions_rel),
                (asset_execution_id, asset_id),
            )
            ar = cur.fetchone()
            if not ar:
                raise ValueError(f"asset_execution_id={asset_execution_id} does not exist for asset_id={asset_id}.")
            execution_id = ar[0]

            params = parse_json(assignment["parameter_values"] if assignment["parameter_values"] is not None else parameter_values)
            assignment_sql = assignment["sql_expression"]

            if sql_expression:
                # An explicitly supplied SQL expression takes precedence.
                template = sql_expression
            elif assignment_sql and re.search(
                r'\bFROM\s+""[^"]+""',
                assignment_sql,
                re.IGNORECASE,
            ):
                # Fall back to the global template when stored SQL contains
                # the known malformed, double-double-quoted table reference.
                template = rule["sql_query_template"]
                print(
                    f"[INFO] Assignment {assignment['assignment_id']} "
                    f"({rule_code}): using global SQL template because "
                    "the stored assignment SQL has a malformed table reference."
                )
            else:
                template = assignment_sql or rule["sql_query_template"]

            if not template:
                raise ValueError(f"Rule {rule_code} has no SQL template/expression.")
            effective_threshold = threshold_value if threshold_value is not None else assignment["threshold_value"]
            if effective_threshold is None:
                effective_threshold = rule["default_threshold"]
            effective_operation = evaluation_operation or assignment["evaluation_operation"] or rule["default_evaluation_operation"]

            related_asset_specs = {}
            asset_params = {k: v for k, v in params.items() if str(k).casefold() in {"source_asset", "related_asset", "parent_asset", "target_asset"}}
            for _, value in asset_params.items():
                cur.execute(
                    sql.SQL("SELECT asset_id, asset_name, status FROM {} WHERE asset_id::text=%s OR lower(asset_name)=lower(%s) LIMIT 2").format(assets_rel),
                    (str(value), str(value)),
                )
                matches = cur.fetchall()
                if len(matches) != 1:
                    raise ValueError(f"Cross-asset parameter {value!r} did not resolve to exactly one asset.")
                related = matches[0]
                if str(related[2]).upper() != "ACTIVE":
                    raise ValueError(f"Related asset {related[0]} is not ACTIVE.")
                related_asset_specs[str(value).casefold()] = (int(related[0]), str(related[1]))

    actual_source_schema = source_schema or data_schema_default
    actual_source_table = source_table or str(asset["asset_name"])
    source_view = f"edq_asset_{int(asset_id)}"
    spark_master = os.getenv("EDQ_SPARK_MASTER", "local[*]")
    spark = _spark_session()
    try:
        with data_connection() as dconn:
            _, source_columns, source_row_count = fetch_table_as_spark_df(
                dconn, spark, actual_source_schema, actual_source_table, source_view
            )
            if target_column_name and target_column_name.casefold() not in source_columns:
                raise ValueError(f"Target column {target_column_name!r} is absent from source table {actual_source_schema}.{actual_source_table}.")

            related_assets: dict[str, tuple[str, str, str, dict[str, str]]] = {}
            # Register each cross-asset source as its own Spark DataFrame/view.
            for value_key, (related_id, related_name) in related_asset_specs.items():
                related_schema = data_schema_default
                related_table = related_name
                related_view = f"edq_related_{related_id}"
                _, related_columns, _ = fetch_table_as_spark_df(
                    dconn, spark, related_schema, related_table, related_view
                )
                related_assets[value_key] = (related_schema, related_table, related_view, related_columns)
                # Also permit the canonical asset name and numeric ID as lookup keys.
                related_assets[str(related_id).casefold()] = related_assets[value_key]
                related_assets[str(related_name).casefold()] = related_assets[value_key]

        rendered_sql = build_spark_sql(template, source_view, source_columns, target_column_name, params, related_assets)
        try:
            result_df = spark.sql(rendered_sql)
            result_rows = result_df.limit(2).collect()
        except Exception as exc:
            raise RuntimeError(f"Spark SQL failed for rule {rule_code}: {exc}. Rendered Spark SQL: {rendered_sql}") from exc

        if len(result_rows) != 1:
            raise ValueError(f"Rule SQL must return exactly one row; Spark returned {len(result_rows)} row(s).")
        metrics = {key.casefold(): value for key, value in result_rows[0].asDict(recursive=True).items()}
        missing = REQUIRED_METRIC_COLUMNS - set(metrics)
        if missing:
            raise ValueError(f"Rule SQL must return columns {sorted(REQUIRED_METRIC_COLUMNS)}; missing {sorted(missing)}, got {sorted(metrics)}.")

        metric = metrics["metric_value"]
        total = int(metrics["total_count"] or 0)
        passed = int(metrics["passed_count"] or 0)
        failed = int(metrics["failed_count"] or 0)
        applicable = int(metrics["applicable_count"] or 0)
        failure_pct = round(100.0 * failed / total, 2) if total else 0.0
        status = evaluate_metric(metric, effective_threshold, effective_operation)
        completed_at = datetime.now(timezone.utc)
        details = {
            "rule_code": rule_code,
            "rule_name": rule["rule_name"],
            "dimension": assignment["dimension"] or rule["dimension"],
            "target_type": assignment["target_type"] or rule["target_type"],
            "target_column_id": target_column_id,
            "target_column_name": target_column_name,
            "metric_value": metric,
            "threshold_value": effective_threshold,
            "evaluation_operation": effective_operation,
            "records_evaluated": total,
            "records_passed": passed,
            "records_failed": failed,
            "applicable_count": applicable,
            "failure_percentage": failure_pct,
            "source_row_count_loaded": source_row_count,
            "parameter_values": params,
            "rendered_spark_sql": rendered_sql,
        }

        # Persist the execution result and, for FAIL only, create a linked issue
        # plus its initial management/audit records. Assignment status is untouched.
        with metadata_connection() as mconn:
            with mconn.cursor() as cur:
                cur.execute(
                    sql.SQL("""INSERT INTO {} (
                        asset_execution_id, rule_code, assignment_id, dimension, status,
                        records_evaluated, records_passed, records_failed, failure_percentage,
                        threshold_value, result_details, started_at, completed_at, created_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
                    RETURNING result_id""").format(results_rel),
                    (
                        asset_execution_id, rule_code, assignment_id, assignment["dimension"] or rule["dimension"],
                        status, total, passed, failed, failure_pct, effective_threshold,
                        json.dumps(details, default=str), started_at, completed_at, completed_at,
                    ),
                )
                result_id = cur.fetchone()[0]
                issue_id = None
                issue_created = False

                if status == "FAIL":
                    # Only an active SUBDOMAIN_OWNER may receive
                    # automatically generated issues.
                    # Default owner for this environment: 810000005.
                    # An environment override is accepted only when it
                    # identifies an active SUBDOMAIN_OWNER.
                    configured_owner = (
                        os.getenv("EDQ_DEFAULT_ISSUE_OWNER_ID")
                        or "810000005"
                    ).strip()

                    try:
                        owner_id = int(configured_owner)
                    except ValueError as exc:
                        raise ValueError(
                            "EDQ_DEFAULT_ISSUE_OWNER_ID must be an integer "
                            "user_id belonging to an active SUBDOMAIN_OWNER."
                        ) from exc

                    cur.execute(
                        sql.SQL(
                            "SELECT user_id FROM {} "
                            "WHERE user_id=%s "
                            "AND UPPER(status)='ACTIVE' "
                            "AND UPPER(full_name)='SUBDOMAIN_OWNER'"
                        ).format(users_rel),
                        (owner_id,),
                    )

                    owner_row = cur.fetchone()

                    if not owner_row:
                        raise ValueError(
                            f"Issue owner {owner_id} is missing, inactive, "
                            "or is not a SUBDOMAIN_OWNER. "
                            "Refusing to assign the issue to another role."
                        )

                    owner_id = int(owner_row[0])

                    # Idempotency guard: don't create a second issue for the same
                    # execution, asset, rule, and target column on a retry.
                    cur.execute(
                        sql.SQL("""SELECT issue_id FROM {}
                            WHERE execution_id=%s AND asset_id=%s AND rule_code=%s
                              AND column_id IS NOT DISTINCT FROM %s
                            ORDER BY generated_at DESC, issue_id DESC
                            LIMIT 1""").format(issues_rel),
                        (execution_id, asset_id, rule_code, target_column_id),
                    )
                    existing_issue = cur.fetchone()

                    if existing_issue:
                        issue_id = int(existing_issue[0])
                    else:
                        # Default severity policy based on percentage of failed records.
                        # These thresholds can be tuned to match business policy.
                        if failure_pct >= 50:
                            severity = "CRITICAL"
                        elif failure_pct >= 25:
                            severity = "HIGH"
                        elif failure_pct >= 10:
                            severity = "MEDIUM"
                        else:
                            severity = "LOW"

                        # Leave issue descriptions empty initially.
                        description = ""

                        cur.execute(
                            sql.SQL("""INSERT INTO {} (
                                rule_code, execution_id, asset_id, column_id,
                                issue_description, issue_category, severity, issue_owner
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            RETURNING issue_id""").format(issues_rel),
                            (
                                rule_code, execution_id, asset_id, target_column_id,
                                description, "QUALITY", severity, owner_id,
                            ),
                        )
                        issue_id = int(cur.fetchone()[0])
                        issue_created = True

                        cur.execute(
                            sql.SQL("""INSERT INTO {} (
                                issue_id, issue_status, assigned_to, assigned_by,
                                assigned_at, updated_by
                            ) VALUES (%s,%s,%s,%s,%s,%s)
                            RETURNING issue_management_id""").format(issue_management_rel),
                            (
                                issue_id, "OPEN", owner_id, owner_id,
                                completed_at, owner_id,
                            ),
                        )
                        issue_management_id = int(cur.fetchone()[0])

                        cur.execute(
                            sql.SQL("""INSERT INTO {} (
                                issue_id, issue_management_id, event_type,
                                previous_status, new_status, actioned_by, note
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s)""").format(issue_audit_rel),
                            (
                                issue_id, issue_management_id, "ISSUE_CREATED",
                                None, "OPEN", owner_id,
                                f"Automatically created from failed rule execution result {result_id}.",
                            ),
                        )
            # AUTO_DEACTIVATE_FAILED_ASSIGNMENT
            if status == "FAIL":
                cur.execute(
                    sql.SQL("""UPDATE {} SET status = %s
                        WHERE assignment_id = %s
                          AND asset_id = %s
                          AND rule_code = %s
                          AND UPPER(status) = 'ACTIVE'""").format(assignment_rel),
                    ("INACTIVE", assignment["assignment_id"], asset_id, rule_code),
                )
            mconn.commit()

        return {
            "result_id": result_id,
            "issue_id": issue_id,
            "status": status,
            "asset_id": asset_id,
            "asset_execution_id": asset_execution_id,
            "execution_id": execution_id,
            "assignment_id": assignment_id,
            "rule_code": rule_code,
            "metric_value": metric,
            "threshold_value": effective_threshold,
            "evaluation_operation": effective_operation,
            "records_evaluated": total,
            "records_passed": passed,
            "records_failed": failed,
            "failure_percentage": failure_pct,
            "spark_master": spark_master,
            "source_row_count_loaded": source_row_count,
            "result_written": True,
            "issue_created": issue_created,
        }
    finally:
        # The public wrapper owns Spark lifecycle for the entire metadata-to-write flow.
        pass


def run_rule_execution(
    asset_id: int,
    assignment_id: int,
    rule_code: str,
    asset_execution_id: int,
    target_column_id: int | None = None,
    parameter_values: dict | str | None = None,
    sql_expression: str | None = None,
    threshold_value: float | Decimal | None = None,
    evaluation_operation: str | None = None,
    source_schema: str | None = None,
    source_table: str | None = None,
    airflow_context: dict | None = None,
) -> dict:
    """Own the Spark lifecycle, including failures during metadata lookup."""
    spark = _spark_session()
    try:
        return _run_rule_execution_impl(
            asset_id=asset_id,
            assignment_id=assignment_id,
            rule_code=rule_code,
            asset_execution_id=asset_execution_id,
            target_column_id=target_column_id,
            parameter_values=parameter_values,
            sql_expression=sql_expression,
            threshold_value=threshold_value,
            evaluation_operation=evaluation_operation,
            source_schema=source_schema,
            source_table=source_table,
            airflow_context=airflow_context,
        )
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Execute one EDQ rule assignment using PySpark SQL.")
    parser.add_argument("--asset-id", type=int, required=True)
    parser.add_argument("--assignment-id", type=int, required=True)
    parser.add_argument("--rule-code", required=True)
    parser.add_argument("--asset-execution-id", type=int, required=True)
    parser.add_argument("--target-column-id", type=int)
    parser.add_argument("--parameter-values", help="JSON object override (used only when assignment has no parameter_values)")
    parser.add_argument("--sql-expression")
    parser.add_argument("--threshold-value", type=Decimal)
    parser.add_argument("--evaluation-operation")
    parser.add_argument("--source-schema")
    parser.add_argument("--source-table")
    args = parser.parse_args()
    output = run_rule_execution(
        asset_id=args.asset_id,
        assignment_id=args.assignment_id,
        rule_code=args.rule_code,
        asset_execution_id=args.asset_execution_id,
        target_column_id=args.target_column_id,
        parameter_values=args.parameter_values,
        sql_expression=args.sql_expression,
        threshold_value=args.threshold_value,
        evaluation_operation=args.evaluation_operation,
        source_schema=args.source_schema,
        source_table=args.source_table,
    )
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
