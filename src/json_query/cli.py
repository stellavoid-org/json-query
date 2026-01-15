#!/usr/bin/env python3
"""json-query

A pragmatic Python CLI for working with large JSON logs.

Features
- Normalize mixed inputs (JSON array / NDJSON / single JSON object) into one NDJSON
- Generate a *flattened* extraction SELECT (policy A):
    * scalars -> extracted into columns (as strings to avoid type drift)
    * arrays/objects -> kept as JSON columns
- Build a Parquet dataset with DuckDB
- Run ad-hoc SQL queries and export CSV

Notes
- This tool intentionally avoids "fully automatic recursive dot-column flattening"
  inside DuckDB, and instead generates the extraction SQL from sampled data.
- For big data, the default output is a Parquet *dataset directory* (multiple files).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Iterator, List, Optional

import duckdb


# ---------------------------
# JSON streaming utilities
# ---------------------------
def _jsonpath_key_segment(k: str) -> str:
    """
    DuckDB JSONPath: $.key か $."key.with.specials" のどちらか。
    ブラケット表記 $["key"] は使わない（DuckDB非対応）。
    """
    if k.isidentifier():
        return "." + k
    # escape backslash and double quote for $."..."
    esc = k.replace("\\", "\\\\").replace('"', '\\"')
    return f'."{esc}"'


def iter_json_array_stream(fp) -> Iterator[object]:
    """Stream-parse a JSON array from a file-like object without reading it all into RAM.

    Supports files shaped like:
        [ {...}, {...}, ... ]
    potentially pretty-printed with whitespace/newlines.
    """
    decoder = json.JSONDecoder()
    buf = ""
    chunk_size = 1024 * 1024  # 1MB

    # Seek first '['
    while True:
        chunk = fp.read(chunk_size)
        if not chunk:
            raise ValueError("Unexpected EOF while searching for '['")
        buf += chunk
        stripped = buf.lstrip()
        if not stripped:
            buf = ""
            continue
        idx = stripped.find("[")
        if idx != -1:
            buf = stripped[idx + 1 :]
            break

    # Decode elements until ']'
    while True:
        # Skip whitespace and commas
        i = 0
        while True:
            if i >= len(buf):
                chunk = fp.read(chunk_size)
                if not chunk:
                    raise ValueError("Unexpected EOF inside JSON array")
                buf += chunk
                continue
            c = buf[i]
            if c in " \t\r\n,":
                i += 1
                continue
            break
        buf = buf[i:]

        # Ensure we have some content
        while not buf:
            chunk = fp.read(chunk_size)
            if not chunk:
                raise ValueError("Unexpected EOF before ']'")
            buf += chunk

        # Skip whitespace
        if buf and buf[0] in " \t\r\n":
            buf = buf.lstrip()

        # End?
        if buf.startswith("]"):
            return

        # Decode one JSON value
        while True:
            try:
                obj, end = decoder.raw_decode(buf)
                yield obj
                buf = buf[end:]
                break
            except json.JSONDecodeError:
                chunk = fp.read(chunk_size)
                if not chunk:
                    raise
                buf += chunk


def sniff_json_kind(path: Path) -> str:
    """Return one of: 'array' | 'object' | 'ndjson' | 'unknown'."""
    try:
        head = path.read_bytes()[:4096]
    except Exception:
        return "unknown"
    text = head.decode("utf-8", errors="ignore").lstrip()
    if not text:
        return "unknown"
    if text[0] == "[":
        return "array"
    if text[0] == "{":
        return "object"
    # best-effort NDJSON guess
    if "{" in text[:200]:
        return "ndjson"
    return "unknown"


def iter_records_from_file(path: Path) -> Iterator[object]:
    """Yield JSON records from a file.

    Supports:
    - JSON array file: [ {...}, {...} ]
    - NDJSON file: each line is a JSON object
    - Single JSON object file: { ... }
    """
    kind = sniff_json_kind(path)
    if kind == "array":
        with path.open("r", encoding="utf-8") as fp:
            yield from iter_json_array_stream(fp)
        return
    if kind == "object":
        with path.open("r", encoding="utf-8") as fp:
            yield json.load(fp)
        return

    # Assume NDJSON-ish: parse line-by-line
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except json.JSONDecodeError:
                # skip malformed line
                continue


def iter_input_files(inputs: List[str], glob_pat: str) -> List[Path]:
    """Expand file/dir inputs into a de-duplicated file list."""
    out: List[Path] = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            out.extend(sorted(p.glob(glob_pat)))
        elif p.is_file():
            out.append(p)
        else:
            print(f"WARN: not found: {inp}", file=sys.stderr)

    seen = set()
    uniq: List[Path] = []
    for p in out:
        if p not in seen:
            uniq.append(p)
            seen.add(p)
    return uniq


# ---------------------------
# Schema generation (flat select)
# ---------------------------

def is_scalar(x) -> bool:
    return x is None or isinstance(x, (str, int, float, bool))


def walk_paths(obj, path: str, scalar_paths: "OrderedDict[str,bool]", json_paths: "OrderedDict[str,bool]") -> None:
    if is_scalar(obj):
        scalar_paths[path] = True
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                k = str(k)
            p = f"{path}{_jsonpath_key_segment(k)}"
            if is_scalar(v):
                scalar_paths[p] = True
            elif isinstance(v, (dict, list)):
                json_paths[p] = True
                walk_paths(v, p, scalar_paths, json_paths)
            else:
                json_paths[p] = True
        return
    if isinstance(obj, list):
        # Policy A: do NOT explode arrays; keep as JSON
        json_paths[path] = True
        return


def colname(path: str) -> str:
    """Convert JSONPath like $.a.b into a safe SQL identifier like a__b."""
    p = path[2:] if path.startswith("$.") else path.replace("$", "root")
    p = p.replace('["', ".").replace('"]', "")
    p = p.replace("[", "_").replace("]", "")
    p = p.replace(".", "__")
    p = re.sub(r"[^A-Za-z0-9_]+", "_", p)
    return p or "root"


def generate_flat_select_sql(ndjson_path: Path, sample_max: int) -> str:
    """Generate a SELECT that extracts all discovered scalar paths and JSON paths."""
    scalar_paths: "OrderedDict[str,bool]" = OrderedDict()
    json_paths: "OrderedDict[str,bool]" = OrderedDict()

    cnt = 0
    with ndjson_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                continue
            walk_paths(obj, "$", scalar_paths, json_paths)
            cnt += 1
            if cnt >= sample_max:
                break

    exprs = ["raw_json"]
    # To avoid type drift, extract scalars as strings; cast later if needed.
    for p in scalar_paths.keys():
        exprs.append(f"json_extract_string(raw_json, '{p}') AS {colname(p)}")
    for p in json_paths.keys():
        exprs.append(f"json_extract(raw_json, '{p}') AS {colname(p)}__json")

    return "SELECT\n  " + ",\n  ".join(exprs) + "\nFROM raw"


# ---------------------------
# DuckDB pipeline
# ---------------------------

def duckdb_connect(db_path: Path, memory_limit: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    return con


def create_raw_from_ndjson(con: duckdb.DuckDBPyConnection, ndjson_path: Path) -> None:
    # Read NDJSON as a single VARCHAR column; parse with json_extract* later.
    con.execute(f"""
    CREATE OR REPLACE TABLE raw AS
    SELECT line AS raw_json
    FROM read_csv('{ndjson_path.as_posix()}',
                  delim='\\n',
                  header=false,
                  columns={{'line':'VARCHAR'}},
                  quote='',
                  escape='');
    """)


def create_flat_and_parquet(
    con: duckdb.DuckDBPyConnection,
    flat_select_sql: str,
    out_parquet: Path,
    compression: str,
) -> None:
    con.execute(f"CREATE OR REPLACE TABLE flat AS {flat_select_sql};")
    con.execute(
        f"COPY flat TO '{out_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION '{compression}');"
    )


def ensure_view_over_parquet(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> None:
    if parquet_path.is_dir():
        glob = parquet_path.as_posix().rstrip("/") + "/*.parquet"
        # Guard: directory exists but empty -> fail fast (avoids the scary "only one file read" misconception)
        if not list(parquet_path.glob("*.parquet")):
            raise SystemExit(f"Parquet directory has no .parquet files: {parquet_path}")
        con.execute(f"CREATE OR REPLACE VIEW v AS SELECT * FROM read_parquet('{glob}');")
    else:
        if not parquet_path.exists():
            raise SystemExit(f"Parquet file not found: {parquet_path}")
        con.execute(f"CREATE OR REPLACE VIEW v AS SELECT * FROM read_parquet('{parquet_path.as_posix()}');")


# ---------------------------
# Commands
# ---------------------------

def cmd_normalize(args: argparse.Namespace) -> None:
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    out_ndjson = work / args.ndjson_name

    files = iter_input_files(args.inputs, args.glob)
    if not files:
        raise SystemExit("No input files found.")

    n = 0
    skipped = 0
    with out_ndjson.open("w", encoding="utf-8") as out:
        for f in files:
            for rec in iter_records_from_file(f):
                try:
                    out.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                    n += 1
                except Exception:
                    skipped += 1

    print(f"OK normalize: {n} records -> {out_ndjson} (skipped={skipped})")


def cmd_gen_schema(args: argparse.Namespace) -> None:
    work = Path(args.work)
    ndjson_path = work / args.ndjson_name
    if not ndjson_path.exists():
        raise SystemExit(f"NDJSON not found: {ndjson_path} (run normalize/build first)")

    sql = generate_flat_select_sql(ndjson_path, args.sample)
    out_sql = work / args.schema_name
    out_sql.write_text(sql + ";\n", encoding="utf-8")
    print(f"OK schema: {out_sql}")


def cmd_to_parquet(args: argparse.Namespace) -> None:
    work = Path(args.work)
    ndjson_path = work / args.ndjson_name
    schema_path = work / args.schema_name
    if not ndjson_path.exists():
        raise SystemExit(f"NDJSON not found: {ndjson_path} (run normalize/build first)")

    if args.regen_schema or not schema_path.exists():
        sql = generate_flat_select_sql(ndjson_path, args.sample)
        schema_path.write_text(sql + ";\n", encoding="utf-8")
    else:
        sql = schema_path.read_text(encoding="utf-8").strip().rstrip(";")

    db_path = work / args.db_name
    con = duckdb_connect(db_path, args.memory_limit)
    try:
        if args.temp_dir:
            con.execute(f"PRAGMA temp_directory='{Path(args.temp_dir).as_posix()}'")

        create_raw_from_ndjson(con, ndjson_path)

        out_parquet = Path(args.out) if args.out else (work / args.parquet_name)
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        create_flat_and_parquet(con, sql, out_parquet, args.compression)

        print(f"OK parquet: {out_parquet}")
    finally:
        con.close()


def cmd_export_csv(args: argparse.Namespace) -> None:
    work = Path(args.work)
    db_path = work / args.db_name
    parquet = Path(args.parquet) if args.parquet else (work / args.parquet_name)
    out_csv = Path(args.out)

    con = duckdb_connect(db_path, args.memory_limit)
    try:
        if args.temp_dir:
            con.execute(f"PRAGMA temp_directory='{Path(args.temp_dir).as_posix()}'")
        ensure_view_over_parquet(con, parquet)

        con.execute(f"""
        COPY (
          {args.sql}
        ) TO '{out_csv.as_posix()}' (HEADER, DELIMITER ',');
        """)
        print(f"OK csv: {out_csv}")
    finally:
        con.close()


def cmd_query(args: argparse.Namespace) -> None:
    work = Path(args.work)
    db_path = work / args.db_name
    parquet = Path(args.parquet) if args.parquet else (work / args.parquet_name)

    con = duckdb_connect(db_path, args.memory_limit)
    try:
        if args.temp_dir:
            con.execute(f"PRAGMA temp_directory='{Path(args.temp_dir).as_posix()}'")
        ensure_view_over_parquet(con, parquet)

        res = con.execute(args.sql)
        cols = [d[0] for d in res.description]
        rows = res.fetchmany(args.limit) if args.limit else res.fetchall()

        if args.format == "table":
            print("\t".join(cols))
            for r in rows:
                print("\t".join("" if v is None else str(v) for v in r))
        else:
            for r in rows:
                obj = {cols[i]: r[i] for i in range(len(cols))}
                print(json.dumps(obj, ensure_ascii=False, default=str))
    finally:
        con.close()


def cmd_build(args: argparse.Namespace) -> None:
    # normalize -> gen-schema -> to-parquet
    cmd_normalize(args)
    cmd_gen_schema(args)

    # Reuse args for to-parquet; ensure regen_schema False (already generated)
    args.regen_schema = False
    cmd_to_parquet(args)


# ---------------------------
# CLI
# ---------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="json-query",
        description="Normalize JSON/NDJSON, generate a flattened schema, build Parquet with DuckDB, and query it.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_in_opts(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--in",
            dest="inputs",
            action="append",
            required=True,
            help="Input file or directory. Use multiple --in to add more.",
        )
        sp.add_argument("--glob", default="*.json", help="Glob pattern when --in points to a directory.")

    def add_work_opts(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--work", default="staging", help="Working directory for artifacts.")
        sp.add_argument("--ndjson-name", default="all.ndjson", help="NDJSON filename within --work.")
        sp.add_argument("--schema-name", default="flat_select.sql", help="Schema filename within --work.")
        sp.add_argument("--parquet-name", default="flat_parquet", help="Parquet path (dir by default) within --work.")
        sp.add_argument("--db-name", default="work.duckdb", help="DuckDB file within --work.")
        sp.add_argument("--memory-limit", default="4GB", help="DuckDB memory limit (e.g., 2GB, 8GB).")
        sp.add_argument("--temp-dir", default=None, help="DuckDB temp dir for spill files (optional).")

    sp = sub.add_parser("normalize", help="Normalize mixed JSON inputs into work/all.ndjson")
    add_in_opts(sp)
    add_work_opts(sp)
    sp.set_defaults(func=cmd_normalize)

    sp = sub.add_parser("gen-schema", help="Generate work/flat_select.sql (flatten extraction SELECT)")
    add_work_opts(sp)
    sp.add_argument("--sample", type=int, default=20000, help="How many NDJSON records to scan for schema.")
    sp.set_defaults(func=cmd_gen_schema)

    sp = sub.add_parser("to-parquet", help="Build Parquet dataset from NDJSON + schema")
    add_work_opts(sp)
    sp.add_argument("--out", default=None, help="Parquet output path (dir or file). Default: work/flat_parquet")
    sp.add_argument("--sample", type=int, default=20000, help="Scan count if (re)generating schema.")
    sp.add_argument("--regen-schema", action="store_true", help="Regenerate schema even if schema file exists.")
    sp.add_argument("--compression", default="ZSTD", help="Parquet compression (e.g., ZSTD, SNAPPY, GZIP).")
    sp.set_defaults(func=cmd_to_parquet)

    sp = sub.add_parser("build", help="normalize + gen-schema + to-parquet")
    add_in_opts(sp)
    add_work_opts(sp)
    sp.add_argument("--out", default=None, help="Parquet output path (dir or file). Default: work/flat_parquet")
    sp.add_argument("--sample", type=int, default=20000, help="How many NDJSON records to scan for schema.")
    sp.add_argument("--regen-schema", action="store_true", help="Regenerate schema even if schema file exists.")
    sp.add_argument("--compression", default="ZSTD", help="Parquet compression (e.g., ZSTD, SNAPPY, GZIP).")
    sp.set_defaults(func=cmd_build)

    sp = sub.add_parser("query", help="Run SQL against Parquet view v")
    add_work_opts(sp)
    sp.add_argument("--parquet", default=None, help="Parquet path (dir or file). Default: work/flat_parquet")
    sp.add_argument("--format", choices=["table", "jsonl"], default="table")
    sp.add_argument("--limit", type=int, default=0, help="Limit printed rows (0=all fetched).")
    sp.add_argument("sql", help="SQL to run (can reference view v)")
    sp.set_defaults(func=cmd_query)

    sp = sub.add_parser("export-csv", help="Export SQL result to CSV")
    add_work_opts(sp)
    sp.add_argument("--parquet", default=None, help="Parquet path (dir or file). Default: work/flat_parquet")
    sp.add_argument("--out", required=True, help="Output CSV path")
    sp.add_argument("sql", help="SQL to run (can reference view v)")
    sp.set_defaults(func=cmd_export_csv)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
