#!/usr/bin/env python3
"""Capture AES column results in Spark 4.2.0, including valid and failing rows."""

import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from capture_spark_requests import capture_requested_queries


KEYS = ("0123456789abcdef", "0123456789abcdefghijklmn", "0123456789abcdefghijklmnopqrstuv")
IVS = {"ECB": "", "CBC": "000102030405060708090a0b0c0d0e0f", "GCM": "000102030405060708090a0b"}


def binary(hex_value):
    return f"X'{hex_value}'"


def statement(expr, where):
    return f"SELECT id, {expr} AS result FROM aes_inputs WHERE {where} ORDER BY id"


def requests():
    columns = [
        ("id", "integer"), ("message", "binary"), ("key_bytes", "binary"), ("mode_name", "string"),
        ("padding_name", "string"), ("iv_bytes", "binary"), ("associated_data", "binary"),
        ("ciphertext", "binary"),
    ]
    fields = [dict(name=name, type=kind, nullable=name != "id", metadata={}) for name, kind in columns]
    rows = [
        ["0", "614756736247383d", KEYS[0].encode().hex(), "ECB", "DEFAULT", "", "", "5e973c583b41baf1447882f2f219ecb7"],
        ["1", "616c706861", KEYS[1].encode().hex(), "CBC", "PKCS", IVS["CBC"], "", "01"],
        ["2", "636166c3a9", KEYS[2].encode().hex(), "GCM", "NONE", IVS["GCM"], "686561646572", ""],
        ["3", "", KEYS[0].encode().hex(), "gcm", "default", "0c0d0e0f1011121314151617", "00ff", None],
        ["4", None, KEYS[0].encode().hex(), "ECB", "PKCS", "", "", None],
        ["5", "6162", None, "CBC", "PKCS", IVS["CBC"], "", None],
        ["6", "6162", KEYS[0].encode().hex(), None, "DEFAULT", "", "", None],
        ["7", "6162", KEYS[0].encode().hex(), "CBC", None, IVS["CBC"], "", None],
        ["8", "6162", KEYS[0].encode().hex(), "CBC", "PKCS", None, "", None],
        ["9", "6162", KEYS[0].encode().hex(), "GCM", "NONE", IVS["GCM"], None, None],
        ["10", "6162", "73686f7274", "ECB", "PKCS", "", "", None],
        ["11", "6162", KEYS[0].encode().hex(), "CBC", "PKCS", "0102", "", None],
        ["12", bytes(range(16)).hex(), KEYS[0].encode().hex(), "ECB", "PKCS", "", "", None],
        ["13", bytes(range(40)).hex(), KEYS[0].encode().hex(), "ECB", "PKCS", "", "", None],
    ]
    queries = []

    def add(name, expr, where="id < 5", spark_expr=None, *, scope="parity"):
        sql = statement(expr, where)
        oracle = statement(spark_expr, where) if spark_expr is not None else sql
        for ansi in (False, True):
            queries.append(dict(id=name, corpus="aes-columns", sql=oracle, dar_sql=sql,
                                ordered=True, ansi=ansi, scope=scope))

    def add_encrypt(name, arguments, where="id < 5", valid_rows=None):
        encrypt = f"aes_encrypt({arguments})"
        add(name, encrypt, where)
        oracle = encrypt if valid_rows is None else f"CASE WHEN {valid_rows} THEN {encrypt} END"
        add("try_" + name, f"try_aes_encrypt({arguments})", where, oracle, scope="dar-try-encrypt")

    for mode, iv in IVS.items():
        pad = "NONE" if mode == "GCM" else "PKCS"
        aad = "686561646572" if mode == "GCM" else ""
        for size, key in zip((128, 192, 256), KEYS, strict=True):
            naming = f"{mode.lower()}_{size}"
            suffix = f"'{key}', '{mode}', '{pad}'"
            arguments = f"message, {suffix}, {binary(iv)}, {binary(aad)}"
            add_encrypt("encrypt_" + naming, arguments)
            cipher = f"aes_encrypt({arguments})"
            for fn in ("aes_decrypt", "try_aes_decrypt"):
                add(f"{fn}_{naming}", f"{fn}({cipher}, {suffix}, {binary(aad)})")

    key = f"'{KEYS[0]}'"
    add_encrypt("key_column", "message, key_bytes, 'ECB', 'PKCS'")
    add_encrypt("padding_column", f"message, {key}, 'GCM', padding_name, {binary(IVS['GCM'])}", "id IN (0,2,3)")
    add_encrypt("iv_column", f"message, {key}, 'GCM', 'NONE', iv_bytes", "id IN (2,3)")
    add_encrypt("aad_column", f"message, {key}, 'GCM', 'NONE', {binary(IVS['GCM'])}, associated_data", "id < 5")
    column_args = "message, key_bytes, mode_name, padding_name, iv_bytes, associated_data"
    add_encrypt("all_columns_and_nulls", column_args, "id < 10")
    mode_args = f"message, {key}, mode_name"
    add("mode_column_roundtrip",
        f"aes_decrypt(aes_encrypt({mode_args}), {key}, mode_name)")
    add("try_mode_column_roundtrip",
        f"try_aes_decrypt(try_aes_encrypt({mode_args}), {key}, mode_name)",
        spark_expr=f"try_aes_decrypt(aes_encrypt({mode_args}), {key}, mode_name)", scope="dar-try-encrypt")
    # A decrypt workload must also read ciphertext directly from a registered column.
    add("decrypt_ciphertext_column", f"aes_decrypt(ciphertext, {key}, 'ECB')", "id IN (0,3,4)")
    add("try_decrypt_ciphertext_column", f"try_aes_decrypt(ciphertext, {key}, 'ECB')", "id < 5")
    for fn in ("aes_decrypt", "try_aes_decrypt"):
        add(f"{fn}_all_columns", f"{fn}(aes_encrypt({column_args}), key_bytes, mode_name, padding_name, associated_data)", "id < 10")
        add(f"{fn}_null_key", f"{fn}(ciphertext, key_bytes, 'ECB')", "id = 5")
        add(f"{fn}_null_mode", f"{fn}(X'5e973c583b41baf1447882f2f219ecb7', {key}, mode_name)", "id = 6")
        add(f"{fn}_null_padding", f"{fn}(X'5e973c583b41baf1447882f2f219ecb7', {key}, 'ECB', padding_name)", "id = 7")
        add(f"{fn}_null_aad", f"{fn}(X'5e973c583b41baf1447882f2f219ecb7', {key}, 'GCM', 'NONE', associated_data)", "id = 9")
        for mode in ("CBC", "GCM"):
            add(f"{fn}_{mode.lower()}_random_iv_roundtrip",
                f"{fn}(aes_encrypt(message, {key}, '{mode}'), {key}, '{mode}')")
        add(f"{fn}_literal_control", f"{fn}(X'5e973c583b41baf1447882f2f219ecb7', {key}, 'ECB')")
        add(f"{fn}_empty_ecb_body", f"{fn}(X'', {key}, 'ECB')")
        add(f"{fn}_empty_cbc_body", f"{fn}({binary(IVS['CBC'])}, {key}, 'CBC')")

    add_encrypt("literal_control", f"'aGVsbG8=', {key}, 'ECB'")
    for mode in ("CBC", "GCM"):
        add_encrypt(f"literal_{mode.lower()}_control", f"'ab', {key}, '{mode}', 'DEFAULT', {binary(IVS[mode])}")
    add_encrypt("string_column", f"CAST(message AS STRING), {key}, 'ECB'")
    add_encrypt("ecb_block_boundaries", f"message, {key}, 'ECB'", "id >= 12")
    for fn in ("aes_decrypt", "try_aes_decrypt"):
        add(f"{fn}_ecb_block_boundaries",
            f"{fn}(aes_encrypt(message, {key}, 'ECB'), {key}, 'ECB')", "id >= 12")
    add("strict_invalid_key", "aes_encrypt(message, key_bytes, 'ECB')", "id = 10")
    add("strict_invalid_iv", "aes_encrypt(message, key_bytes, 'CBC', 'PKCS', iv_bytes)", "id = 11")
    add("strict_invalid_padding",
        f"aes_encrypt(message, {key}, 'GCM', padding_name, {binary(IVS['GCM'])})", "id = 1")
    add("strict_short_cipher", f"aes_decrypt(ciphertext, {key}, 'GCM')", "id = 1")
    add("strict_short_cbc_cipher", f"aes_decrypt(ciphertext, {key}, 'CBC')", "id = 1")
    add("strict_bad_tag", f"aes_decrypt(X'{'00' * 28}', {key}, 'GCM')")
    add("try_bad_tag", f"try_aes_decrypt(X'{'00' * 28}', {key}, 'GCM')")
    add("try_mixed_invalid_key", "try_aes_encrypt(message, key_bytes, 'ECB')", "id IN (0,10)",
        "CASE WHEN id = 0 THEN aes_encrypt(message, key_bytes, 'ECB') END", scope="dar-try-encrypt")
    add("try_mixed_invalid_iv", "try_aes_encrypt(message, key_bytes, 'CBC', 'PKCS', iv_bytes)", "id IN (1,11)",
        "CASE WHEN id = 1 THEN aes_encrypt(message, key_bytes, 'CBC', 'PKCS', iv_bytes) END", scope="dar-try-encrypt")
    add("try_mixed_invalid_padding",
        f"try_aes_encrypt(message, {key}, 'GCM', padding_name, {binary(IVS['GCM'])})", "id IN (1,2)",
        f"CASE WHEN id = 2 THEN aes_encrypt(message, {key}, 'GCM', padding_name, {binary(IVS['GCM'])}) END",
        scope="dar-try-encrypt")
    add("try_short_cipher", f"try_aes_decrypt(ciphertext, {key}, 'GCM')", "id IN (1,2)")
    add("spark_try_encrypt_registration", f"try_aes_encrypt(message, {key}, 'ECB')", "id = 0", scope="spark-surface")

    assert len(queries) == len({(q["id"], q["ansi"]) for q in queries})
    return {"issue": 1405, "expected_record_count": len(queries), "corpora": [{
        "corpus": "aes-columns",
        "source": {"path": str(Path(__file__).resolve()), "sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        "metadata": {"inputs": [{"name": "aes_inputs", "schema": {"type": "struct", "fields": fields}, "rows": rows}]},
        "cases": queries,
    }]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compact", type=Path, help="write executable results and fields for the Rust test")
    parser.add_argument("--use-capture", action="store_true", help="read an existing raw capture at output")
    args = parser.parse_args()
    if not args.use_capture:
        driver = Path(__file__)
        capture_requested_queries(requests(), args.output, [driver], [driver],
                                  spark_settings={"spark.sql.adaptive.enabled": "false"})
    if args.compact is not None:
        compact_results(args.output, args.compact)


def compact_results(raw, output):
    records = [json.loads(line) for line in raw.read_text().splitlines()]
    header, cases = records[0], records[1:]
    reference = requests()
    corpus = reference["corpora"][0]
    assert header["spark_version"] == "4.2.0" and header["spark_only"]
    assert header["effective_settings"]["spark.sql.session.timeZone"] == "UTC"
    assert header["corpora"][0]["metadata"] == corpus["metadata"]
    assert len(cases) == len(corpus["cases"]) == reference["expected_record_count"]
    compact = [{
        "spark_version": header["spark_version"], "spark_revision": header["spark_build"]["SPARK_REVISION"]["value"],
        "timezone": "UTC", "raw_capture_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
        "expected_cases": len(cases), **corpus["metadata"],
    }]
    for observed, expected in zip(cases, corpus["cases"], strict=True):
        assert all(observed[key] == expected[key] for key in ("id", "ansi", "sql", "dar_sql", "scope"))
        assert observed["effective_settings"]["spark.sql.ansi.enabled"] == str(expected["ansi"]).lower()
        row = {key: observed[key] for key in ("id", "ansi", "ordered", "scope", "status")}
        row["sql"] = observed["dar_sql"]
        if observed["dar_sql"] != observed["sql"]:
            row["spark_sql"] = observed["sql"]
        if observed["status"] == "ok":
            row["expected"] = {"schema": observed["schema_stages"]["executed"], "rows": observed["rows_raw"]}
        else:
            assert observed["status"] == "error"
            row["expected"] = observed["expected"]
        compact.append(row)
    with output.open("x") as destination:
        for row in compact:
            print(json.dumps(row, separators=(",", ":")), file=destination)


if __name__ == "__main__":
    main()
