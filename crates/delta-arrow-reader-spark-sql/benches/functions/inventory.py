#!/usr/bin/env python3
"""Reconcile the frozen function manifest with the actual registration lists."""

import collections
import hashlib
import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[3]
FUNCTION = ROOT / "crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function"
BASE = "cde51da6dca2203f53dfe702dc06914931bd7845"
COLUMNS = "id grp high_key i p d dec s s2 num_s date_s ts_s time_s tm map_s json_array_s json_s csv_s xml_s url_s b hex_s hll_b theta_b dt ts flag arr nested records vec wkb cipher m m2 st".split()


def mask(source):
    """Keep delimiters and offsets, hide Rust comments and string contents."""
    pattern = r"//[^\n]*|/\*[\s\S]*?\*/|\br(?P<hashes>#{0,16})\"[\s\S]*?\"(?P=hashes)|'(?:\\.|[^'\\\n])'|\"(?:\\.|[^\"\\])*\""
    return re.sub(pattern, lambda match: "".join("\n" if c == "\n" else " " for c in match[0]), source, flags=re.S)


def closing(code, start):
    pairs = {"(": ")", "[": "]", "{": "}"}
    stack = []
    for index in range(start, len(code)):
        char = code[index]
        if char in pairs:
            stack.append(pairs[char])
        elif char in ")]}":
            assert stack and char == stack.pop(), (index, char)
            if not stack:
                return index
    raise AssertionError("unclosed Rust list")


def registrations(path, kind):
    source = path.read_text()
    code = mask(source)
    match = re.search(r"fn list_built_in_\w+functions\(", code)
    if not match:
        return []
    body = code.index("{", match.end())
    end = closing(code, body)
    start = code.index("vec![", body, end) + 4
    end = closing(code, start)
    rows = []
    index = start + 1
    while index < end:
        if code[index] in " \n\t\r,":
            index += 1
            continue
        assert code[index] == "(", (path, source[index:index + 60])
        finish = closing(code, index)
        entry = source[index:finish + 1]
        name = re.match(r'\(\s*"([^"\\]+)"\s*,', entry)
        assert name, (path, entry)
        expression = entry[name.end():].rstrip().removesuffix(")").strip().removesuffix(",").strip()
        rows.append({"kind": kind, "category": path.stem if kind == "scalar" else kind,
                     "name": name[1], "registration": expression,
                     "anchor": f"{path.relative_to(ROOT)}:{source.count(chr(10), 0, index) + 1}"})
        index = finish + 1
    return rows


def registry():
    paths = sorted(path for path in FUNCTION.joinpath("scalar").glob("*.rs") if path.stem != "mod")
    rows = [row for path in paths for row in registrations(path, "scalar")]
    declared = set(re.findall(r"output\.extend\((?:r#)?(\w+)::list_built_in_\w+functions\(\)\);", (FUNCTION/"scalar/mod.rs").read_text()))
    assert {row["category"] for row in rows} == declared, "scalar module/registry coverage drift"
    for kind in ("aggregate", "window", "generator"):
        rows.extend(registrations(FUNCTION / f"{kind}.rs", kind))
    winners = {}
    shadows = []
    for row in rows:
        key = row["kind"], row["name"]
        if key in winners:
            shadows.append(winners[key])
        winners[key] = row
    return sorted(winners.values(), key=lambda row: (row["kind"], row["category"], row["name"])), shadows, paths


def arguments():
    """One ordinary bounded column form. Constants are tuning/format arguments."""
    out = {}

    def add(names, args):
        for name in names.split():
            out[name] = args

    add("abs bin bit_count bitwise_not negative positive sign signum hex", "i")
    add("acos asin asinh atan atanh cbrt cos cosh cot csc degrees exp expm1 floor ceil ceiling ln log10 log1p log2 radians rint round bround sec sin sinh sqrt tan tanh", "d")
    out["acosh"] = "d+1.0"
    add("atan2 hypot pow power", "d,d+1.0")
    add("greatest least", "i,CAST(p AS BIGINT)")
    out.update(log="10.0,d", conv="hex_s,16,10", factorial="p", div="i,p", mod="i,p", pmod="i,p", unhex="hex_s", width_bucket="d,0.0,1.0,10")
    add("try_add try_subtract try_multiply try_mod", "i,CAST(p AS BIGINT)")
    out["try_divide"] = "d,CAST(p AS DOUBLE)"
    add("bit_get getbit shiftleft shiftright shiftrightunsigned", "i,1")
    add("coalesce ifnull nvl nullif", "i,CAST(p AS BIGINT)")
    add("nullifzero zeroifnull", "i")
    out.update(nanvl="d,d+1.0", nvl2="i,i,CAST(p AS BIGINT)")
    add("array_compact array_distinct array_max array_min array_size cardinality size reverse shuffle", "arr")
    add("array_contains array_position array_remove array_append array_prepend", "arr,i")
    add("array_contains_all array_except array_intersect array_union arrays_overlap arrays_zip", "arr,arr")
    out.update(array="i,CAST(p AS BIGINT)", array_insert="arr,2,i", array_join="arr,','", array_repeat="i,3", flatten="nested", get="arr,1", sequence="p,p+2", slice="arr,1,2", sort_array="arr", element_at="arr,1", try_element_at="arr,1", concat="s,s2", deep_size="arr")
    out.update(aggregate="arr,CAST(0 AS BIGINT),(a,x) -> a+coalesce(x,0)", reduce="arr,CAST(0 AS BIGINT),(a,x) -> a+coalesce(x,0)", array_sort="arr", exists="arr,x -> x>2", forall="arr,x -> x>2", filter="arr,x -> x>2", transform="arr,x -> x+1", zip_with="arr,arr,(x,y) -> x+y", map_filter="m,(k,v) -> v>2", map_zip_with="m,m,(k,x,y) -> x+y", transform_keys="m,(k,v) -> concat(k,'x')", transform_values="m,(k,v) -> v+1")
    out.update(map="s,i,s2,CAST(p AS BIGINT)", map_concat="m,m2", map_contains_key="m,'a'", map_entries="m", map_from_arrays="array(s,s2),array(i,CAST(p AS BIGINT))", map_from_entries="map_entries(m)", map_keys="m", map_values="m", str_to_map="map_s")
    add("ascii bit_length char_length character_length initcap lcase len length lower luhn_check make_valid_utf8 mask octet_length quote soundex trim ltrim rtrim btrim ucase upper validate_utf8 try_validate_utf8 is_valid_utf8", "s")
    add("char chr space", "p")
    add("contains endswith instr startswith strpos", "s,s2")
    add("left right repeat", "s,p")
    add("lpad rpad", "s,12,'x'")
    add("position locate", "s2,s")
    add("substr substring", "s,2,3")
    out.update(base64="b", unbase64="b", concat_ws="',',s,s2", decode="b,'UTF-8'", encode="s,'UTF-8'", elt="p,s,s2", find_in_set="s,concat(s,',',s2)", format_number="dec,2", format_string="'%s',s", printf="'%s',s", levenshtein="s,s2", overlay="s,s2,2", regexp_count="s,'[0-9]'", regexp_extract="s,'([0-9]+)',1", regexp_extract_all="s,'([0-9]+)',1", regexp_instr="s,'[0-9]'", regexp_replace="s,'[0-9]','x'", regexp_substr="s,'[0-9]+'", replace="s,'item','row'", sentences="s", split="s,'-'", split_part="s,'-',2", substring_index="s,'-',1", to_binary="hex_s,'hex'", try_to_binary="hex_s,'hex'", to_char="dec,'9999.99'", to_varchar="dec,'9999.99'", to_number="num_s,'9999.99'", try_to_number="num_s,'9999.99'", translate="s,'it','IT'")
    add("crc32 md5 sha sha1", "s")
    add("hash xxhash64", "i,s")
    out["sha2"] = "s,256"
    out.update(from_csv="csv_s,'x BIGINT, label STRING'", to_csv="st", from_json="json_s,'x BIGINT, a ARRAY<INT>'", get_json_object="json_s,'$.x'", json_array_length="json_array_s", json_object_keys="json_s", json_tuple="json_s,'x'", to_json="st")
    add("parse_json try_parse_json", "json_s")
    add("is_variant_null schema_of_variant variant_to_json", "parse_json(json_s)")
    add("variant_get try_variant_get", "parse_json(json_s),'$.x','BIGINT'")
    out["to_variant_object"] = "st"
    out.update(from_xml="xml_s,'x BIGINT'", to_xml="st", xpath="xml_s,'/r/x/text()'")
    add("xpath_boolean xpath_double xpath_float xpath_int xpath_long xpath_number xpath_short xpath_string", "xml_s,'/r/x'")
    out.update(parse_url="url_s,'HOST'", url_decode="s", url_encode="s", try_parse_url="url_s,'HOST'", try_url_decode="s", st_asbinary="wkb", st_geomfromwkb="wkb", st_geogfromwkb="wkb", vector_inner_product="vec,vec")
    add("day dayofmonth dayofweek dayofyear dayname month monthname quarter weekofyear weekday year years last_day unix_date", "dt")
    add("hour minute second unix_micros unix_millis unix_seconds", "ts")
    out["window_time"] = "window(ts,'5 minutes')"
    add("add_years add_months add_days date_add date_sub dateadd", "dt,p")
    add("date_diff datediff", "dt,CAST('2020-01-01' AS DATE)")
    out.update(date_format="ts,'yyyy-MM-dd'", date_from_unix_date="p", date_part="'YEAR',ts", datepart="'YEAR',ts", date_trunc="'DAY',ts", extract="YEAR FROM ts", from_unixtime="CAST(p AS BIGINT)", from_utc_timestamp="ts,'UTC'", to_utc_timestamp="ts,'UTC'", convert_timezone="'UTC','UTC',CAST(ts AS TIMESTAMP_NTZ)", make_date="2020,1,p", make_dt_interval="p", make_interval="p", try_make_interval="p", make_time="p,0,CAST(0 AS DECIMAL(16,6))", make_ym_interval="p", months_between="dt,CAST('2020-01-01' AS DATE)", next_day="dt,'MON'", session_window="ts,'5 minutes'", window="ts,'5 minutes'", trunc="dt,'MONTH'", timestampadd="DAY,p,ts", timestampdiff="DAY,ts,ts", to_unix_timestamp="ts_s", unix_timestamp="ts_s", time_diff="'SECOND',CAST('12:00:00' AS TIME),CAST('13:00:00' AS TIME)", timediff="'SECOND',CAST('12:00:00' AS TIME),CAST('13:00:00' AS TIME)", time_trunc="'HOUR',CAST(ts_s AS TIME)", time_bucket="INTERVAL '1' HOUR,ts")
    add("to_date try_to_date", "date_s")
    add("to_timestamp to_timestamp_ltz to_timestamp_ntz try_to_timestamp", "ts_s")
    add("to_time try_to_time", "time_s")
    add("time_to_micros time_to_millis time_to_seconds", "tm")
    add("time_diff", "'SECOND',tm,CAST('13:00:00' AS TIME)")
    out["timediff"] = "SECOND,ts,CAST('2020-01-01 12:00:00' AS TIMESTAMP)"
    out["time_trunc"] = "'HOUR',tm"
    add("make_timestamp make_timestamp_ltz make_timestamp_ntz try_make_timestamp try_make_timestamp_ltz try_make_timestamp_ntz", "2020,1,p,12,34,CAST(56 AS DECIMAL(16,6))")
    add("timestamp_micros timestamp_millis timestamp_seconds time_from_micros time_from_millis time_from_seconds", "CAST(p AS BIGINT)")
    out.update(bitmap_bit_position="i", bitmap_bucket_number="i", bitmap_count="b", equal_null="i,CAST(p AS BIGINT)", hll_sketch_estimate="hll_b", hll_union="hll_b,hll_b", theta_sketch_estimate="theta_b", theta_difference="theta_b,theta_b", theta_intersection="theta_b,theta_b", theta_union="theta_b,theta_b", typeof="i")
    add("aes_encrypt try_aes_encrypt", "b,'0123456789abcdef','ECB'")
    add("aes_decrypt try_aes_decrypt", "cipher,'0123456789abcdef','ECB'")
    add("isnan", "d")
    add("isnull isnotnull", "i")
    add("like ilike", "s,'item%'")
    add("regexp regexp_like rlike", "s,'item'")
    out.update(named_struct="'x',i", struct="i,p")
    return out


ARGS = arguments()
NATIVE_NAMES = {
    "lcase":"lower", "ucase":"upper", "char":"chr", "len":"length", "character_length":"character_length", "sha":"sha1", "power":"power", "signum":"signum", "negative":"-", "positive":"+", "getbit":"get_bit", "map_contains_key":"map_extract", "array_prepend":"array_prepend", "array_repeat":"array_repeat", "sort_array":"array_sort", "array_size":"array_length", "size":"array_length", "cardinality":"array_length", "array_compact":"array_remove_all", "array_contains_all":"array_has_all", "array_contains":"array_has", "array_append":"array_append", "array_intersect":"array_intersect", "array_position":"array_position", "arrays_overlap":"array_has_any", "arrays_zip":"arrays_zip", "flatten":"flatten", "map_keys":"map_keys", "map_values":"map_values", "map_entries":"map_entries", "map_from_arrays":"map", "map_from_entries":"map_from_entries", "substring_index":"substr_index", "locate":"strpos", "nullifzero":"nullif", "zeroifnull":"coalesce", "nvl":"coalesce", "ifnull":"coalesce", "equal_null":"is_not_distinct_from", "date_from_unix_date":"to_date", "mean":"avg", "std":"stddev", "stddev_samp":"stddev", "variance":"var_samp", "some":"bool_or", "any":"bool_or", "every":"bool_and", "first":"first_value", "last":"last_value", "percentile_approx":"approx_percentile_cont", "approx_percentile":"approx_percentile_cont", "approx_count_distinct":"approx_distinct", "collect_list":"array_agg", "collect_set":"array_agg", "string_agg":"string_agg", "listagg":"string_agg", "array_join":"array_to_string", "position":"strpos", "round":"round", "sha2":"sha256", "to_date":"to_date", "try_to_date":"to_date", "to_timestamp_ntz":"to_timestamp_micros", "to_timestamp":"to_timestamp_micros", "to_timestamp_ltz":"to_timestamp_micros", "try_to_timestamp":"to_timestamp_micros"
}

# The native registry is checked at preflight. These are explicit equivalent SQL forms,
# not assertions that equal names have equal behavior.
NATIVE_EXPR = {
    "array_compact":"array_remove_all(arr,CAST(NULL AS BIGINT))", "array_remove":"array_remove_all(arr,i)",
    "negative":"-i", "positive":"+i", "nvl2":"CASE WHEN i IS NOT NULL THEN i ELSE CAST(p AS BIGINT) END",
    "nullifzero":"nullif(i,0)", "zeroifnull":"coalesce(i,0)", "nanvl":"CASE WHEN isnan(d) THEN d+1.0 ELSE d END",
    "if":"CASE WHEN flag THEN i ELSE CAST(p AS BIGINT) END", "equal_null":"i IS NOT DISTINCT FROM CAST(p AS BIGINT)",
    "map_contains_key":"array_has(map_keys(m),'a')", "base64":"encode(b,'base64')", "unbase64":"decode(CAST(b AS VARCHAR),'base64')",
    "to_date":"CAST(date_s AS DATE)", "try_to_date":"TRY_CAST(date_s AS DATE)", "to_timestamp":"CAST(ts_s AS TIMESTAMP)", "to_timestamp_ltz":"CAST(ts_s AS TIMESTAMP)", "to_timestamp_ntz":"CAST(ts_s AS TIMESTAMP)", "try_to_timestamp":"TRY_CAST(ts_s AS TIMESTAMP)",
    "date_from_unix_date":"CAST(p AS DATE)", "unix_date":"CAST(dt AS INT)", "array_repeat":"array_repeat(i,CAST(3 AS BIGINT))",
    "sort_array":"array_sort(arr,'ASC','NULLS FIRST')", "array_sort":"array_sort(arr,'ASC','NULLS LAST')",
    "element_at":"array_element(arr,1)", "try_element_at":"array_element(arr,1)", "get":"array_element(arr,2)",
    "array_prepend":"array_prepend(i,arr)", "array_join":"array_to_string(arr,',')", "map_from_arrays":"map(array(s,s2),array(i,CAST(p AS BIGINT)))",
    "array":"make_array(i,CAST(p AS BIGINT))", "struct":"struct(i,p)", "named_struct":"named_struct('x',i)",
    "isnull":"i IS NULL", "isnotnull":"i IS NOT NULL", "like":"s LIKE 'item%'", "ilike":"s ILIKE 'item%'",
    "regexp":"regexp_like(s,'item')", "rlike":"regexp_like(s,'item')", "regexp_like":"regexp_like(s,'item')",
    "bitwise_not":"~i", "bit_get":"(i >> 1) & 1", "getbit":"(i >> 1) & 1", "shiftleft":"i << 1", "shiftright":"i >> 1",
    "csc":"1.0/sin(d)", "sec":"1.0/cos(d)", "hypot":"sqrt(d*d+(d+1.0)*(d+1.0))", "rint":"round(d)",
    "try_add":"i+CAST(p AS BIGINT)", "try_subtract":"i-CAST(p AS BIGINT)", "try_multiply":"i*CAST(p AS BIGINT)", "try_divide":"d/CAST(p AS DOUBLE)", "try_mod":"i%CAST(p AS BIGINT)",
    "ceil":"CAST(ceil(d) AS BIGINT)", "ceiling":"CAST(ceil(d) AS BIGINT)", "floor":"CAST(floor(d) AS BIGINT)", "try_avg":"avg(d)", "try_sum":"sum(i)",
    "array_size":"CAST(array_length(arr) AS INT)", "size":"CAST(array_length(arr) AS INT)", "cardinality":"CAST(array_length(arr) AS INT)",
    "sha1":"encode(sha1(s),'hex')", "sha":"encode(sha1(s),'hex')", "sha2":"encode(sha256(s),'hex')",
    "startswith":"starts_with(s,s2)", "endswith":"ends_with(s,s2)", "split":"string_to_array(s,'-')", "array_insert":"array_concat(array_slice(arr,1,1),make_array(i),array_slice(arr,2,3))",
    "vector_inner_product":"CAST(inner_product(CAST(vec AS ARRAY<FLOAT>),CAST(vec AS ARRAY<FLOAT>)) AS FLOAT)",
    "regexp_extract":"array_element(regexp_match(s,'([0-9]+)'),1)",
    "count_if":"count(*) FILTER (WHERE flag)", "any_value":"first_value(i)",
    "max_by":"first_value(i ORDER BY id DESC)", "min_by":"first_value(i ORDER BY id ASC)",
    "approx_count_distinct":"CAST(approx_distinct(i) AS BIGINT)",
}


for spark_name, part in {"day":"day","dayofmonth":"day","dayofyear":"doy","month":"month","quarter":"quarter","year":"year","years":"year","weekofyear":"week","hour":"hour","minute":"minute","second":"second"}.items():
    NATIVE_EXPR[spark_name] = f"CAST(date_part('{part}',{'ts' if spark_name in ('hour','minute','second') else 'dt'}) AS INT)"
for spark_name, unit in {"add_days":"DAY","date_add":"DAY","dateadd":"DAY","date_sub":"DAY","add_months":"MONTH","add_years":"YEAR"}.items():
    sign = "-" if spark_name == "date_sub" else "+"
    NATIVE_EXPR[spark_name] = f"CAST(dt {sign} CAST(concat(CAST(p AS VARCHAR),' {unit.lower()}s') AS INTERVAL) AS DATE)"
NATIVE_EXPR.update({
    "weekday":"CAST((date_part('dow',dt)+6)%7 AS INT)", "dayofweek":"CAST(date_part('dow',dt)+1 AS INT)",
    "date_diff":"CAST(dt AS INT)-CAST(CAST('2020-01-01' AS DATE) AS INT)", "datediff":"CAST(dt AS INT)-CAST(CAST('2020-01-01' AS DATE) AS INT)",
    "last_day":"CAST(date_trunc('MONTH',CAST(dt AS TIMESTAMP))+INTERVAL '1' MONTH-INTERVAL '1' DAY AS DATE)",
    "trunc":"CAST(date_trunc('MONTH',CAST(dt AS TIMESTAMP)) AS DATE)", "dayname":"to_char(dt,'%a')", "monthname":"to_char(dt,'%b')",
    "to_timestamp":"CAST(ts_s AS TIMESTAMP) AT TIME ZONE 'UTC'", "to_timestamp_ltz":"CAST(ts_s AS TIMESTAMP) AT TIME ZONE 'UTC'", "try_to_timestamp":"TRY_CAST(ts_s AS TIMESTAMP) AT TIME ZONE 'UTC'",
    "timestamp_micros":"to_timestamp_micros(CAST(p AS BIGINT)) AT TIME ZONE 'UTC'", "timestamp_millis":"to_timestamp_millis(CAST(p AS BIGINT)) AT TIME ZONE 'UTC'", "timestamp_seconds":"to_timestamp_seconds(CAST(p AS BIGINT)) AT TIME ZONE 'UTC'",
    "unix_seconds":"CAST(date_part('epoch',ts) AS BIGINT)", "unix_millis":"CAST(date_part('epoch',ts)*1000 AS BIGINT)", "unix_micros":"CAST(date_part('epoch',ts)*1000000 AS BIGINT)",
    "to_unix_timestamp":"CAST(date_part('epoch',CAST(ts_s AS TIMESTAMP)) AS BIGINT)", "unix_timestamp":"CAST(date_part('epoch',CAST(ts_s AS TIMESTAMP)) AS BIGINT)",
    "hex":"upper(to_hex(i))", "unhex":"decode(hex_s,'hex')", "mod":"i%CAST(p AS BIGINT)", "pmod":"((i%CAST(p AS BIGINT))+CAST(p AS BIGINT))%CAST(p AS BIGINT)",
    "div":"CAST(CAST(i AS DOUBLE)/CAST(p AS DOUBLE) AS BIGINT)", "expm1":"exp(d)-1.0", "log1p":"ln(d+1.0)", "sign":"CAST(signum(i) AS DOUBLE)",
    "space":"repeat(' ',p)", "array_repeat":"array_repeat(i,CAST(3 AS BIGINT))", "slice":"array_slice(arr,1,2)", "sequence":"range(p,p+3)",
    "shiftrightunsigned":"((i & 9223372036854775807) >> 1) + CASE WHEN i < 0 THEN 4611686018427387904 ELSE 0 END",
    "bitmap_bit_position":"CASE WHEN i>0 THEN (i-1)%32768 ELSE (-i)%32768 END", "bitmap_bucket_number":"CASE WHEN i>0 THEN 1+(i-1)/32768 ELSE i/32768 END",
    "typeof":"'bigint'", "array_from_arrays":"map(make_array(s,s2),make_array(i,CAST(p AS BIGINT)))",
    "convert_timezone":"arrow_cast(ts,'Timestamp(Microsecond, None)')", "from_utc_timestamp":"ts", "to_utc_timestamp":"ts",
    "to_binary":"decode(hex_s,'hex')", "try_to_binary":"decode(hex_s,'hex')", "try_to_time":"to_time(time_s)",
    "regexp_substr":"array_element(regexp_match(s,'([0-9]+)'),1)",
    "reverse":"array_reverse(arr)", "deep_size":"cardinality(arr)", "bitwise_not":"-1-i",
    "decode":"arrow_cast(b,'Utf8')", "encode":"arrow_cast(s,'Binary')",
    "position":"strpos(s,s2)", "locate":"strpos(s,s2)",
    "bit_get":"CAST((i >> 1) & 1 AS TINYINT)", "getbit":"CAST((i >> 1) & 1 AS TINYINT)",
    "array_position":"CASE WHEN i IS NULL OR arr IS NULL THEN CAST(NULL AS BIGINT) ELSE coalesce(CAST(array_position(arr,i) AS BIGINT),0) END",
    "array_contains":"CASE WHEN i IS NULL OR arr IS NULL THEN CAST(NULL AS BOOLEAN) WHEN array_has(arr,i) THEN TRUE WHEN array_position(arr,CAST(NULL AS BIGINT)) IS NOT NULL THEN CAST(NULL AS BOOLEAN) ELSE FALSE END",
    "struct":"named_struct('i',i,'p',p)", "make_time":"arrow_cast(make_time(p,0,0),'Time64(Microsecond)')",
    "from_unixtime":"to_char(from_unixtime(CAST(p AS BIGINT)),'%Y-%m-%d %H:%M:%S')",
})

NATIVE_TYPES = {
    "array_compact":"List(non-null Int64)", "array_agg":"List(non-null Int64)", "collect_list":"List(non-null Int64)", "collect_set":"List(non-null Int64)",

    "sequence":"List(non-null Int32)",
    "listagg":"Utf8", "string_agg":"Utf8", "substr":"Utf8", "substring":"Utf8", "substring_index":"Utf8View",
    "to_time":"Time64(Microsecond)", "try_to_time":"Time64(Microsecond)", "timestamp_micros":'Timestamp(Microsecond, Some("UTC"))', "timestamp_millis":'Timestamp(Microsecond, Some("UTC"))', "timestamp_seconds":'Timestamp(Microsecond, Some("UTC"))',
    "to_timestamp":'Timestamp(Microsecond, Some("UTC"))', "to_timestamp_ltz":'Timestamp(Microsecond, Some("UTC"))', "to_timestamp_ntz":"Timestamp(Microsecond, None)", "try_to_timestamp":'Timestamp(Microsecond, Some("UTC"))',
}


def expression(row):
    name, category = row["name"], row["category"]
    if row["kind"] in ("aggregate", "window"):
        args = "i"
        if name in "avg mean std stddev stddev_pop stddev_samp var_pop var_samp variance median percentile approx_percentile percentile_approx percentile_cont percentile_disc kurtosis skewness product try_avg".split(): args = "d"
        if name in "bool_and bool_or any every some count_if".split(): args = "flag"
        if name in "corr covar_pop covar_samp regr_avgx regr_avgy regr_count regr_intercept regr_r2 regr_slope regr_sxx regr_sxy regr_syy".split(): args = "d,CAST(p AS DOUBLE)"
        if name in ("max_by","min_by"): args = "i,id"
        if name in ("approx_percentile","percentile_approx","percentile","percentile_cont","percentile_disc"): args += ",0.5"
        if name == "count_min_sketch": args = "i,CAST(0.1 AS DOUBLE),CAST(0.9 AS DOUBLE),7"
        if name == "bitmap_construct_agg": args = "CAST(p AS BIGINT)"
        if name == "histogram_numeric": args = "d,4"
        if name in ("listagg","string_agg","array_join"): args = "s,','"
        if name in ("hll_union_agg","bitmap_and_agg","bitmap_or_agg"): args = "hll_b" if name == "hll_union_agg" else "b"
        if name in ("theta_union_agg","theta_intersection_agg"): args = "theta_b"
        if name == "schema_of_variant_agg": args = "parse_json(json_s)"
        if name in ("row_number","rank","dense_rank","percent_rank","cume_dist"): args = ""
        if name == "ntile": args = "4"
        if name == "nth_value": args = "i,2"
        return f"{name}({args})"
    if row["kind"] == "generator":
        if name == "stack": return "stack(2,i,CAST(p AS BIGINT))"
        if name.startswith("variant_"): return f"{name}(parse_json(json_s))"
        return f"{name}(records)" if "inline" in name else f"{name}(arr)"
    if category == "conversion":
        types = dict(bigint="BIGINT",binary="BINARY",boolean="BOOLEAN",date="DATE",decimal="DECIMAL(10,0)",double="DOUBLE",float="FLOAT",int="INT",smallint="SMALLINT",string="STRING",time="TIME",timestamp="TIMESTAMP",tinyint="TINYINT")
        arg = "date_s" if name == "date" else "time_s" if name == "time" else "ts_s" if name == "timestamp" else "flag" if name == "boolean" else "s" if name == "binary" else "i"
        native = f"CAST({arg} AS {types.get(name, 'BIGINT')})"
        target = {"binary":"Binary","string":"Utf8","time":"Time64(Microsecond)","timestamp":'Timestamp(Microsecond, Some("UTC"))'}.get(name)
        if target: native = f"arrow_cast({arg},'{target}')"
        return f"{name}({arg})", native
    operators = {"!":"NOT flag","not":"NOT flag","~":"~i","and":"flag AND (i>0)","or":"flag OR (i>0)","in":"i IN (1,2)","if":"if(flag,i,CAST(p AS BIGINT))","case":"CASE WHEN flag THEN i ELSE CAST(p AS BIGINT) END","when":"CASE WHEN flag THEN i END"}
    for op in ("+","-","*","/","%","&","|","^","<<",">>",">>>","!=","<","<=","<=>","=","==",">",">="):
        operators[op] = f"i {op} CAST(p AS BIGINT)"
    if name in operators:
        native = operators[name].replace("<=>", "IS NOT DISTINCT FROM").replace("==", "=")
        if name == "if": native = NATIVE_EXPR["if"]
        if name == "/": native = "CAST(i AS DOUBLE)/CAST(p AS DOUBLE)"
        if name == "^": native = "(i | CAST(p AS BIGINT))-(i & CAST(p AS BIGINT))"
        if name == "~": native = "-1-i"
        if name == ">>>": native = "((i & 9223372036854775807) >> p) + CASE WHEN i < 0 THEN (CAST(1 AS BIGINT) << (63-p)) ELSE 0 END"
        return operators[name], native
    if name in ARGS:
        if name == "vector_inner_product": return "vector_inner_product(CAST(vec AS ARRAY<FLOAT>),CAST(vec AS ARRAY<FLOAT>))"
        return f"{name}({ARGS[name]})"
    return None


def exclusions(row):
    name = row["name"]
    if "F::unknown" in row["registration"]:
        return "registry_placeholder", "Registry explicitly uses F::unknown; this is not an executable implementation."
    if name in "e pi curdate current_date current_time current_timestamp current_timezone localtimestamp now current_catalog current_database current_path current_schema current_user session_user user version".split():
        return "no_column_form", "The registered form accepts no data columns and resolves a constant or session/clock value; it has no ordinary column execution form."
    if name in "rand random randn random_poisson uniform randstr".split():
        return "literal_only_random", "The registered random implementation accepts only foldable seed/length/distribution arguments, not varying data columns; see implementation anchors."
    if name in "uuid monotonically_increasing_id spark_partition_id input_file_name input_file_block_start input_file_block_length".split():
        return "nondeterministic_or_partition_context", "Randomness or physical partition/file identity has no deterministic native row reference for this in-memory table; the source registration identifies the context-dependent implementation."
    if name in ("schema_of_json", "schema_of_csv"):
        return "literal_only", "The schema-inference implementation requires a foldable string input; a varying column is not a valid form."
    if name in ("raise_error", "assert_true"):
        return "intentional_error", "The ordinary varying input intentionally raises an error; no successful full-input function cost exists on this fixture."
    return None


def source_anchors(rows):
    symbols = collections.defaultdict(list)
    roots = [FUNCTION, FUNCTION.parents[2] / "sail-function/src", FUNCTION.parents[4] / "dependencies/datafusion-functions/src"]
    for root in roots:
        for path in sorted(root.rglob("*.rs")):
            text = path.read_text()
            for match in re.finditer(r"\b(?:fn|struct|enum)\s+(\w+)", mask(text)):
                symbols[match[1]].append((path, text.count("\n",0,match.start())+1))
    for row in rows:
        path = ROOT / row["anchor"].rsplit(":",1)[0]
        anchors = {row["anchor"]}
        for token in set(re.findall(r"\b\w+\b", row["registration"])) - {"new", "from", "default", "custom", "udf"}:
            matches = symbols[token]
            local = [entry for entry in matches if entry[0] == path]
            for found, line in local or matches[:4]:
                anchors.add(f"{found.relative_to(ROOT)}:{line}")
                # Capture the UDF types used by a local planner helper as source evidence.
                if found == path:
                    lines = found.read_text().splitlines()
                    for symbol in set(re.findall(r"\b(?:Spark\w+|[A-Z]\w+(?:Function|Func))\b", "\n".join(lines[line-1:line+90]))):
                        for p, n in symbols[symbol][:2]: anchors.add(f"{p.relative_to(ROOT)}:{n}")
        row["implementation_anchors"] = sorted(anchors)


def build():
    rows, shadows, paths = registry()
    native_registry = json.loads((HERE / "native-registry.json").read_text())
    for kind in ("scalar","generator"):
        assert sorted(row["name"] for row in rows if row["kind"] == kind) == native_registry[kind], kind
    native_names = set(native_registry["native_scalar"] + native_registry["native_aggregate"] + native_registry["native_window"])
    source_anchors(rows)
    for row in rows:
        name = row["name"]
        row["id"] = f'{row["kind"]}.{row["category"]}.' + (name if re.fullmatch(r"\w+",name) else "op_" + name.encode().hex())
        row["aliases"] = sorted(other["name"] for other in rows if other["kind"] == row["kind"] and other["name"] != name and re.sub(r"\s+","",other["registration"]) == re.sub(r"\s+","",row["registration"]))
        excluded = exclusions(row)
        expr = expression(row) if not excluded else None
        native = None
        if expr is not None:
            if isinstance(expr,tuple): expr, native = expr
            elif name in NATIVE_EXPR: native = NATIVE_EXPR[name]
            else:
                native_name = NATIVE_NAMES.get(name,name)
                native = native_name + expr[len(name):]
            if native: native = re.sub(r"\barray\(","make_array(",native)
            if row["kind"] == "generator": native = None
            if row["kind"] in ("aggregate","window"):
                if name in ("collect_list","array_agg"): native = "array_agg(i) FILTER (WHERE i IS NOT NULL)"
                if name == "collect_set": native = "array_agg(DISTINCT i) FILTER (WHERE i IS NOT NULL)"
                if name in ("listagg","string_agg","array_join"): native = "string_agg(s,',')"
                if name in ("percentile_approx","approx_percentile"): native = "approx_percentile_cont(d,0.5)"
                if name == "percentile": native = "percentile_cont(0.5) WITHIN GROUP (ORDER BY d)"
                if name in ("percentile_cont","percentile_disc"):
                    expr = f"{name}(0.5) WITHIN GROUP (ORDER BY d)"
                    native = expr
                if row["kind"] == "window":
                    if name == "approx_count_distinct": native = "approx_distinct(i)"
                    if name == "max_by": native = "last_value(i)"
                    if name == "min_by": native = "first_value(i)"
                    if name == "percentile": native = "quantile_cont(d,0.5)"
                    frame = "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW"
                    if name in ("row_number","rank","dense_rank","percent_rank","ntile"):
                        frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                    elif name == "cume_dist":
                        frame = "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                    elif name in ("lag","lead"):
                        frame = ""
                    over = f" OVER (PARTITION BY grp ORDER BY id{(' '+frame) if frame else ''})"
                    expr += over
                    if native is not None: native += over
                    if name in ("row_number","rank","dense_rank","ntile"):
                        native = f"CAST({native} AS INT)"
                    if name == "approx_count_distinct": native = f"CAST({native} AS BIGINT)"
                # Ordered collectors avoid comparing unspecified internal list order.
                if name in ("collect_list","array_agg","collect_set"):
                    expr = f"sort_array({expr})"; native = f"array_sort({native},'ASC','NULLS FIRST')"
            target = NATIVE_TYPES.get(name)
            if row["kind"] == "window" and name == "array_join": target = "Utf8"
            if native is not None and target:
                native = f"arrow_cast({native},'{target}')"
                row["native_type_alignment"] = target
            prefix = "SELECT " if row["kind"] == "aggregate" else "SELECT id,"
            row["spark_sql"] = f"{prefix}{expr} AS value FROM bench"
            row["native_sql"] = f"{prefix}{native} AS value FROM bench" if native else None
            if row["kind"] == "generator":
                row["spark_sql"] = f"SELECT id,{expr} FROM bench"
                if name in ("explode","stack"):
                    argument = "arr" if name == "explode" else "make_array(i,CAST(p AS BIGINT))"
                    row["native_sql"] = f"SELECT id,unnest({argument}) AS value FROM bench"
                elif name in ("inline","inline_outer"):
                    row["native_sql"] = "SELECT id,i AS value FROM bench" + (" WHERE records IS NOT NULL" if name == "inline" else "")
            if row["kind"] == "aggregate" and name in ("grouping","grouping_id"):
                row["spark_sql"] = f"SELECT grp,{name}(grp) AS value FROM bench GROUP BY GROUPING SETS ((grp),())"
                row["native_sql"] = f"SELECT grp,CAST(grouping(grp) AS {'TINYINT' if name == 'grouping' else 'BIGINT'}) AS value FROM bench GROUP BY GROUPING SETS ((grp),())"
            if row["native_sql"]:
                sql = re.sub(r"'(?:''|[^'])*'", "''", row["native_sql"])
                calls = set(re.findall(r"\b([a-zA-Z_]\w*)\s*\(", sql)) - {"CAST","TRY_CAST","OVER","FILTER","WHERE","GROUP","SETS","IN","AS","DECIMAL","ARRAY","GROUPING","AND","OR","THEN","ELSE","extract"}
                unavailable = sorted(calls - native_names - {"unnest"})
                if unavailable or name in ("format_number","format_string","printf","to_number","try_to_number","to_char","to_varchar","hash","xxhash64","shuffle"):
                    row["native_sql"] = None
                    row["native_unavailable"] = unavailable
                    row["native_rationale"] = "Native catalogue has no matching column implementation for the Spark " + row["category"] + " behavior; rejected native calls: " + (", ".join(unavailable) or "same spelling has different formatting/encoding/random/typed-hash semantics") + ". This is a source/registry classification, not a failed semantic comparison."
            words = set(re.findall(r"\b\w+\b", row["spark_sql"] + (row["native_sql"] or "")))
            columns = [column for column in COLUMNS if column in words]
            row["input_columns"] = columns
            row["projection_sql"] = f'SELECT {",".join(columns)} FROM bench'
        else:
            row.update(spark_sql=None,native_sql=None,projection_sql=None,input_columns=[])
        assert excluded or expr is not None, f"missing ordinary column form: {row['id']}"
        row["exclusion"] = dict(zip(("kind","reason"),excluded)) if excluded else None
        row["class"] = "paired" if row["native_sql"] else "Spark only"
        row["class_evidence"] = ("Explicit native reference candidate; both ANSI comparisons and native registry/physical evidence must pass before any timing. Same spelling alone supplies no equivalence evidence." if row["native_sql"] else row.get("native_rationale", row["exclusion"]["reason"] if row["exclusion"] else "Native catalogue has no corresponding generator expansion with these output fields and Spark outer/variant semantics. Source registration and the frozen native catalogue are the evidence."))
    counts = dict(collections.Counter(row["kind"] for row in rows))
    assert counts == {"scalar":457,"aggregate":84,"window":81,"generator":9}, counts
    assert len(shadows) == 4, shadows
    assert len({row["id"] for row in rows}) == len(rows)
    special = [
        dict(id="shape.decimal_division_round",kind="shape",category="close_noise",name="decimal_division_round",spark_sql="SELECT id,CAST(ROUND(CAST(dec AS DECIMAL(18,2))/CAST(p AS DECIMAL(18,2)),2) AS DECIMAL(18,2)) AS value FROM bench",native_sql="SELECT id,CAST(ROUND(CAST(dec AS DECIMAL(18,2))/CAST(p AS DECIMAL(18,2)),2) AS DECIMAL(18,2)) AS value FROM bench",projection_sql="SELECT id,dec,p FROM bench",input_columns=["id","dec","p"]),
        dict(id="shape.group_high",kind="shape",category="close_noise",name="group_high",spark_sql="SELECT high_key AS key,count(*) AS count,sum(i) AS total,max(dec) AS maximum FROM bench GROUP BY high_key",native_sql="SELECT high_key AS key,count(*) AS count,sum(i) AS total,max(dec) AS maximum FROM bench GROUP BY high_key",projection_sql="SELECT high_key,i,dec FROM bench",input_columns=["high_key","i","dec"]),
    ]
    for row in special: row.update(aliases=[],implementation_anchors=[],exclusion=None,**{"class":"paired","class_evidence":"The two newly requested close-noise expressions on this issue's in-memory input."})
    return {"source_commit":BASE,"rows":1_000_000,"seed":"0x13915eed20261119","registry_counts":counts,"distinct_spellings":len({row['name'] for row in rows}),"native_registry_sha256":hashlib.sha256((HERE/"native-registry.json").read_bytes()).hexdigest(),"registry_shadows":shadows,"table_registry_scope":"table functions are outside scalar/aggregate/window/generator scope; range is not silently counted as a scalar", "functions":rows+special}


def main():
    manifest = build()
    path = HERE / "manifest.json"
    if sys.argv[1:] == ["--write"]:
        path.write_text(json.dumps(manifest,indent=2)+"\n")
    else:
        assert sys.argv[1:] in ([],["--check"])
        assert json.loads(path.read_text()) == manifest, "registry/SQL freeze drift; regenerate explicitly before freeze"
    counts = collections.Counter(row["exclusion"]["kind"] if row["exclusion"] else "column_statement" for row in manifest["functions"])
    print(json.dumps({"registry_counts":manifest["registry_counts"],"statements":dict(counts),"status":"passed"}))


if __name__ == "__main__":
    main()
