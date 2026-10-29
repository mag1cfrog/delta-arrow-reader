# Analyze a table with SQL

Start here after [reading your first rows](cli-quickstart.md). Keep the table
path and chosen snapshot version. The examples assume columns named
`id`, `amount`, and `day`; substitute names from your table.

```sh
table=/path/to/table
version=0
```

Replace `0` with the version you chose. For remote tables, retain the
[authorization arguments](cli-storage.md) on every command.

## Filter and sort

Save one read-only SQL query in a file. The table is available as `t`:

```sh
cat > selected.sql <<'SQL'
SELECT id, amount
FROM t
WHERE amount >= 1.0000
ORDER BY id;
SQL
dar query "$table" --table-version "$version" --sql-file selected.sql --format json
```

Choose the columns that answer your question. Add SQL `LIMIT` only when a
subset is the intended answer. A successful query returns the whole SQL
result; `--max-rows` is a failure limit, not a way to take a sample.
Use sorting columns that resolve ties when the row order matters.

JSON rows are arrays in the order of `schema.fields`. Integers and decimals
are strings. A decimal string is the unscaled integer: `"12345"` with scale
4 means `1.2345`, not `12345`. Use the returned type to interpret values;
do not infer a type from their appearance. For an unfamiliar encoding, request
only its detail, for example `dar capabilities --schema rows --detail decimal`.

## Group and count

For a table with a `day` column:

```sh
cat > totals.sql <<'SQL'
SELECT day, COUNT(*) AS n
FROM t
GROUP BY day
ORDER BY day;
SQL
dar query "$table" --table-version "$version" --sql-file totals.sql --format json
```

If `day` is a `Date32`, its JSON values are integer strings counting days since
1970-01-01. `"0"` means that date, and `"1"` means 1970-01-02. The envelope's
`row_count` counts groups; each row's `n` counts records in that group.

JSON stdout is intended for small answers and defaults to an 8 KiB ceiling.
If the complete result is larger, [save the same query to a file](cli-results.md#save-a-complete-result).
Keep the SQL and versions unchanged when you still need the same answer.

## Join two snapshots

Use the supplied version for each table; inspect only tables whose version you
still need to discover. For two tables with an
`id` column, put this `tables.json` beside the `orders/` and `customers/` Delta
directories. Replace both versions with your chosen values:

```json
{"format_version":1,"tables":[
  {"alias":"orders","location":"./orders","table_version":"0"},
  {"alias":"customers","location":"./customers","table_version":"0"}
]}
```

```sh
cat > matched.sql <<'SQL'
SELECT o.id, COUNT(*) AS matches
FROM orders o JOIN customers c ON o.id = c.id
GROUP BY o.id
ORDER BY o.id;
SQL
dar query --tables-file tables.json --sql-file matched.sql --format json
```

Each alias keeps its own snapshot. These versions do not form a transaction
across tables. With `--tables-file`, put remote authorization references in each
table entry and omit positional `TABLE`, `--table-version`, and command-level
authorization flags. See [table manifest fields](reference/cli-inputs.md#join-table-snapshots)
when adding a remote binding.

## Filter a preview without SQL

A predicate file is useful when you want the same filter for `preview` and
`scan`. Use the column type returned by your preview. If you do not have it,
inspect that column before constructing the literal:

```sh
dar inspect "$table" --view summary --table-version "$version" --column id
```

This example requires an `Int64` `id`:

```sh
cat > predicate.json <<'JSON'
{"op":"ge","column":"id","value":{"type":"int64","value":"10"}}
JSON
dar preview "$table" --table-version "$version" --column id \
  --predicate-file predicate.json --limit 3
```

The integer literal must be a JSON string. A different column type needs its
own literal type; there is no automatic conversion. Retrieve only the rule
you lack, such as `dar capabilities --schema predicate --detail int64`.

<details markdown="1">
<summary>Filter an exact decimal value</summary>

For an `amount` column inspected as `Decimal128(18, 4)`, this predicate selects
the exact value `1.2345`:

```sh
cat > amount.json <<'JSON'
{"op":"eq","column":"amount","value":{"type":"decimal128","value":"12345","precision":18,"scale":4}}
JSON
dar preview "$table" --table-version "$version" --column id --column amount \
  --predicate-file amount.json --limit 3
```

Use the column's exact precision and scale. The literal's `value` is an
unscaled integer string. `dar capabilities --schema predicate --detail decimal128`
explains this input; the `rows --detail decimal` lookup explains the output.

</details>

For a rejected name, type, SQL statement, or resource limit, use
[Fix a failed command](cli-recovery.md). To keep a complete answer, see
[Save and read results](cli-results.md).
For query planning or resuming a task later, see [Automation and replay](cli-automation.md).
