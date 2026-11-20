"""Run the committed phase probe against a disposable copy of staging."""

import argparse
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
STAGING = "1ec4b096432007a0d9f0f8f9347cfce5e47cddc4"
WORKSPACE = Path("crates/delta-arrow-reader-spark-sql")
PARSER = WORKSPACE / "vendor/sail/crates/sail-sql-analyzer/src/parser.rs"


def instrument(parser):
    source = parser.read_text()
    start = source.index("pub(crate) fn parse_interval_literal")
    end = source.index("/// Parses an interval string", start)
    scoped = source[start:end].replace(
        "    parse!(s, create_interval_literal_parser)",
        """    use interval_attribution::timed;
    let options = ParserOptions::default();
    let lexer = timed(0, || create_lexer::<_, chumsky::extra::Err<
        chumsky::error::Rich<_, _>>>(&options));
    let tokens = timed(1, || lexer.parse(s).into_result().map_err(SqlError::parser))?;
    let tokens = tokens.as_slice().map((s.len()..s.len()).into(), map_parser_input);
    let parser = timed(0, || create_interval_literal_parser::<_, chumsky::extra::Err<
        chumsky::error::Rich<_, _>>>(&options));
    timed(2, || parser.parse(tokens).into_result().map_err(SqlError::parser))""",
    )
    start_batch = source.index("pub fn interval_parser")
    end_batch = source.index("pub fn parse_time(", start_batch)
    assert "interval_attribution::timed" in scoped
    batch = """pub fn interval_parser<'a>() -> impl FnMut(&'a str) -> SqlResult<IntervalValue> {
    use interval_attribution::timed;
    static OPTIONS: LazyLock<ParserOptions> = LazyLock::new(ParserOptions::default);
    let options = &*OPTIONS;
    let mut lexer = None;
    let mut parser = None;
    move |s| {
        let lexer = lexer.get_or_insert_with(|| timed(0, || {
            create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(options)
        }));
        let tokens = timed(1, || lexer.parse(s).into_result().map_err(SqlError::parser))?;
        let parser = parser.get_or_insert_with(|| timed(0, || {
            create_interval_literal_parser::<_, chumsky::extra::Err<
                chumsky::error::Rich<_, _>>>(options)
        }));
        let interval = timed(2, || {
            let tokens = Stream::from_iter(tokens).map((s.len()..s.len()).into(), |(token, span)| (token, span));
            parser.parse(tokens).into_result().map_err(SqlError::parser)
        })?;
        timed(3, || from_ast_unqualified_interval(interval))
    }
}

"""
    parser.write_text(
        source[:start] + scoped + source[end:start_batch] + batch
        + source[end_batch:] + (HERE / "attribute.rs").read_text()
    )


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("output", type=Path)
    cli.add_argument("target", type=Path)
    args = cli.parse_args()
    output = args.output.resolve()
    output.mkdir()
    checkout = output / "source"
    checkout.mkdir()
    with subprocess.Popen(["git", "archive", STAGING], cwd=HERE.parents[3], stdout=subprocess.PIPE) as archive:
        subprocess.run(["tar", "-xf", "-", "-C", checkout], stdin=archive.stdout, check=True)
        assert archive.wait() == 0
    instrument(checkout / PARSER)
    env = os.environ | {
        "CARGO_TARGET_DIR": str(args.target.resolve()),
        "CARGO_PROFILE_RELEASE_DEBUG": "1",
        "CARGO_PROFILE_RELEASE_OPT_LEVEL": "3",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16",
        "CARGO_PROFILE_RELEASE_LTO": "false",
        "CARGO_PROFILE_RELEASE_INCREMENTAL": "false",
        "TZ": "UTC",
    }
    with (output / "build.jsonl").open("w") as log, (output / "build.log").open("w") as errors:
        subprocess.run(
            ["cargo", "test", "--locked", "--release", "-j", "2", "-p", "sail-sql-analyzer",
             "--lib", "--no-run", "--message-format=json"],
            cwd=checkout / WORKSPACE, env=env, stdout=log, stderr=errors, check=True, timeout=1800,
        )
    artifacts = [json.loads(line) for line in (output / "build.jsonl").read_text().splitlines()]
    executables = [
        entry["executable"] for entry in artifacts
        if entry.get("reason") == "compiler-artifact" and entry.get("executable")
    ]
    assert len(executables) == 1, executables
    with (output / "phases.csv.log").open("w") as log:
        subprocess.run(
            ["timeout", "120s", "taskset", "-c", "2-5", "prlimit", "--as=4294967296", "--",
             executables[0], "--nocapture", "--exact", "parser::interval_attribution::split_interval_conversion"],
            env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=125,
        )
    samples = (output / "phases.csv.log").read_text().splitlines()
    assert sum(line.startswith(tuple(f"{sample}," for sample in range(7))) for line in samples) == 35


if __name__ == "__main__":
    main()
