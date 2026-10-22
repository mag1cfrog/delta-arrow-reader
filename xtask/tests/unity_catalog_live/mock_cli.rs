//! Native stand-in for DAR and Databricks, compiled once by the offline harness tests.

use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{self, Command},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env::var_os("UC_FIXTURE_DIR").ok_or("missing fixture directory")?);
    let mut args: Vec<String> = env::args().skip(1).collect();
    if env::current_exe()?.file_stem().and_then(|s| s.to_str()) == Some("databricks") {
        fs::write(
            root.join("databricks.args"),
            format!("{}\n", args.join("\n")),
        )?;
        if args != ["--version"] {
            process::exit(97);
        }
        io::stdout().write_all(&fs::read(root.join("databricks.version"))?)?;
        return Ok(());
    }

    let operation = match args.first().map(String::as_str) {
        Some("inspect" | "scan") => args[0].clone(),
        Some("query") => {
            let sql_file = option(&args, "--sql-file")?;
            let tables_file = option(&args, "--tables-file")?;
            let operation = Path::new(sql_file)
                .file_stem()
                .and_then(|name| name.to_str())
                .ok_or("missing SQL file name")?;
            if !matches!(operation, "sql_aggregate" | "sql_join") {
                process::exit(97);
            }
            fs::copy(tables_file, root.join(format!("{operation}.tables.json")))?;
            fs::copy(sql_file, root.join(format!("{operation}.captured.sql")))?;
            operation.to_owned()
        }
        _ => process::exit(97),
    };
    fs::write(
        root.join(format!("{operation}.args")),
        format!("{}\n", args.join("\n")),
    )?;

    if let Some(binary) = env::var_os("DAR_LOCAL_BINARY") {
        // The opt-in local self-check executes real DAR with local table locations.
        if args[0] == "query" {
            let index = args
                .iter()
                .position(|arg| arg == "--tables-file")
                .ok_or("missing tables file")?;
            args[index + 1] = root
                .join(format!("{operation}.local.json"))
                .to_str()
                .ok_or("non-UTF-8 fixture path")?
                .to_owned();
        } else {
            args[1] = env::var("DAR_LOCAL_TABLE")?;
            args.retain(|arg| !arg.starts_with("--databricks-profile="));
        }
        process::exit(
            Command::new(binary)
                .args(args)
                .status()?
                .code()
                .unwrap_or(1),
        );
    }

    io::stdout().write_all(&fs::read(root.join(format!("{operation}.stdout")))?)?;
    io::stderr().write_all(&fs::read(root.join(format!("{operation}.stderr")))?)?;
    let code = if env::var("UC_FIXTURE_FAIL_AT").as_deref() == Ok(&operation) {
        1
    } else {
        env::var("UC_FIXTURE_EXIT")
            .unwrap_or_else(|_| "0".into())
            .parse()?
    };
    process::exit(code);
}

fn option<'a>(args: &'a [String], name: &str) -> Result<&'a str, &'static str> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
        .ok_or("missing option")
}
