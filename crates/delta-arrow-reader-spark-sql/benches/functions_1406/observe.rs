// Appended to the generated copy of the #1391 benchmark.

fn selected_rows() -> &'static [usize] {
    static SELECTED: std::sync::OnceLock<Vec<usize>> = std::sync::OnceLock::new();
    SELECTED.get_or_init(|| {
        let (groups, rounds) = match std::env::var("DAR_1406_INPUT").as_deref() {
            Ok("reference") => (16, 6),
            Ok("bitmap-512") => (64, 8),
            Ok("bitmap-2048") => (256, 8),
            _ => panic!("unknown #1406 input profile"),
        };
        (0..rounds)
            .flat_map(|round| (0..groups).map(move |group| round * 4096 + group))
            .collect()
    })
}

fn export_input() -> Result<Value> {
    let batches = input()?;
    let columns = [
        "id", "grp", "i", "p", "d", "s", "s2", "arr", "ts", "b", "theta_b",
    ];
    let schema = batches[0].schema();
    let positions = columns
        .iter()
        .map(|name| schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let schema = schema.project(&positions)?;
    let mut rows = Vec::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            rows.push(
                positions
                    .iter()
                    .map(|&column| encode_value(batch.column(column).as_ref(), index))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
    }
    Ok(json!({"schema":spark_results::capture_schema(schema.fields(), None)?,"rows":rows}))
}

async fn observe_case(context: &SessionContext, sql: &str, configuration: &str) -> Result<Value> {
    let physical = match plan(context, sql, configuration).await {
        Ok(physical) => physical,
        Err(error) => {
            return Ok(json!({"status":"failed","phase":"planning","error":error.to_string()}));
        }
    };
    let schema = spark_results::capture_schema(physical.schema().fields(), None)?;
    let arrow_types = types(&physical.schema());
    let mut guards = Vec::new();
    let physical = guard_sources(physical, &mut guards)?;
    assert!(!guards.is_empty(), "probe must scan the input table");
    let mut stream = execute_stream(physical, context.task_ctx())?;
    let mut rows = Vec::new();
    loop {
        match stream.try_next().await {
            Ok(Some(batch)) => {
                assert_eq!(types(&batch.schema()), arrow_types);
                for index in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| encode_value(column.as_ref(), index))
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
            }
            Ok(None) => break,
            Err(error) => {
                return Ok(
                    json!({"status":"failed","phase":"execution","error":error.to_string()}),
                );
            }
        }
    }
    let consumed = guards
        .iter()
        .map(|guard| guard.metrics().and_then(|m| m.output_rows()).unwrap_or(0))
        .sum::<usize>();
    assert_eq!(
        consumed,
        selected_rows().len(),
        "reference scan must consume the complete input"
    );
    Ok(json!({"status":"ok","result":{"schema":schema,"arrow_types":arrow_types,"rows":rows}}))
}

async fn observe_set(args: &[String]) -> Result<Value> {
    let manifest: Manifest = serde_json::from_slice(&fs::read(&args[1])?)?;
    let contexts = sessions()?;
    let configuration = &args[2];
    let context = contexts.get(configuration);
    for case in manifest.functions {
        let Some(sql) = case.spark_sql else { continue };
        let result = observe_case(context, &sql, configuration).await?;
        println!(
            "{}",
            json!({"id":case.id,"configuration":configuration,"observation":result})
        );
    }
    Ok(json!({"status":"complete"}))
}

async fn bitmap_footprint(args: &[String]) -> Result<Value> {
    let contexts = sessions()?;
    let configuration = &args[2];
    let context = contexts.get(configuration);
    let physical = plan(context, &args[1], configuration).await?;
    let mut stream = execute_stream(physical, context.task_ctx())?;
    let (mut rows, mut values, mut payload) = (0, 0, 0);
    while let Some(batch) = stream.try_next().await? {
        let column = batch.column(batch.num_columns() - 1);
        let array = column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or("bitmap output must be binary")?;
        rows += column.len();
        for value in array.iter().flatten() {
            values += 1;
            payload += value.len();
        }
    }
    Ok(
        json!({"input_rows":selected_rows().len(),"output_rows":rows,"nonnull":values,"payload_bytes":payload}),
    )
}
