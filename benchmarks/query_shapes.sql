-- The mixed-column workload is schematic. These generic names preserve its
-- projection width and data-type mix without publishing the private schema.
SELECT
    record_id,
    group_id,
    event_id,
    source_name,
    entity_id,
    entity_number,
    entity_role,
    measure_x,
    measure_y,
    measure_z,
    CAST(event_time AS TIMESTAMP) AS event_time,
    event_year,
    event_month,
    event_day,
    ingestion_year,
    ingestion_month,
    ingestion_day,
    entity_processed_year,
    entity_processed_month,
    entity_processed_day,
    record_processed_year,
    record_processed_month,
    record_processed_day,
    resolution_note,
    resolved_key,
    verified_path,
    tier,
    category
FROM representative_events;

SELECT
    reference_key,
    entity_id,
    entity_name,
    parent_entity_id,
    parent_entity_name,
    status_code,
    event_type,
    description,
    processed_year,
    processed_month,
    processed_day,
    official_date,
    event_date,
    tier
FROM representative_reference;

-- The public and generated workloads use these exact projections after their
-- Delta tables are registered under the aliases below.
SELECT text, language, url, timestamp, source, question_score
FROM stackexchange;

SELECT id FROM deletion_vector_stress LIMIT 1;

SELECT id FROM deletion_vector_stress;
