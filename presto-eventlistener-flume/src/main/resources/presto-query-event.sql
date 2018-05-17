-- Hive SQL
-- use hive catalog alias name , eg.
create schema runtime;
use runtime;
CREATE TABLE queries (
    query_id STRING,
    transaction_id STRING,
    user STRING,
    principal STRING,
    source STRING,
    server_version STRING,
    environment STRING,
    catalog STRING,
    schema STRING,
    queried_columns_by_table MAP<STRING, ARRAY<STRING>>,
    remote_client_address STRING,
    user_agent STRING,
    query_state STRING,
    uri STRING,
    query STRING,
    create_time_ms BIGINT,
    execution_start_time_ms BIGINT,
    end_time_ms BIGINT,
    queued_time_ms BIGINT,
    query_wall_time_ms BIGINT,
    cumulative_memory_byte_second DOUBLE,
    peak_memory_bytes BIGINT,
    cpu_time_ms BIGINT,
    analysis_time_ms BIGINT,
    distributed_planning_time_ms BIGINT,
    total_bytes BIGINT,
    query_stages MAP<INT, MAP<STRING,STRING>>,
    operator_summaries ARRAY<MAP<STRING, STRING>>,
    total_rows BIGINT,
    splits INT,
    error_code_id INT,
    error_code_name STRING,
    failure_type STRING,
    failure_message STRING,
    failure_task STRING,
    failure_host STRING,
    failures_json MAP<STRING, STRING>
)
PARTITIONED BY (y STRING, m STRING, d STRING, h STRING)
CLUSTERED BY (query_id) INTO 32 BUCKETS
STORED AS ORC;

--PARTITIONED BY (y int, m int, d int, h int)


CREATE EXTERNAL TABLE queries_detail (
    query_id STRING,
    transaction_id STRING,
    user STRING,
    principal STRING,
    source STRING,
    server_version STRING,
    environment STRING,
    catalog STRING,
    schema STRING,
    queried_columns_by_table STRING,
    remote_client_address STRING,
    user_agent STRING,
    query_state STRING,
    uri STRING,
    query STRING,
    create_time_ms BIGINT,
    execution_start_time_ms BIGINT,
    end_time_ms BIGINT,
    queued_time_ms BIGINT,
    query_wall_time_ms BIGINT,
    cumulative_memory_byte_second DOUBLE,
    peak_memory_bytes BIGINT,
    cpu_time_ms BIGINT,
    analysis_time_ms BIGINT,
    distributed_planning_time_ms BIGINT,
    total_bytes BIGINT,
    query_stages STRING,
    operator_summaries STRING,
    total_rows BIGINT,
    splits INT,
    error_code_id INT,
    error_code_name STRING,
    failure_type STRING,
    failure_message STRING,
    failure_task STRING,
    failure_host STRING,
    failures_json STRING
)
PARTITIONED BY (y INT, m INT, d INT, h INT)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
LOCATION '/tmp/flume/presto-query-events'
create schema presto;
use presto;
CREATE EXTERNAL TABLE queries_detail (
    server_version STRING,
    environment STRING,
    -- #_client _info
    client_info STRING,
    -- #_session
    query_id STRING,
    transaction_id STRING,
    query_state STRING,
    user STRING,
    principal STRING,
    source STRING,
    remote_client_address STRING,
    user_agent STRING,
    create_timestamp BIGINT,
    end_timestamp BIGINT,
    -- # _data _source
    catalog STRING,
    schema STRING,
    queried_columns_by_table STRING,
    elapsed_time_ms BIGINT,
    queued_time_ms BIGINT,
    -- _resource _group
    uri STRING,
    query STRING,
    -- # _resource _utilization _summary
    cpu_time_ms BIGINT,
    scheduled_time_ms BIGINT,
    blocked_time_ms BIGINT,
    input_rows BIGINT,
    input_bytes BIGINT,
    raw_input_rows BIGINT,
    raw_input_bytes BIGINT,
    peak_memory_bytes BIGINT,
    -- _memory _pool
    cumulative_memory_byte_second DOUBLE,
    output_rows BIGINT,
    output_bytes BIGINT,
    written_rows BIGINT,
    written_bytes BIGINT,
    execution_start_time_ms BIGINT,
    analysis_time_ms BIGINT,
    distributed_planning_time_ms BIGINT,
    query_stages STRING,
    operator_summaries STRING,
    splits INT,
    error_code_id INT,
    error_code_name STRING,
    failure_type STRING,
    failure_message STRING,
    failure_task STRING,
    failure_host STRING,
    failures_json STRING
)
PARTITIONED BY (y INT, m INT, d INT, h INT)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
LOCATION '/tmp/flume/presto-query-events'


alter table queries_detail add partition (y=2017,m=11,d=29,h=16);