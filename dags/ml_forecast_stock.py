from __future__ import annotations
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "data226",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_forecast_stock",
    description="Snowflake ML Forecast pipeline for stock prices (last 180 days)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["lab1", "snowflake", "ml", "forecast"],
) as dag:
    
    # Read upstream ETL DAG id from Airflow Variable
    upstream_dag_id = Variable.get("upstream_etl_dag_id", default_var="etl_yfinance_stock")

    # Wait for ETL DAG to complete
    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_etl",
        external_dag_id=upstream_dag_id,  # "etl_yfinance_stock"
        external_task_id="fetch_and_upsert",  
        allowed_states=["success"],
        failed_states=["failed", "skipped"],   
        poke_interval=60,  
        timeout=60 * 60 * 1,  
        mode="reschedule",
        soft_fail=True,  
        execution_delta=None,  
        check_existence=False  
    )

    # Create or refresh the 180-day training view
    create_training_view = SnowflakeOperator(
        task_id="create_training_view",
        snowflake_conn_id="snowflake_default",
        trigger_rule="none_failed_min_one_success",  
        sql=r"""
        USE WAREHOUSE {{ var.value.snowflake_warehouse | default('BEETLE_QUERY_WH') }};
        USE DATABASE {{ var.value.snowflake_database | default('ASH_DB') }};
        USE SCHEMA {{ var.value.snowflake_schema | default('STOCK_SCHEMA') }};

        CREATE OR REPLACE VIEW stock_price_daily_v1 AS
        SELECT
            TO_TIMESTAMP_NTZ("DATE") AS DATE_V1,
            CLOSE,
            SYMBOL
        FROM {{var.value.snowflake_database | default('ASH_DB')}}.{{var.value.snowflake_schema | default('STOCK_SCHEMA')}}.fact_stock_price_daily
        WHERE "DATE" >= DATEADD(DAY, -180, CURRENT_DATE());
        """
    )

    # Create or replace the Snowflake ML Forecast model
    create_or_replace_model = SnowflakeOperator(
        task_id="create_or_replace_model",
        snowflake_conn_id="snowflake_default",
        trigger_rule="none_failed_min_one_success",  
        autocommit=True,  
        sql=r"""
        USE WAREHOUSE {{ var.value.snowflake_warehouse | default('BEETLE_QUERY_WH') }};
        USE DATABASE {{ var.value.snowflake_database | default('ASH_DB') }};
        USE SCHEMA {{ var.value.snowflake_schema | default('STOCK_SCHEMA') }};

        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST stock_price(
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'stock_price_daily_v1'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE_V1',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {'ON_ERROR': 'SKIP'}
        );
        """,
    )

    # Run forecast
    run_forecast = SnowflakeOperator(
        task_id="run_forecast",
        snowflake_conn_id="snowflake_default",
        trigger_rule="none_failed_min_one_success",  
        autocommit=True, 
        sql=r"""
        USE WAREHOUSE {{ var.value.snowflake_warehouse | default('BEETLE_QUERY_WH') }};
        USE DATABASE {{ var.value.snowflake_database | default('ASH_DB') }};
        USE SCHEMA {{ var.value.snowflake_schema | default('STOCK_SCHEMA') }};

        -- Run the model forecast (FIXED: use correct model name)
        CALL stock_price!FORECAST(
            FORECASTING_PERIODS => {{var.value.forecast_periods | default(7)}}
        );
        
        -- Retrieve the most recent forecast results
        CREATE OR REPLACE TABLE forecast_latest AS
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        """,
    )

    # Build and refresh a final union table
    build_union_table = SnowflakeOperator(
        task_id="build_union_table",  
        snowflake_conn_id="snowflake_default",
        trigger_rule="none_failed_min_one_success",  # Run even if upstream skipped
        sql=r"""
        USE WAREHOUSE {{ var.value.snowflake_warehouse | default('BEETLE_QUERY_WH') }};
        USE DATABASE {{ var.value.snowflake_database | default('ASH_DB') }};
        USE SCHEMA {{ var.value.snowflake_schema | default('STOCK_SCHEMA') }};
        
        CREATE OR REPLACE TABLE union_stock_price AS
        SELECT 
            SYMBOL,
            CAST("DATE" AS DATE) AS DATE,
            CLOSE AS actual,
            NULL::FLOAT AS forecast,
            NULL::FLOAT AS lower_bound,
            NULL::FLOAT AS upper_bound
        FROM {{ var.value.snowflake_database | default('ASH_DB') }}.{{ var.value.snowflake_schema | default('STOCK_SCHEMA') }}.fact_stock_price_daily
        WHERE "DATE" >= DATEADD(DAY, -180, CURRENT_DATE())
        UNION ALL
        SELECT
            REPLACE(series,'\"','') AS SYMBOL,
            CAST(ts AS DATE) AS DATE,
            NULL::FLOAT AS actual,  
            forecast,
            lower_bound,
            upper_bound
        FROM forecast_latest;
        """,
    )

    wait_for_etl >> create_training_view >> create_or_replace_model >> run_forecast >> build_union_table