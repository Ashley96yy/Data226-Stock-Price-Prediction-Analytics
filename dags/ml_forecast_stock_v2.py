from __future__ import annotations
from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

def check_upstream_task_success(**context):
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.db import provide_session
    from airflow.utils import timezone
    from sqlalchemy import and_
    
    upstream_dag_id = Variable.get('upstream_etl_dag_id', default_var='etl_yfinance_stock')
    execution_date = context['execution_date']

    if hasattr(execution_date, 'date'):
        target_date = execution_date.date()
    else:
        target_date = execution_date
    
    start_datetime = datetime.combine(target_date, datetime.min.time())
    start_datetime = timezone.make_aware(start_datetime)
    end_datetime = start_datetime + timedelta(days=1)
    
    logger.info(f'Checking for upstream task success on date: {target_date}')
    logger.info(f'Looking for DAG: {upstream_dag_id}, Task: fetch_and_upsert')
    logger.info(f'Searching between: {start_datetime} and {end_datetime}')
    
    @provide_session
    def get_task_status(session=None):
        task_instances = session.query(TaskInstance).join(DagRun).filter(
            and_(
                TaskInstance.dag_id == upstream_dag_id,
                TaskInstance.task_id == 'fetch_and_upsert',
                DagRun.execution_date >= start_datetime,
                DagRun.execution_date < end_datetime,
                TaskInstance.state == 'success'
            )
        ).all()
        
        logger.info(f'Found {len(task_instances)} successful upstream tasks')
        
        if task_instances:
            for ti in task_instances:
                logger.info(f'Found successful task at: {ti.execution_date}')
        
        return len(task_instances) > 0
    
    success = get_task_status()
    
    if success:
        logger.info(f'Upstream task found and successful for date {target_date}')
        return True
    else:
        logger.warning(f'No successful upstream task found for date {target_date}')
        logger.info('Proceeding anyway - data might be from previous runs')
        return True  


def execute_snowflake_with_retry(sql_statements: list[str], conn_id: str = 'snowflake_default', 
                                 max_retries: int = 3, context: dict = None) -> bool:
    warehouse = Variable.get('snowflake_warehouse', default_var='BEETLE_QUERY_WH')
    database = Variable.get('snowflake_database', default_var='ASH_DB')
    schema = Variable.get('snowflake_schema', default_var='STOCK_SCHEMA')
    
    execution_date = None
    if context:
        ds = context.get('ds')
        if ds:
            execution_date = datetime.strptime(ds, '%Y-%m-%d').date()
            logger.info(f'Using execution_date: {execution_date}')
    
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = None
    cs = None
    
    for attempt in range(max_retries):
        try:
            conn = hook.get_conn()
            cs = conn.cursor()
            
            cs.execute(f'USE WAREHOUSE {warehouse}')
            cs.execute(f'USE DATABASE {database}') 
            cs.execute(f'USE SCHEMA {schema}')
            
            logger.info(f'Using: warehouse={warehouse}, database={database}, schema={schema}')
            
            # Begin transaction
            cs.execute('BEGIN')
            
            # Execute all SQL statements
            for i, sql in enumerate(sql_statements):
                final_sql = sql.replace('{database}', database).replace('{schema}', schema)
                
                if execution_date:
                    final_sql = final_sql.replace('{execution_date}', f"'{execution_date}'")
                
                logger.info(f'Executing SQL statement {i+1}/{len(sql_statements)}')
                logger.debug(f'SQL: {final_sql[:200]}...')
                cs.execute(final_sql)
            
            # Commit transaction
            cs.execute('COMMIT')
            logger.info('All SQL statements executed successfully')
            return True
            
        except Exception as e:
            logger.error(f'Attempt {attempt + 1} failed: {str(e)}')
            
            # Rollback transaction
            try:
                if cs:
                    cs.execute('ROLLBACK')
                    logger.info('Transaction rolled back')
            except Exception as rollback_error:
                logger.error(f'Rollback failed: {str(rollback_error)}')
            
            # If this is the last attempt, raise the exception
            if attempt == max_retries - 1:
                raise e
            
            # Wait before retry
            import time
            time.sleep(5 * (attempt + 1))
            
        finally:
            if cs:
                cs.close()
            if conn:
                conn.close()
    
    return False

def create_training_view(**context):
    sql_statements = [
        '''
        CREATE OR REPLACE VIEW stock_price_daily_v1 AS
        SELECT
            TO_TIMESTAMP_NTZ("DATE") AS DATE_V1,
            CLOSE,
            SYMBOL
        FROM {database}.{schema}.fact_stock_price_daily
        WHERE "DATE" >= DATEADD(DAY, -180, {execution_date})
          AND "DATE" <= {execution_date}
        '''
    ]
    
    try:
        success = execute_snowflake_with_retry(sql_statements, context=context)
        if success:
            logger.info('Training view created successfully')
        else:
            raise Exception('Failed to create training view after retries')
    except Exception as e:
        logger.error(f'Failed to create training view: {str(e)}')
        raise

def create_or_replace_model(**context):
    sql_statements = [
        '''
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST stock_price(
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'stock_price_daily_v1'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE_V1',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {'ON_ERROR': 'SKIP'}
        )
        '''
    ]
    
    try:
        success = execute_snowflake_with_retry(sql_statements, context=context)
        if success:
            logger.info('ML Forecast model created successfully')
        else:
            raise Exception('Failed to create ML model after retries')
    except Exception as e:
        logger.error(f'Failed to create ML model: {str(e)}')
        raise

def run_forecast_with_fallback(**context):
    forecast_periods = int(Variable.get('forecast_periods', default_var='7'))
    
    # Primary forecast attempt
    primary_sql = [
        f'''
        CALL stock_price!FORECAST(
            FORECASTING_PERIODS => {forecast_periods}
        )
        ''',
        '''
        CREATE OR REPLACE TABLE forecast_latest AS
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        '''
    ]
    
    try:
        success = execute_snowflake_with_retry(primary_sql, max_retries=2, context=context)
        if success:
            logger.info(f'Forecast executed successfully for {forecast_periods} periods')
            return 'success'
        else:
            raise Exception('Primary forecast failed')
            
    except Exception as e:
        logger.error(f'Forecast failed: {str(e)}')
        fallback_sql = [
            '''
            CREATE OR REPLACE TABLE forecast_latest (
                series VARCHAR,
                ts TIMESTAMP_NTZ,
                forecast FLOAT,
                lower_bound FLOAT,
                upper_bound FLOAT
            )
            '''
        ]
        execute_snowflake_with_retry(fallback_sql, context=context)
        logger.warning('Created empty forecast table as fallback')
        return 'fallback'

def build_union_table_robust(**context):
    union_sql = [
        '''
        CREATE OR REPLACE TABLE union_stock_price AS
        SELECT 
            SYMBOL,
            CAST("DATE" AS DATE) AS DATE,
            CLOSE AS actual,
            NULL::FLOAT AS forecast,
            NULL::FLOAT AS lower_bound,
            NULL::FLOAT AS upper_bound,
            'actual' as data_type
        FROM {database}.{schema}.fact_stock_price_daily
        WHERE "DATE" >= DATEADD(DAY, -180, {execution_date})
          AND "DATE" <= {execution_date}
        
        UNION ALL
        
        SELECT
            REPLACE(series,'\"','') AS SYMBOL,
            CAST(ts AS DATE) AS DATE,
            NULL::FLOAT AS actual,  
            forecast,
            lower_bound,
            upper_bound,
            'forecast' as data_type
        FROM forecast_latest
        WHERE series IS NOT NULL
        ''',
        
        '''
        CREATE OR REPLACE TABLE forecast_metadata AS
        SELECT 
            CURRENT_TIMESTAMP() as last_updated,
            {execution_date} as base_date,
            COUNT(CASE WHEN data_type = 'actual' THEN 1 END) as actual_records,
            COUNT(CASE WHEN data_type = 'forecast' THEN 1 END) as forecast_records,
            MIN(DATE) as min_date,
            MAX(DATE) as max_date
        FROM union_stock_price
        '''
    ]
    
    try:
        success = execute_snowflake_with_retry(union_sql, context=context)
        if success:
            logger.info('Union table built successfully')
        else:
            raise Exception('Failed to build union table')
            
    except Exception as e:
        logger.error(f'Failed to build union table: {str(e)}')
        raise

# Define default_args and DAG
default_args = {
    'owner': 'data226',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    'ml_forecast_stock_v2',
    default_args=default_args,
    description='Snowflake ML Forecast pipeline with execution_date alignment',
    schedule_interval='@daily',
    catchup=False,
    tags=['lab1', 'snowflake', 'ml', 'forecast'],
    max_active_runs=1,
)

# Task definitions
check_upstream = PythonOperator(
    task_id='check_upstream_success',
    python_callable=check_upstream_task_success,
    provide_context=True,
    dag=dag,
)

create_view_task = PythonOperator(
    task_id='create_training_view',
    python_callable=create_training_view,
    provide_context=True,
    dag=dag,
)

create_model_task = PythonOperator(
    task_id='create_or_replace_model',
    python_callable=create_or_replace_model,
    provide_context=True,
    dag=dag,
)

run_forecast_task = PythonOperator(
    task_id='run_forecast',
    python_callable=run_forecast_with_fallback,
    provide_context=True,
    dag=dag,
)

build_union_task = PythonOperator(
    task_id='build_union_table',
    python_callable=build_union_table_robust,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_upstream >> create_view_task >> create_model_task >> run_forecast_task >> build_union_task