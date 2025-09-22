
from __future__ import annotations
from datetime import datetime, timedelta
import json
import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_stock_symbols(raw: str | None) -> list[str]:
    if not raw:
        return ["NVDA", "AAPL"]
    try:
        vals = json.loads(raw)
        return [str(v).strip().upper() for v in vals]
    except Exception:
        return [s.strip().upper() for s in raw.strip("[]").replace('"','').split(",") if s.strip()]

def fetch_and_upsert(**context):
    # 1. Determine data range: 180 days back from execution date
    ds = context["ds"]
    execution_date = datetime.strptime(ds, "%Y-%m-%d").date()
    end_date = execution_date + timedelta(days = 1) # inclusive end date
    start_date = execution_date - timedelta(days = 179)
    
    print(f"[DEBUG] Date range: {start_date} to {end_date}")
    
    # 2. Get stock symbols from Airflow Variable
    symbols = get_stock_symbols(Variable.get("yfinance_symbols", default_var = None))
    print(f"[DEBUG] Stock symbols: {symbols}")

    # 3. Fetch data from yfinance
    frames = []

    for symbol in symbols:
        try:
            print(f"[DEBUG] Fetching data for {symbol}...")
            df = yf.download(symbol, start = start_date.isoformat(), end = end_date.isoformat(), progress = False)
            
            if df.empty:
                print(f"[fetch_and_upsert] No data for symbol: {symbol}")
                continue
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df = df.reset_index()
            
            df = df.rename(columns = {
                "Date": "DATE",
                "Open": "OPEN",
                "Close": "CLOSE", 
                "High": "MAX",
                "Low": "MIN",
                "Volume": "VOLUME",
            })
            
            df["SYMBOL"] = symbol
            df = df[["SYMBOL", "DATE", "OPEN", "CLOSE", "MIN", "MAX", "VOLUME"]]
            
            print(f"[DEBUG] Processed data shape for {symbol}: {df.shape}")
            print(f"[DEBUG] Processed columns for {symbol}: {list(df.columns)}")
            print(f"[DEBUG] Sample data for {symbol}:")
            print(df.head(2))
            
            frames.append(df)
            
        except Exception as e:
            print(f"[fetch_and_upsert] Error fetching data for {symbol}: {e}")
            continue
    
    if not frames:
        print("[fetch_and_upsert] No data fetched for any symbol.")
        return 0

    final_df = pd.concat(frames, ignore_index=True)
    print(f"[DEBUG] Combined data shape: {final_df.shape}")
    print(f"[DEBUG] Combined data columns: {list(final_df.columns)}")
    
    final_df["DATE"] = pd.to_datetime(final_df["DATE"]).dt.date
    
    print(f"[DEBUG] NaN count per column:")
    print(final_df.isnull().sum())
    
    original_rows = len(final_df)
    
    final_df = final_df.dropna(subset=['OPEN', 'CLOSE'], how='any')
    final_df['VOLUME'] = final_df['VOLUME'].fillna(0)
    
    cleaned_rows = len(final_df)
    
    if original_rows > cleaned_rows:
        print(f"[fetch_and_upsert] Removed {original_rows - cleaned_rows} rows with NaN values")
    
    if final_df.empty:
        print("[fetch_and_upsert] No valid data after cleaning NaN values.")
        return 0

    print(f"[DEBUG] Final data shape: {final_df.shape}")
    print(f"[DEBUG] Final data sample:")
    print(final_df.head())

    # 4. Write to Snowflake (Transaction + MERGE)    
    hook = SnowflakeHook(snowflake_conn_id = "snowflake_default")
    conn = hook.get_conn()
    cs = conn.cursor()

    try:
        cs.execute("USE WAREHOUSE BEETLE_QUERY_WH") 
        cs.execute("USE DATABASE ASH_DB")
        cs.execute("USE SCHEMA STOCK_SCHEMA")
        cs.execute("BEGIN")

        cs.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        ver, wh, role, db, sch = cs.fetchone()
        print(f"[ctx] version={ver}, wh={wh}, role={role}, db={db}, schema={sch}")
        
        # Create temporary table 
        cs.execute("""
            CREATE TEMPORARY TABLE tmp_fact_stock_price_daily AS
            SELECT * FROM fact_stock_price_daily WHERE 1=0
        """)

        rows = [
            {
                "SYMBOL": row[0],      
                "DATE": row[1],      
                "OPEN": float(row[2]),  
                "CLOSE": float(row[3]), 
                "MIN": float(row[4]), 
                "MAX": float(row[5]),   
                "VOLUME": int(row[6]) if pd.notna(row[6]) else 0,  
            }
            for row in final_df.itertuples(index=False)
        ]

        print(f"[DEBUG] Prepared {len(rows)} rows for insertion")
        if len(rows) > 0:
            print(f"[DEBUG] First row sample: {rows[0]}")

        if rows:
            cs.executemany("""
                INSERT INTO tmp_fact_stock_price_daily
                (SYMBOL, "DATE", "OPEN", "CLOSE", "MIN", "MAX", VOLUME)
                VALUES (%(SYMBOL)s, %(DATE)s, %(OPEN)s, %(CLOSE)s, %(MIN)s, %(MAX)s, %(VOLUME)s)
            """, rows)

        
        # MERGE operation 
        cs.execute("""
            MERGE INTO fact_stock_price_daily t
            USING tmp_fact_stock_price_daily s
              ON t.SYMBOL = s.SYMBOL AND t."DATE" = s."DATE"
            WHEN MATCHED THEN UPDATE SET
              "OPEN"=s."OPEN", "CLOSE"=s."CLOSE", "MIN"=s."MIN", "MAX"=s."MAX", VOLUME=s.VOLUME
            WHEN NOT MATCHED THEN INSERT
              (SYMBOL, "DATE", "OPEN", "CLOSE", "MIN", "MAX", VOLUME)
            VALUES
              (s.SYMBOL, s."DATE", s."OPEN", s."CLOSE", s."MIN", s."MAX", s.VOLUME)
        """)

        cs.execute("COMMIT")
        print(f"[fetch_and_upsert] Upserted {len(rows)} rows into fact_stock_price_daily.")
        return len(rows)
    
    except Exception as e:
        cs.execute("ROLLBACK")
        print(f"[fetch_and_upsert] Error during database operation: {e}")
        raise
    finally:
        cs.close()
        conn.close()

default_args = {
    "owner": "dingyuyao",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_yfinance_stock",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  
    catchup=False,
    default_args=default_args,
    tags=["lab1", "yfinance", "snowflake", "last180"],
) as dag:

    PythonOperator(
        task_id="fetch_and_upsert",
        python_callable=fetch_and_upsert,
        provide_context=True,
    )