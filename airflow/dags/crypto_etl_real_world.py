import pendulum
import pandas as pd
import requests
from airflow.sdk import dag, task, TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

# postgresql pw: Baller248243$!
# Free API: CoinGecko (no key required for demo)
API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false"

@dag(
    dag_id="crypto_etl_real_world",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "crypto", "pandas", "postgres", "advanced"],
    template_searchpath="/home/airflow/dags/sql",
)
def crypto_etl():

    with TaskGroup("extract_and_validate") as extract_group:
        @task()
        def extract() -> list:
            """Extract data from CoinGecko API."""
            response = requests.get(API_URL)
            response.raise_for_status()
            data = response.json()
            print(f"Extracted {len(data)} coins")
            return data

        @task()
        def validate(raw_data: list) -> list:
            """Basic data quality validation."""
            if not raw_data:
                raise ValueError("No data extracted from API")
        
            # Simple check for essential keys
            required_keys = ['id', 'symbol', 'current_price']
            for item in raw_data:
                if not all(k in item for k in required_keys):
                    raise ValueError(f"Missing required keys in data: {item.keys()}")
        
            print(f"Data validation passed for {len(raw_data)} coins")
            return raw_data

        raw = extract()
        validated = validate(raw)

    @task(multiple_outputs=False)
    def transform(raw_data: list) -> list:
        """Transform data using Pandas and add analytics."""
        if not raw_data:
            return []
        
        df = pd.DataFrame(raw_data)
        
        # Select and rename columns
        cols = ['id', 'symbol', 'name', 'current_price', 'market_cap', 'last_updated', 'price_change_percentage_24h']
        df = df[cols] if all(c in df.columns for c in cols) else df
        
        # Clean data
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        if 'symbol' in df.columns:
            df['symbol'] = df['symbol'].str.upper()
        
        # Add a simple calculated column
        df['is_high_value'] = df['market_cap'] > 100_000_000_000
        
        # Add a sentiment/trend indicator based on 24h change
        def get_trend(change):
            if change > 5: return 'Strong Bullish'
            if change > 0: return 'Bullish'
            if change < -5: return 'Strong Bearish'
            if change < 0: return 'Bearish'
            return 'Neutral'
        
        if 'price_change_percentage_24h' in df.columns:
            df['trend'] = df['price_change_percentage_24h'].apply(get_trend)
        
        # Add load timestamp for tracking history (as string for serialization)
        df['load_timestamp'] = pendulum.now('UTC').to_iso8601_string()
        
        # Handle potential NaN values which cause JSON serialization errors in XCom
        # Convert NaN to None for JSON compatibility
        # We also need to handle cases where columns might be objects or numeric
        df = df.where(pd.notnull(df), None)
        
        # Ensure all types are standard Python types (numpy types can sometimes cause issues in XCom)
        # This is a more robust way to ensure JSON serializability
        transformed_records = df.to_dict(orient='records')
        
        # Final safety check: convert everything to standard Python types via a round-trip or explicit cast
        # Some Airflow environments have trouble with numpy types in XCom
        import json
        import numpy as np
        try:
            # Explicitly force conversion to native Python types before return
            clean_records = []
            for record in transformed_records:
                clean_record = {}
                for k, v in record.items():
                    # Handle NaN and None first using pandas.isna which is robust
                    if pd.isna(v):
                        clean_record[k] = None
                    elif isinstance(v, (bool, str, int, float)):
                        clean_record[k] = v
                    elif hasattr(v, 'item'): # numpy types
                        val = v.item()
                        # Check if the extracted item is nan
                        clean_record[k] = None if pd.isna(val) else val
                    else:
                        clean_record[k] = str(v)
                clean_records.append(clean_record)

            print(f"Transformed data for {len(clean_records)} coins - explicit conversion completed")
            
            # Print a small sample to verify types and content
            if clean_records:
                print(f"Sample record keys: {list(clean_records[0].keys())}")
                print(f"Sample record types: {[(k, type(v).__name__) for k, v in clean_records[0].items()]}")
            
            return clean_records
        except Exception as e:
            print(f"Serialization check failed: {e}")
            # Final fallback: stringify everything
            return [{str(k): str(v) for k, v in r.items()} for r in transformed_records]

    @task()
    def load(transformed_data: list, **context):
        """Load data using SCD Type 2 (Stage & Merge) logic."""
        if not transformed_data:
            print("No data to load.")
            return

        pg_hook = PostgresHook(postgres_conn_id='airflow-3-db')
        
        # 1. Create the target table if it doesn't exist
        # We need to render the template manually since we are using TaskFlow/pg_hook directly
        dag = context['dag']
        sql_create = dag.get_template_env().get_template("create_target_table.sql").render()
        pg_hook.run(sql_create)

        # 2. Load data into a staging table
        df = pd.DataFrame(transformed_data)
        df.to_sql('stg_crypto_prices', pg_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)

        # 3. The SCD2 Merge Logic
        now = pendulum.now('UTC').to_iso8601_string()
        max_date = '9999-12-31 23:59:59'

        sql_merge = dag.get_template_env().get_template("merge_scd2.sql").render(
            params={"now": now, "max_date": max_date}
        )
        pg_hook.run(sql_merge)
        
        # Cleanup staging
        pg_hook.run("DROP TABLE IF EXISTS stg_crypto_prices;")
        print(f"Successfully processed {len(df)} records using SCD2 logic.")

    # Pipeline logic
    validated_data = validated
    transformed_data = transform(validated_data)
    load(transformed_data)

crypto_etl()
