from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
from datetime import timedelta

local_tz = pendulum.timezone("Asia/Kolkata")
start_date = pendulum.datetime(2025, 5, 27, 17, 38, 0, tz=local_tz)

# Configuration
AIRBYTE_CONN_ID = 'airbyte_cloud'
AIRBYTE_CONNECTION_ID = '9d58d10f-8e37-4912-8cf7-410c1bae9156'
SNOWFLAKE_CONN_ID = 'snowflake_conn'

with DAG(
    dag_id='complete_sales_data_pipeline',
    start_date=start_date,
    schedule_interval='@daily',  # Every day at 5:40 PM local time
    catchup=True,
    default_args = {
        'owner': 'pooja',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
    },
    tags=['airbyte', 'snowflake', 'elt'],
) as dag:

    # Task 1: Trigger Airbyte data sync
    trigger_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTION_ID,
    )

    # Task 2: Wait for Airbyte sync to complete
    wait_for_sync = AirbyteJobSensor(
        task_id='wait_for_airbyte_sync',
        airbyte_conn_id=AIRBYTE_CONN_ID,
        airbyte_job_id=trigger_sync.output,
    )

    # Task 3: Transform data in Snowflake
    transform_sales_stream = SQLExecuteQueryOperator(
        task_id='transform_sales_stream',
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        -- Region Dimension
        CREATE OR REPLACE TABLE ELT_SALESDATA.PUBLIC.Region AS
        SELECT ROW_NUMBER() OVER (ORDER BY REGION) AS Region_id, REGION
        FROM (
            SELECT DISTINCT REGION
            FROM ELT_SALESDATA.PUBLIC."SALES RECORD"
            WHERE REGION IS NOT NULL
        );

        -- Country Dimension
        CREATE OR REPLACE TABLE ELT_SALESDATA.PUBLIC.Country AS
        SELECT ROW_NUMBER() OVER (ORDER BY COUNTRY) AS Country_id, COUNTRY
        FROM (
            SELECT DISTINCT COUNTRY
            FROM ELT_SALESDATA.PUBLIC."SALES RECORD"
            WHERE COUNTRY IS NOT NULL
        );

        -- Item Dimension
        CREATE OR REPLACE TABLE ELT_SALESDATA.PUBLIC.Item AS
        SELECT ROW_NUMBER() OVER (ORDER BY "ITEM TYPE") AS Item_id, "ITEM TYPE" AS Item_type
        FROM (
            SELECT DISTINCT "ITEM TYPE"
            FROM ELT_SALESDATA.PUBLIC."SALES RECORD"
            WHERE "ITEM TYPE" IS NOT NULL
        );

        -- Order Priority Dimension
        CREATE OR REPLACE TABLE ELT_SALESDATA.PUBLIC.Order_Priority AS
        SELECT ROW_NUMBER() OVER (ORDER BY ORDER_PRIORITY_FULL) AS OrderPriority_id, ORDER_PRIORITY_FULL AS ORDER_PRIORITY
        FROM (
            SELECT DISTINCT
                CASE 
                    WHEN "ORDER PRIORITY" = 'H' THEN 'High'
                    WHEN "ORDER PRIORITY" = 'L' THEN 'Low'
                    WHEN "ORDER PRIORITY" = 'M' THEN 'Medium'
                    WHEN "ORDER PRIORITY" = 'C' THEN 'Cold'
                    ELSE "ORDER PRIORITY"
                END AS ORDER_PRIORITY_FULL
            FROM ELT_SALESDATA.PUBLIC."SALES RECORD"
            WHERE "ORDER PRIORITY" IS NOT NULL
        );

        -- Orders Fact Table
        CREATE OR REPLACE TABLE ELT_SALESDATA.PUBLIC.Orders AS
        SELECT
            s."ORDER ID",
            s."ITEM TYPE",
            s."SHIP DATE",
            s."UNIT COST",
            s."ORDER DATE",
            s."TOTAL COST",
            s."UNIT PRICE",
            s."UNITS SOLD",
            s."TOTAL PROFIT",
            s."SALES CHANNEL",
            s."TOTAL REVENUE",
            r.Region_id,
            c.Country_id,
            i.Item_id,
            op.OrderPriority_id,
            DATEDIFF(
                'day',
                TRY_TO_DATE(s."ORDER DATE", 'MM/DD/YYYY'),
                TRY_TO_DATE(s."SHIP DATE", 'MM/DD/YYYY')
            ) AS SHIPMENT_LEAD_TIME
        FROM (
            SELECT *,
                CASE 
                    WHEN "ORDER PRIORITY" = 'H' THEN 'High'
                    WHEN "ORDER PRIORITY" = 'L' THEN 'Low'
                    WHEN "ORDER PRIORITY" = 'M' THEN 'Medium'
                    WHEN "ORDER PRIORITY" = 'C' THEN 'Cold'
                    ELSE "ORDER PRIORITY"
                END AS ORDER_PRIORITY_FULL
            FROM ELT_SALESDATA.PUBLIC."SALES RECORD"
            WHERE 
                "ORDER ID" IS NOT NULL AND
                "ITEM TYPE" IS NOT NULL AND
                REGION IS NOT NULL AND
                COUNTRY IS NOT NULL AND
                "ORDER PRIORITY" IS NOT NULL
        ) s
        JOIN ELT_SALESDATA.PUBLIC.Region r ON r.Region = s.REGION
        JOIN ELT_SALESDATA.PUBLIC.Country c ON c.Country = s.COUNTRY
        JOIN ELT_SALESDATA.PUBLIC.Item i ON i.Item_type = s."ITEM TYPE"
        JOIN ELT_SALESDATA.PUBLIC.Order_Priority op ON op.ORDER_PRIORITY = s.ORDER_PRIORITY_FULL;
        """
    )

    # Set task dependencies
    trigger_sync >> wait_for_sync >> transform_sales_stream