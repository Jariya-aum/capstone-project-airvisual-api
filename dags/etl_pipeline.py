from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import pendulum
from psycopg2.errors import UniqueViolation
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Asia/Bangkok")

# ตั้งค่า Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 28),  # กำหนดวันเริ่มต้น
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# สร้าง DAG
dag = DAG(
    'etl_airquality',
    default_args=default_args,
    description='ETL Pipeline for Air Quality Data',
    schedule_interval='0 10 * * *',  # รันทุกวันเวลา 10:00 ตามเวลาไทย
    start_date=datetime(2025, 3, 28, tzinfo=local_tz),  # ตั้งค่า Timezone
    catchup=False, # ป้องกันการรันย้อนหลัง (เลือกใช้ตามความเหมาะสม)
    tags=["joke", "lolo", "my_first_dag"],
)

# **1️⃣ EXTRACT: ดึงข้อมูลจาก API**
def extract_data():
    API_KEY = "ca802658-e8b7-4810-91e5-a304faf0f38c"
    city, state, country = 'Bangkok', 'Bangkok', 'Thailand'
    
    url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"
    response = requests.get(url)
    data = response.json()

    if data["status"] == "success":
        with open("/opt/airflow/dags/airquality.json", "w") as f:
            json.dump(data, f)
        print("✅ Extracted data saved to airquality.json อ่านข้อมูล API แล้วบันทึกเป็นไฟล์ airquality.json เรียบร้อย")
    else:
        print("❌ Failed to extract data สกัดข้อมูลไม่สำเร็จ")

# **2️⃣ TRANSFORM: แปลงข้อมูลจาก JSON**
def transform_data():
    try:
        with open("/opt/airflow/dags/airquality.json", "r") as f:
            data = json.load(f)

        pollution = data["data"]["current"]["pollution"]
        weather = data["data"]["current"]["weather"]

        transformed_data = {
            "city": data["data"]["city"],
            "state": data["data"]["state"],
            "country": data["data"]["country"],
            "timestamp": pollution["ts"],
            "aqius": pollution["aqius"],
            "mainus": pollution["mainus"],
            "aqicn": pollution["aqicn"],
            "maincn": pollution["maincn"],
            "temp": weather["tp"],
            "pressure": weather["pr"],
            "humidity": weather["hu"],
            "wind_speed": weather["ws"],
            "wind_direction": weather["wd"],
            "weather_icon": weather["ic"]
        }

        with open("/opt/airflow/dags/airquality_transformed.json", "w") as f:
            json.dump(transformed_data, f)
        
        print("✅ Transformed data saved to airquality_transformed.json")
    
    except Exception as e:
        print(f"❌ Error transforming data: {e}")

def create_airquality_table():
    pg_hook = PostgresHook(
        postgres_conn_id="airvisual_postgres_conn",
        schema="opendata"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS airflow.air_quality (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            state VARCHAR(50),
            country VARCHAR(50),
            timestamp TIMESTAMP,
            aqius INT,
            mainus VARCHAR(10),
            aqicn INT,
            maincn VARCHAR(10),
            temp INT,
            pressure INT,
            humidity INT,
            wind_speed FLOAT,
            wind_direction INT,
            weather_icon VARCHAR(10)
        )
    """
    cursor.execute(sql)
    connection.commit()

# **3️⃣ LOAD: นำข้อมูลเข้า PostgreSQL**
def load_data():
    """โหลดข้อมูลเข้าสู่ PostgreSQL โดยใช้ PostgresHook"""

    # เชื่อมต่อกับ PostgreSQL ผ่าน Airflow Connection
    pg_hook = PostgresHook(
        postgres_conn_id="airvisual_postgres_conn", 
        schema="opendata")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    # อ่านข้อมูลจากไฟล์ JSON ที่ extract มา
    with open("/opt/airflow/dags/airquality_transformed.json", "r") as f:
        data = json.load(f)

    # ดึงข้อมูลจาก JSON
    city = data["city"]
    state = data["state"]
    country = data["country"]
    timestamp = data["timestamp"]
    aqius = data["aqius"]
    mainus = data["mainus"]
    aqicn = data["aqicn"]
    maincn = data["maincn"]
    temp = data["temp"]
    pressure = data["pressure"]
    humidity = data["humidity"]
    wind_speed = data["wind_speed"]
    wind_direction = data["wind_direction"]
    weather_icon = data["weather_icon"]

    # SQL สำหรับ INSERT ข้อมูล
    sql = """
        INSERT INTO airflow.air_quality (
            city, state, country, timestamp, aqius, mainus, aqicn, maincn,
            temp, pressure, humidity, wind_speed, wind_direction, weather_icon
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    values = (
        city, state, country, timestamp, aqius, mainus, aqicn, maincn,
        temp, pressure, humidity, wind_speed, wind_direction, weather_icon
    )

    try:
        cursor.execute(sql, values)
        connection.commit()
        print("✅ Data inserted successfully นำเข้าข้อมูลสำเร็จ")
    except UniqueViolation:  # ✅ ดักจับข้อผิดพลาด Unique Constraint
        connection.rollback()  # ย้อนกลับการเปลี่ยนแปลง
        print("⚠️ Duplicate Entry: Skipping this step พบข้อมูลซ้ำ ทำการข้ามขั้นตอนนี้")
    finally:
        cursor.close()
        connection.close()

# **สร้าง Tasks สำหรับ DAG**
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

create_airquality_table_task = PythonOperator(
        task_id="create_airquality_table",
        python_callable=create_airquality_table,
    )

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# **กำหนดลำดับการทำงาน**
extract_task >> transform_task >> create_airquality_table_task >> load_task
