from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json
import psycopg2

JAR_PATH = "file:///opt/flink/usrlib/flink-connector-kafka-1.17.0.jar"
env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(JAR_PATH)
env.set_parallelism(1)

kafka_props = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flink-consumer'
}

consumer = FlinkKafkaConsumer(
    topics='laptop_raw_data',
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

ds = env.add_source(consumer).map(lambda x: x, output_type=Types.STRING())

def write_to_postgres(json_str):
    try:
        data = json.loads(json_str)
        conn = psycopg2.connect(
            host="postgres",
            database="laptopdb",
            user="postgres",
            password="Truongdz2004@"
        )
        cur = conn.cursor()
        insert_query = """
        INSERT INTO laptops (
            battery, color_coverage, cores, cpu, cpu_speed_unit, cpu_speed_value,
            current_price, description, discount, features, gpu,
            hard_drive_unit, hard_drive_value, image, keyboard_backlight, link,
            material, max_speed, model, name, os, ports, price,
            ram_max_unit, ram_max_value, ram_speed, ram_unit, ram_value,
            refresh_rate_unit, refresh_rate_value, release_time, resolution,
            review, screen_tech, screen_unit, screen_value, size, sound_tech,
            threads, webcams, wireless
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            data.get('battery'),
            data.get('color_coverage'),
            data.get('cores'),
            data.get('cpu'),
            data.get('cpu_speed_unit'),
            data.get('cpu_speed_value'),
            data.get('current_price'),
            data.get('description'),
            data.get('discount'),
            data.get('features'),
            data.get('gpu'),
            data.get('hard_drive_unit'),
            data.get('hard_drive_value'),
            data.get('image'),
            data.get('keyboard_backlight'),
            data.get('link'),
            data.get('material'),
            data.get('max_speed'),
            data.get('model'),
            data.get('name'),
            data.get('os'),
            data.get('ports'),
            data.get('price'),
            data.get('ram_max_unit'),
            data.get('ram_max_value'),
            data.get('ram_speed'),
            data.get('ram_unit'),
            data.get('ram_value'),
            data.get('refresh_rate_unit'),
            data.get('refresh_rate_value'),
            data.get('release_time'),
            data.get('resolution'),
            data.get('review'),
            data.get('screen_tech'),
            data.get('screen_unit'),
            data.get('screen_value'),
            data.get('size'),
            data.get('sound_tech'),
            data.get('threads'),
            data.get('webcams'),
            data.get('wireless')
        ))
        conn.commit()
        cur.close()
        conn.close()
        print("[✔] Inserted 1 row")
    except Exception as e:
        print(f"[✘] PostgreSQL Insert Failed: {e}")

def process_and_write(value: str):
    write_to_postgres(value)
    return value

ds.map(process_and_write).add_sink(lambda x: None)

env.execute("KafkaToPostgres_FlinkJob")
