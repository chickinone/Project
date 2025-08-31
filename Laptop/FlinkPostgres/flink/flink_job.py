from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ================== INIT ==================
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10_000)  # checkpoint mỗi 10s

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Bảo hiểm: set pipeline.jars trong code
jars = ";".join([
    "file:///opt/flink/usrlib/flink-sql-connector-kafka-1.17.0.jar",
    "file:///opt/flink/usrlib/flink-connector-jdbc-3.1.1-1.17.jar",
    "file:///opt/flink/usrlib/postgresql-42.6.0.jar",
    "file:///opt/flink/usrlib/flink-json-1.17.0.jar",
])
t_env.get_config().get_configuration().set_string("pipeline.jars", jars)

def exec_sql(sql: str, label: str = ""):
    sql = sql.strip()
    if label:
        print(f"\n---- {label} ----\n{sql}\n")
    return t_env.execute_sql(sql)

def create_kafka_source(table_name: str, topic: str, schema_str: str):
    exec_sql(f"DROP TABLE IF EXISTS {table_name}")
    ddl = f"""
        CREATE TABLE {table_name} (
            {schema_str}
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{topic}',
          'properties.bootstrap.servers' = 'kafka:9092',
          'properties.group.id' = '{table_name}_consumer',
          'scan.startup.mode' = 'earliest-offset',
          'value.format' = 'json',
          'value.json.fail-on-missing-field' = 'false',
          'value.json.ignore-parse-errors' = 'true'
        )
    """
    exec_sql(ddl, f"CREATE KAFKA SOURCE {table_name}")

def create_postgres_sink(table_name: str, schema_str: str):
    exec_sql(f"DROP TABLE IF EXISTS {table_name}_pg")
    ddl = f"""
        CREATE TABLE {table_name}_pg (
            {schema_str}
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:postgresql://flink-postgres:5432/laptopdb',
          'table-name' = '{table_name}',
          'username' = 'admin',
          'password' = 'admin123',
          'driver' = 'org.postgresql.Driver',
          'sink.buffer-flush.max-rows' = '1',
          'sink.buffer-flush.interval' = '1s',
          'sink.max-retries' = '5'
        )
    """
    exec_sql(ddl, f"CREATE JDBC SINK {table_name}_pg")

# ================== SCHEMAS ==================
tables = {
    "dim_date": """
        datekey INT, fulldate DATE, dayofmonth SMALLINT, dayname STRING, isweekday BOOLEAN,
        monthnumber SMALLINT, monthname STRING, quarternumber SMALLINT, yearnumber SMALLINT,
        weekofyear SMALLINT, isholiday BOOLEAN, ismonthend BOOLEAN, ismonthstart BOOLEAN,
        isquarterstart BOOLEAN, isquarterend BOOLEAN, isyearend BOOLEAN, isyearstart BOOLEAN
    """,
    "dim_customer": """
        customerkey INT, customerid STRING, fullname STRING, gender STRING, dateofbirth DATE,
        age INT, phonenumber STRING, email STRING, address STRING, city STRING, province STRING,
        country STRING, customertype STRING, isloyalcustomer BOOLEAN, joindate DATE,
        isactive BOOLEAN, createdat DATE
    """,
    "dim_customer_2": """
        customerkey INT, customerid STRING, fullname STRING, gender STRING, dateofbirth DATE,
        phonenumber STRING, email STRING, address STRING, city STRING, province STRING, country STRING,
        customertype STRING, isloyalcustomer BOOLEAN, joindate DATE, isactive BOOLEAN, version INT,
        effectivefrom DATE, effectiveto DATE, iscurrent BOOLEAN, age INT
    """,
    "dim_brand": """
        brandkey INT, brandid STRING, brandname STRING, country STRING, foundedyear SMALLINT, website STRING
    """,
    "dim_category": """
        categorykey INT, categoryid STRING, categoryname STRING, description STRING
    """,
        "dim_employee": """
        employeekey INT,
        employeeid STRING,
        fullname STRING,
        gender STRING,
        `position` STRING,      -- dùng backtick để escape keyword
        storekey INT,
        hiredate DATE,
        isactive BOOLEAN
    """,
    "dim_store": """
        storekey INT, storeid STRING, storename STRING, city STRING, province STRING,
        country STRING, managername STRING, openeddate DATE
    """,
    "dim_supplier": """
        supplierkey INT, supplierid STRING, suppliername STRING, contactname STRING,
        phonenumber STRING, email STRING, country STRING
    """,
    "dim_payment_method": """
        paymentmethodkey INT, paymentmethodid STRING, paymentmethodname STRING, provider STRING,
        isonline BOOLEAN, isactive BOOLEAN, createdat DATE
    """,
    "dim_product": """
        productkey INT, name STRING, model STRING, brandkey INT, categorykey INT,
        cpu STRING, cores INT, threads INT, cpuspeedghz DECIMAL(10,2), maxspeed STRING,
        gpu STRING, ramvalue INT, ramunit STRING, rammaxvalue INT, rammaxunit STRING,
        ramspeed STRING, ramtype STRING, storagevalue INT, storageunit STRING,
        screensize STRING, screenresolution STRING, screenrefreshrate INT,
        screenrefreshrateunit STRING, screentech STRING, screendescription STRING,
        colorcoverage STRING, soundtech STRING, haskeyboardbacklight BOOLEAN,
        haswebcam BOOLEAN, haswireless BOOLEAN, ports STRING, operatingsystem STRING,
        batteryspec STRING, material STRING, features STRING, productdescription STRING,
        releasedate DATE, isactive BOOLEAN, price DECIMAL(18,2), currentprice DECIMAL(18,2),
        discountrate DECIMAL(19,3), productlink STRING, productimage STRING
    """,
    "fact_sales": """
        salesid INT, datekey INT, customerkey INT, productkey INT, employeekey INT, storekey INT,
        quantity INT, unitprice DECIMAL(10,2), totalamount DECIMAL(10,2), salesdiscount DECIMAL(10,2),
        finalamount DECIMAL(10,2)
    """,
    "fact_order": """
        orderid INT, datekey INT, customerkey INT, employeekey INT, storekey INT, paymentmethodkey INT,
        totalamount DECIMAL(10,2), discounttotal DECIMAL(10,2)
    """,
    "fact_order_detail": """
        orderdetailid INT, orderid INT, productkey INT, quantity INT,
        unitprice DECIMAL(10,2), totalprice DECIMAL(10,2), orderdiscount DECIMAL(10,2), finalprice DECIMAL(10,2)
    """,
    "fact_inventory": """
        inventoryid INT, datekey INT, productkey INT, storekey INT,
        quantityinstock INT, reorderlevel INT
    """,
    "fact_return": """
        returnid INT, datekey INT, customerkey INT, productkey INT,
        quantity INT, returnreason STRING, refundamount DECIMAL(10,2)
    """,
    "fact_web_traffic": """
        trafficid INT, datekey INT, customerkey INT, productkey INT,
        actiontype STRING, devicetype STRING, browser STRING, sessionid STRING,
        timespentseconds INT, referrer STRING, ip STRING
    """
}

# ================== BUILD & RUN ==================
ok_tables = []
for name, schema in tables.items():
    try:
        create_kafka_source(name, f"{name}_topic", schema)
        create_postgres_sink(name, schema)
        ok_tables.append(name)
    except Exception as e:
        print(f"❌ Failed for {name}: {e}")

print("📋 All registered tables:", t_env.list_tables())
print("✅ Will insert for:", ok_tables)

stmt = t_env.create_statement_set()
for name in ok_tables:
    stmt.add_insert_sql(f"INSERT INTO {name}_pg SELECT * FROM {name}")

stmt.execute()
