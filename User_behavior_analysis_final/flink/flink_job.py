from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

def main():
    config = Configuration()
    config.set_string("pipeline.jars",
        "file:///opt/flink/jars/flink-connector-kafka-1.17.0.jar;"
        "file:///opt/flink/jars/kafka-clients-3.3.1.jar;"
        "file:///opt/flink/jars/slf4j-api-1.7.30.jar;"
        "file:///opt/flink/jars/flink-streaming-java-1.17.0.jar;"
        "file:///opt/flink/jars/flink-connector-base-1.17.0.jar;"
        "file:///opt/flink/jars/commons-logging-1.2.jar;"
        "file:///opt/flink/jars/log4j-api-2.17.1.jar"
    )

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_parallelism(1)

    props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        'user_behavior',
        SimpleStringSchema(),
        props
    )

    stream = env.add_source(consumer)
    stream = stream.map(lambda x: x, output_type=Types.STRING())
    stream.print()

    print("✅ Flink job started, ready to execute...")

    try:
        env.execute("Kafka to Console Output")
    except Exception as e:
        import traceback
        print("❌ Flink job failed with exception:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
