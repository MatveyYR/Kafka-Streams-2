package alerter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EarningAlertsApp {
    public static final String PURCHASE_WITH_PRODUCT_TOPIC_NAME = "purchase_with_product-processor";
    public static final String RESULT_TOPIC = "product_earnings_alerts-processor";
    public static final double MAX_EARNINGS_PER_MINUTE = 3000;
    public static final String STATE_STORE_NAME = "state-store";

    public static void main(String[] args) throws InterruptedException {
        var client = new CachedSchemaRegistryClient("http://localhost:8081", 16);
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"
        );

        Topology topology = buildTopology(client, serDeProps);

        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AlerterProcessorAPI");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);

        Topology topology = new Topology();

        topology.addSource(
                "source",
                new StringDeserializer(), new KafkaAvroDeserializer(client, serDeConfig),
                PURCHASE_WITH_PRODUCT_TOPIC_NAME
        );

        topology.addProcessor(
                "alerts-transformer", 
                EarningAlertTrasformer::new,
                "source"
        );

        var stateStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(
                        STATE_STORE_NAME
                ),
                Serdes.ByteArray(), new Serdes.DoubleSerde()
        );

        topology.addStateStore(stateStoreSupplier, "alerts-transformer");

        topology.addSink(
                "sink",
                RESULT_TOPIC,
                new StringSerializer(), new KafkaAvroSerializer(client, serDeConfig),
                "alerts-transformer" 
        );

        return topology;
    }
}
