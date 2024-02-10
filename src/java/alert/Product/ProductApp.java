package alert.Product;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ProductApp {
    public static final String PRODUCT_TOPIC_NAME = "products";
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    public static final String RESULT_TOPIC = "purchase_with_product-processor";
    public static final String DLQ_TOPIC = "purchases_product_dlq-processor";
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
        System.exit(0);
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProcessorAPI");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);
        var purchasesStream = builder.stream(
                PURCHASE_TOPIC_NAME, 
                Consumed.with(new Serdes.StringSerde(), avroSerde) 
        );

        var stateStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATE_STORE_NAME),
                        new Serdes.StringSerde(), avroSerde
                )

                        .withLoggingDisabled();
        builder.addGlobalStore(stateStoreSupplier, PRODUCT_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde),
                new StateUpdateSupplier(STATE_STORE_NAME));

        var purchaseWithProduct = purchasesStream
                .transformValues(ProductJoiner::new);

        purchaseWithdProduct
                .filter((key, val) -> val.success)
                .mapValues(val -> val.result)
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        purchaseWithProduct
                .filter((key, val) -> !val.success)
                .mapValues(val -> val.result)
                .to(DLQ_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }

    public static record JoinResult(boolean success, GenericRecord result){}
}
