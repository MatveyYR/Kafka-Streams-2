package alerter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static alerter.EarningAlertsApp.STATE_STORE_NAME;

public class EarningAlertTrasformer implements Processor<String, GenericRecord, String, GenericRecord> {
    public static final Duration ONE_MINUTE = Duration.ofMinutes(1);
    private KeyValueStore<byte[], Double> stateStore;
    private ProcessorContext<String, GenericRecord> context;
    private long lastProcessedWindowEnd = 0;

    @Override
    public void init(ProcessorContext<String, GenericRecord> context) {
        stateStore = context.getStateStore(STATE_STORE_NAME);
        this.context = context;
        context.schedule(ONE_MINUTE, PunctuationType.WALL_CLOCK_TIME, this::sendAlerts);
    }

    @Override
    public void process(Record<String, GenericRecord> record) {
        long timestamp = record.timestamp();
        long nearestMinutesTs = timestamp - timestamp % 60_000;
        String productId = record.value().get("product_id").toString();
        Long quantity = (Long) record.value().get("purchase_quantity");
        Double price = (Double) record.value().get("product_price");
        Double earnings = quantity * price;
        byte[] stateStoreKey = createKey(nearestMinutesTs, productId);
        Double oldVal = stateStore.get(stateStoreKey);
        double newVal = oldVal != null ? oldVal + earnings : earnings;
        stateStore.put(stateStoreKey, newVal);
    }

    @Override
    public void close() {
    }

    private void sendAlerts(long timestamp) {
        long nearestMinutesTs = timestamp - timestamp % 60_000;
        stateStore.range(createKey(lastProcessedWindowEnd, ""), createKey(nearestMinutesTs, ""))
                        .forEachRemaining(keyVal -> {
                            long ts = extractTsFromKey(keyVal.key);
                            String productId = extractProductIdFromKey(keyVal.key);
                            Double earnings = keyVal.value;
                            if (earnings > EarningAlertsApp.MAX_EARNINGS_PER_MINUTE) {
                                Schema schema = SchemaBuilder.record("QuantityAlert").fields()
                                        .name("window_start")
                                        .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                                        .noDefault()
                                        .requiredDouble("earnings")
                                        .endRecord();
                                GenericRecord record = new GenericData.Record(schema);
                                record.put("window_start", ts);
                                record.put("earnings", earnings);
                                context.forward(new Record<>(productId, record, ts));
                            }
                            stateStore.delete(keyVal.key);
                        });
        lastProcessedWindowEnd = nearestMinutesTs;
    }

    private String extractProductIdFromKey(byte[] key) {
        int productIdLength = key.length - Long.BYTES;
        byte[] productIdBytes = new byte[productIdLength];
        System.arraycopy(key, Long.BYTES, productIdBytes, 0, productIdLength);
        return new String(productIdBytes);
    }

    private long extractTsFromKey(byte[] key) {
        return ByteBuffer.wrap(key, 0, Long.BYTES).getLong();
    }

    private byte[] createKey(long nearestMinutesTs, String productId) {
        byte[] key = new byte[Long.BYTES + productId.length()];
        System.arraycopy(
                ByteBuffer.allocate(Long.BYTES).putLong(nearestMinutesTs).array(),
                0,
                key,
                0,
                Long.BYTES
        );
        System.arraycopy(
                productId.getBytes(StandardCharsets.UTF_8),
                0,
                key,
                Long.BYTES,
                productId.getBytes(StandardCharsets.UTF_8).length
        );
        return key;
    }
}
