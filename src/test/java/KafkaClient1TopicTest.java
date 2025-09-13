import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class KafkaClient1TopicTest {

    private static final String TOPIC = System.getenv().getOrDefault("TOPIC", "quickstart");
    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

    @BeforeAll
    public static void waitKafkaServer() throws InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.RETRIES_CONFIG, "3");

        int retries = 10;
        while (retries-- > 0) {
            try (AdminClient admin = AdminClient.create(props)) {
                admin.listTopics().names().get(); // attempt to list topics
                System.out.println("Kafka server is ready!");
                return;
            } catch (Exception e) {
                System.out.println("Kafka server not ready yet, retrying...");
                Thread.sleep(2000);
            }
        }

        throw new RuntimeException("Kafka broker not available after waiting");
    }

    @Test
    public void A_producer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing messages...");
            for (int i = 1; i <= 5; i++) {
                String key = "key-" + i;
                String value = "Hello Kafka " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record);
                System.out.printf("Produced: key=%s, value=%s%n", key, value);
            }
            producer.flush(); // ensure all messages are sent
        }
    }

    @Test
    public void B_consumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "my-consumer-group-" + System.currentTimeMillis());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println("Consuming messages...");
            ConsumerRecords<String, String> records = ConsumerRecords.empty();

            // Poll loop to ensure messages are received
            int retries = 10;
            while (records.isEmpty() && retries-- > 0) {
                records = consumer.poll(Duration.ofSeconds(2));
            }

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed: offset=%d, key=%s, value=%s%n",
                        record.offset(), record.key(), record.value());
            }

            if (!records.isEmpty()) {
                consumer.commitSync();
                System.out.println("Offsets committed manually.");
            } else {
                System.out.println("No messages consumed.");
            }
        }
    }
}
