package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaClient {

    private static final String TOPIC = "quickstart";
    private static final String BOOTSTRAP_SERVERS =
      System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

    // Producer method
    public static void produceMessages() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                String key = "key-" + i;
                String value = "Hello Kafka " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record);
                System.out.printf("Produced: key=%s, value=%s%n", key, value);
            }
        }
    }

    // Consumer method
    public static void consumeMessages() {
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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed: offset=%d, key=%s, value=%s%n",
                        record.offset(), record.key(), record.value());
            }

            // After processing all messages in this batch, commit offsets
            if (!records.isEmpty()) {
                consumer.commitSync();
                System.out.println("Offsets committed manually.");
            }
        }
    }

    public static void consumeLastMessage() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "last-message-reader-" + System.currentTimeMillis());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Manually assign partitions instead of subscribing
            List<TopicPartition> partitions = new ArrayList<>();
            int numPartitions = consumer.partitionsFor(TOPIC).size();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(new TopicPartition(TOPIC, i));
            }
            consumer.assign(partitions);

            // Get the end offset (offset after last message) for each partition
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            // Seek to the last message in each partition
            for (TopicPartition tp : partitions) {
                long lastOffset = endOffsets.get(tp);
                if (lastOffset > 0) {
                    consumer.seek(tp, lastOffset - 1);
                }
            }

            // Poll and print the last messages
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Partition %d: Last message: key=%s, value=%s, offset=%d%n",
                        record.partition(), record.key(), record.value(), record.offset());
            }
        }
    }

    // Main method
    public static void main(String[] args) {
        System.out.println("Consume all messages");
        KafkaTopicUtils.resetTopic("quickstart", 1, 1);
        System.out.println("Producing messages...");
        produceMessages();
        System.out.println("**********************************************");
        System.out.println("Consuming messages...");
        consumeMessages();

        System.out.print("\n\n\n");

        System.out.println("Consume last message");
        KafkaTopicUtils.resetTopic("quickstart", 1, 1);
        System.out.println("Producing messages...");
        produceMessages();
        System.out.println("**********************************************");
        System.out.println("Consuming messages...");
        consumeLastMessage();

    }
}


/*
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaClient {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SimpleKafkaConsumer <bootstrap-servers> <topic>");
            System.exit(1);
        }

        String brokers = args[0];   // e.g., "localhost:9092"
        String topic = args[1];     // e.g., "my-topic"

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "my-simple-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // or "latest"
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Subscribed to topic: %s%n", topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> rec : records) {
                    System.out.printf("Offset=%d, Key=%s, Value=%s%n",
                                      rec.offset(), rec.key(), rec.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
*/
