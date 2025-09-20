package kafka;

import org.apache.kafka.clients.admin.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {

    private static final String BOOTSTRAP_SERVERS =
      System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

    public static void waitForServer() throws InterruptedException {
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

    public static void createTopic(String topicName, int partitions, short replicationFactor) {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {

            // Create new topic
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.printf("Topic %s created with %d partitions and replication factor %d.%n",
                    topicName, partitions, replicationFactor);

        } catch (ExecutionException e) {
            System.err.println("Error while creating topic: " + e.getCause().getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void resetTopic(String topicName, int partitions, int replicationFactor) {
        resetTopic(topicName, partitions, (short) replicationFactor);
    }

    public static void resetTopic(String topicName, int partitions, short replicationFactor) {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            // Check if topic exists
            Set<String> existingTopics = admin.listTopics().names().get();

            if (existingTopics.contains(topicName)) {
                System.out.println("Topic " + topicName + " already exists. Deleting...");
                admin.deleteTopics(Collections.singletonList(topicName)).all().get();

                // Wait until topic is fully deleted
                while (admin.listTopics().names().get().contains(topicName)) {
                    System.out.println("Waiting for topic deletion...");
                    Thread.sleep(500);
                }

                System.out.println("Topic " + topicName + " deleted.");
            }

            // Create new topic
            createTopic(topicName, partitions, replicationFactor);

        } catch (ExecutionException e) {
            System.err.println("Error while resetting topic: " + e.getCause().getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
