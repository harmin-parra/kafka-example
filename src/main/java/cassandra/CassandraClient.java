package cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class CassandraClient {

    private static final String CASSANDRA_HOST =
      System.getenv().getOrDefault("CASSANDRA_HOST", "localhost");
    private static final int CASSANDRA_PORT =
      Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_PORT", "9042"));

    public static void insert() {
        // Connect to Cassandra
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, CASSANDRA_PORT))
                .withLocalDatacenter("datacenter1")
                .withKeyspace("directory")
                .build()) {

            // Prepare the INSERT statement once
            String insertQuery = "INSERT INTO user_profile "
                    + "(id, name, age, tags, created_at) "
                    + "VALUES (?, ?, ?, ?, ?)";
            PreparedStatement ps = session.prepare(insertQuery);

            // Create a few sample profiles
            List<UserProfile> profiles = new ArrayList<>();

            profiles.add(new UserProfile(
                    UUID.randomUUID(), "Alice", 29,
                    new HashSet<>(Arrays.asList("premium", "beta")),
                    Instant.now()
            ));

            profiles.add(new UserProfile(
                    UUID.randomUUID(), "Bob", 34,
                    new HashSet<>(Collections.singletonList("standard")),
                    Instant.now()
            ));

            profiles.add(new UserProfile(
                    UUID.randomUUID(), "Charlie", 42,
                    new HashSet<>(Arrays.asList("vip", "tester")),
                    Instant.now()
            ));

            // Build a batch
            BatchStatement batch = BatchStatement.builder(BatchType.LOGGED).build();

            for (UserProfile profile : profiles) {
                BoundStatement bound = ps.bind(
                        profile.getId(),
                        profile.getName(),
                        profile.getAge(),
                        profile.getTags(),
                        profile.getCreatedAt()
                );
                batch = batch.add(bound);
            }

            // Execute the batch
            session.execute(batch);

            System.out.println("Inserted " + profiles.size() + " profiles in a single batch.");
        }
    }


    public static void read() {
        // Connect to the cluster
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, CASSANDRA_PORT))
                .withLocalDatacenter("datacenter1")
                .withKeyspace("directory")
                .build()) {

            // Suppose we want all users older than 25
            String query = "SELECT id, name, age, tags, created_at FROM user_profile";  // WHERE age > ?";
            PreparedStatement ps = session.prepare(query);

            BoundStatement bound = ps.bind();  // (25);
            ResultSet rs = session.execute(bound);

            // Iterate over all returned rows
            for (Row row : rs) {
                UserProfile profile = new UserProfile();
                profile.setId(row.getUuid("id"));
                profile.setName(row.getString("name"));
                profile.setAge(row.getInt("age"));
                Set<String> tags = row.getSet("tags", String.class);
                profile.setTags(tags != null ? tags : new HashSet<>());
                Instant createdAt = row.getInstant("created_at");
                profile.setCreatedAt(createdAt);

                System.out.println("User: " + profile.getName()
                        + " (" + profile.getAge() + " y/o), tags=" + profile.getTags());
            }
        }
    }

    // Main method
    public static void main(String[] args) throws InterruptedException {
        CassandraUtils.waitForServer(CASSANDRA_HOST, CASSANDRA_PORT, Duration.ofMinutes(5));
        CassandraUtils.waitForCql(CASSANDRA_HOST, CASSANDRA_PORT, "datacenter1");

        System.out.println("Creating schema");
        CassandraUtils.schemaLoader(CASSANDRA_HOST, CASSANDRA_PORT, "schema.cql");

        System.out.println("Inserting data");
        CassandraClient.insert();

        System.out.println("Reading data");
        CassandraClient.read();
    }

}

