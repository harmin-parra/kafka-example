package cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

public class CassandraUtils {

    // Wait until the port is accepting connections
    public static void waitForServer(String host, int port, Duration timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        long end = start + timeout.toMillis();

        while (System.currentTimeMillis() < end) {
            try (Socket socket = new Socket(host, port)) {
                System.out.println("Port " + port + " is open on " + host);
                return;
            } catch (Exception e) {
                System.out.println("Waiting for port " + port + " on " + host + "...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        throw new RuntimeException("Timeout waiting for port " + port + " on " + host);
    }

    // Try to open a CqlSession to verify the CQL service is ready
    public static void waitForCql(String host, int port, String datacenter) throws InterruptedException {
        long start = System.currentTimeMillis();
        long timeoutMillis = 300_000; // 5 min

        while (System.currentTimeMillis() - start < timeoutMillis) {
            try (CqlSession session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host, port))
                    .withLocalDatacenter(datacenter)
                    .build()) {
                System.out.println("CQL session established, Cassandra is ready!");
                return;
            } catch (Exception e) {
                System.out.println("Waiting for Cassandra CQL to be ready...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }

        throw new RuntimeException("Timeout waiting for Cassandra CQL service on " + host + ":" + port);
    }

    public static void schemaLoader(String host, int port, String file) {
        // Read file content 
        String cql = null;
        try (InputStream in = CassandraUtils.class.getClassLoader().getResourceAsStream(file)) {
            cql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error reading file: " + file, e);
        }
        /*
        try {
            cql = Files.readString(Paths.get("src", "test", "resources", file));
        } catch (IOException e) {
            throw new java.lang.RuntimeException("Error reading file: " + file);
        }
        */

        // Split statements on semicolon
        String[] statements = cql.split("(?m);\\s*");

        // Connect to Cassandra
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter("datacenter1")
                .build()) {

            for (String raw : statements) {
                String stmt = raw.trim();
                if (!stmt.isEmpty()) {
                    System.out.println("Executing: " + stmt);
                    session.execute(SimpleStatement.newInstance(stmt));
                }
            }
        }
    }
}