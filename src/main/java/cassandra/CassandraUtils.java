package cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.InetSocketAddress;

public class CassandraUtils {

    public static void schemaLoader(String host, int port, String file) {

        String cql = null;
        try {
            cql = Files.readString(Paths.get(file));
        } catch (IOException e) {
            throw new java.lang.RuntimeException("Error reading file: " + file);
        }

        // --- 2) Split statements on semicolon ---
        String[] statements = cql.split("(?m);\\s*");

        // --- 3) Connect to Cassandra ---
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