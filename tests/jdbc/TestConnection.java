import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Basic JDBC connection test for VibeSQL
 *
 * Compile: javac -cp postgresql-42.7.1.jar TestConnection.java
 * Run: java -cp .:postgresql-42.7.1.jar TestConnection
 */
public class TestConnection {
    private static final String URL = "jdbc:postgresql://localhost:5432/testdb";
    private static final String USER = "testuser";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        System.out.println("=== JDBC Connection Test for VibeSQL ===\n");

        // Test 1: Driver loading
        System.out.println("Test 1: Loading PostgreSQL JDBC driver...");
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("✓ Driver loaded successfully\n");
        } catch (ClassNotFoundException e) {
            System.err.println("✗ PostgreSQL JDBC driver not found");
            System.err.println("  Download from: https://jdbc.postgresql.org/download/");
            System.exit(1);
        }

        // Test 2: Connection
        System.out.println("Test 2: Connecting to VibeSQL server...");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("✓ Connection successful\n");
        } catch (Exception e) {
            System.err.println("✗ Connection failed: " + e.getMessage());
            System.err.println("  Make sure vibesql-server is running on localhost:5432");
            System.exit(1);
        }

        // Test 3: Simple query
        System.out.println("Test 3: Executing simple query...");
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1 as test");
            if (rs.next()) {
                int result = rs.getInt("test");
                if (result == 1) {
                    System.out.println("✓ Simple query successful\n");
                } else {
                    System.err.println("✗ Unexpected result: " + result);
                    System.exit(1);
                }
            } else {
                System.err.println("✗ No results returned");
                System.exit(1);
            }
            rs.close();
        } catch (Exception e) {
            System.err.println("✗ Query failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        // Test 4: Connection metadata
        System.out.println("Test 4: Checking connection metadata...");
        try {
            java.sql.DatabaseMetaData meta = conn.getMetaData();
            System.out.println("  Database: " + meta.getDatabaseProductName());
            System.out.println("  Driver: " + meta.getDriverName() + " " + meta.getDriverVersion());
            System.out.println("  JDBC Version: " + meta.getJDBCMajorVersion() + "." + meta.getJDBCMinorVersion());
            System.out.println("✓ Metadata retrieved successfully\n");
        } catch (Exception e) {
            System.err.println("✗ Failed to get metadata: " + e.getMessage());
        }

        // Cleanup
        try {
            conn.close();
            System.out.println("✓ Connection closed\n");
        } catch (Exception e) {
            System.err.println("Warning: Failed to close connection: " + e.getMessage());
        }

        System.out.println("=== All JDBC connection tests passed ===");
    }
}
