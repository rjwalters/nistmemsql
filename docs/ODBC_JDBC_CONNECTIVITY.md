# ODBC and JDBC Connectivity Guide

This guide explains how to connect to VibeSQL using ODBC and JDBC drivers via PostgreSQL wire protocol compatibility.

## Overview

VibeSQL implements the PostgreSQL wire protocol, which means it's compatible with existing PostgreSQL ODBC and JDBC drivers. No custom drivers are needed - simply use the standard PostgreSQL drivers and point them at your VibeSQL server.

## Prerequisites

- VibeSQL server running (`vibesql-server`) on a network-accessible host
- PostgreSQL ODBC driver (psqlODBC) or PostgreSQL JDBC driver (PgJDBC) installed

## Connection Information

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Host** | `localhost` (or server IP) | VibeSQL server hostname |
| **Port** | `5432` (default) | VibeSQL server port |
| **Database** | Your database name | Database to connect to |
| **Username** | Any valid username | Authentication username |
| **Password** | (optional) | Authentication password |
| **SSL Mode** | `disable` (currently) | SSL/TLS encryption (future) |

## ODBC Connectivity

### Windows

#### 1. Install PostgreSQL ODBC Driver (psqlODBC)

Download and install from: https://www.postgresql.org/ftp/odbc/versions/msi/

Or use the community package:
```powershell
winget install PostgreSQL.psqlODBC
```

#### 2. Configure Data Source (DSN)

**Using ODBC Data Source Administrator:**

1. Open ODBC Data Source Administrator (64-bit)
   - Press `Win+R`, type `odbcad32.exe`
2. Click "Add" (User DSN or System DSN)
3. Select "PostgreSQL Unicode" driver
4. Configure connection:
   - **Data Source**: `VibeSQL`
   - **Database**: `mydb`
   - **Server**: `localhost`
   - **Port**: `5432`
   - **User Name**: `myuser`
   - **Password**: (leave empty for trust auth)
   - **SSL Mode**: `disable`
5. Click "Test" to verify connection
6. Click "Save"

**Using Connection String:**

```
Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=mydb;Uid=myuser;Pwd=;SSLMode=disable;
```

#### 3. Test with Excel

1. Open Excel
2. Go to **Data** → **Get Data** → **From Other Sources** → **From ODBC**
3. Select "VibeSQL" DSN
4. Click "OK"
5. Execute SQL queries or import tables

#### 4. Test with Power BI

1. Open Power BI Desktop
2. Click **Get Data** → **ODBC**
3. Select "VibeSQL" DSN or enter connection string
4. Click "OK"
5. Import data from VibeSQL

### Linux

#### 1. Install PostgreSQL ODBC Driver

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install odbc-postgresql unixodbc
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install postgresql-odbc unixODBC
```

**Arch Linux:**
```bash
sudo pacman -S postgresql-odbc unixodbc
```

#### 2. Configure Driver (`/etc/odbcinst.ini`)

```ini
[PostgreSQL]
Description=PostgreSQL ODBC Driver
Driver=/usr/lib/psqlodbcw.so
Setup=/usr/lib/libodbcpsqlS.so
Driver64=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
Setup64=/usr/lib/x86_64-linux-gnu/odbc/libodbcpsqlS.so
FileUsage=1
```

#### 3. Configure DSN (`~/.odbc.ini` or `/etc/odbc.ini`)

```ini
[VibeSQL]
Description=VibeSQL Database
Driver=PostgreSQL
Server=localhost
Port=5432
Database=mydb
Username=myuser
Password=
SSLMode=disable
```

#### 4. Test Connection

```bash
# Test with isql
isql -v VibeSQL

# Execute query
SQL> SELECT 1;
```

### macOS

#### 1. Install PostgreSQL ODBC Driver

```bash
brew install psqlodbc unixodbc
```

#### 2. Configure Driver (`/usr/local/etc/odbcinst.ini`)

```ini
[PostgreSQL]
Description=PostgreSQL ODBC Driver
Driver=/usr/local/lib/psqlodbcw.so
Setup=/usr/local/lib/libodbcpsqlS.so
FileUsage=1
```

#### 3. Configure DSN (`~/.odbc.ini`)

```ini
[VibeSQL]
Description=VibeSQL Database
Driver=PostgreSQL
Server=localhost
Port=5432
Database=mydb
Username=myuser
Password=
SSLMode=disable
```

#### 4. Test Connection

```bash
isql -v VibeSQL
```

### ODBC Connection String Examples

**Minimal:**
```
Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=mydb;
```

**With authentication:**
```
Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=mydb;Uid=myuser;Pwd=mypassword;
```

**With options:**
```
Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=mydb;Uid=myuser;Pwd=;SSLMode=disable;ReadOnly=0;
```

### Programming Languages (ODBC)

#### Python (pyodbc)

```python
import pyodbc

# Using DSN
conn = pyodbc.connect('DSN=VibeSQL')

# Using connection string
conn = pyodbc.connect(
    'Driver={PostgreSQL Unicode};'
    'Server=localhost;'
    'Port=5432;'
    'Database=mydb;'
    'Uid=myuser;'
    'Pwd=;'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
for row in cursor.fetchall():
    print(row)

conn.close()
```

#### C# (.NET)

```csharp
using System.Data.Odbc;

string connectionString =
    "Driver={PostgreSQL Unicode};" +
    "Server=localhost;" +
    "Port=5432;" +
    "Database=mydb;" +
    "Uid=myuser;" +
    "Pwd=;";

using (OdbcConnection conn = new OdbcConnection(connectionString))
{
    conn.Open();

    using (OdbcCommand cmd = new OdbcCommand("SELECT * FROM users", conn))
    using (OdbcDataReader reader = cmd.ExecuteReader())
    {
        while (reader.Read())
        {
            Console.WriteLine(reader[0]);
        }
    }
}
```

#### C++ (unixODBC)

```cpp
#include <sql.h>
#include <sqlext.h>

SQLHENV env;
SQLHDBC dbc;
SQLHSTMT stmt;

// Allocate handles
SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

// Connect
SQLCHAR conn_str[] = "DSN=VibeSQL";
SQLDriverConnect(dbc, NULL, conn_str, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);

// Execute query
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
SQLExecDirect(stmt, (SQLCHAR*)"SELECT * FROM users", SQL_NTS);

// Fetch results...

// Cleanup
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
SQLDisconnect(dbc);
SQLFreeHandle(SQL_HANDLE_DBC, dbc);
SQLFreeHandle(SQL_HANDLE_ENV, env);
```

## JDBC Connectivity

### 1. Add PostgreSQL JDBC Driver to Your Project

#### Maven

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.1</version>
</dependency>
```

#### Gradle

```gradle
dependencies {
    implementation 'org.postgresql:postgresql:42.7.1'
}
```

#### Manual JAR

Download from: https://jdbc.postgresql.org/download/

### 2. JDBC Connection URL Format

```
jdbc:postgresql://hostname:port/database?options
```

**Examples:**

```java
// Basic connection
String url = "jdbc:postgresql://localhost:5432/mydb";

// With SSL disabled (current)
String url = "jdbc:postgresql://localhost:5432/mydb?sslmode=disable";

// With authentication
String url = "jdbc:postgresql://localhost:5432/mydb";
String user = "myuser";
String password = "";
```

### 3. Java Examples

#### Basic Connection

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class VibesqlExample {
    public static void main(String[] args) throws Exception {
        // Load driver (optional in JDBC 4.0+)
        Class.forName("org.postgresql.Driver");

        // Connect
        String url = "jdbc:postgresql://localhost:5432/mydb";
        Connection conn = DriverManager.getConnection(url, "myuser", "");

        // Execute query
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");

        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }

        // Cleanup
        rs.close();
        stmt.close();
        conn.close();
    }
}
```

#### With PreparedStatement

```java
import java.sql.*;

public class VibesqlPreparedExample {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/mydb";

        try (Connection conn = DriverManager.getConnection(url, "myuser", "")) {
            String sql = "SELECT * FROM users WHERE age > ?";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 18);

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getString("name") + " - " + rs.getInt("age"));
                    }
                }
            }
        }
    }
}
```

#### With Connection Pool (HikariCP)

```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class VibesqlPoolExample {
    private static HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/mydb");
        config.setUsername("myuser");
        config.setPassword("");
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(30000);

        dataSource = new HikariDataSource(config);
    }

    public static void main(String[] args) throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {

            while (rs.next()) {
                System.out.println(rs.getString("name"));
            }
        }
    }
}
```

### 4. Spring Boot Integration

#### application.properties

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/mydb
spring.datasource.username=myuser
spring.datasource.password=
spring.datasource.driver-class-name=org.postgresql.Driver

# Connection pool settings
spring.datasource.hikari.maximum-pool-size=10
spring.datasource.hikari.connection-timeout=30000
```

#### Repository Example

```java
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
    List<User> findByAgeGreaterThan(Integer age);
}
```

### 5. Kotlin Example

```kotlin
import java.sql.DriverManager

fun main() {
    val url = "jdbc:postgresql://localhost:5432/mydb"

    DriverManager.getConnection(url, "myuser", "").use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM users").use { rs ->
                while (rs.next()) {
                    println(rs.getString("name"))
                }
            }
        }
    }
}
```

### 6. Scala Example

```scala
import java.sql.DriverManager

object VibesqlExample extends App {
  val url = "jdbc:postgresql://localhost:5432/mydb"

  val conn = DriverManager.getConnection(url, "myuser", "")
  try {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery("SELECT * FROM users")
      try {
        while (rs.next()) {
          println(rs.getString("name"))
        }
      } finally rs.close()
    } finally stmt.close()
  } finally conn.close()
}
```

## Testing Compatibility

### Test Script Locations

- **ODBC Tests**: `tests/odbc/`
- **JDBC Tests**: `tests/jdbc/`

See README files in each directory for running instructions.

## Known Limitations

### Current Implementation

- ✅ Simple query protocol (Query message)
- ✅ Trust authentication
- ✅ Basic password authentication
- ✅ Row data and result sets
- ❌ Extended query protocol (prepared statements) - Planned
- ❌ SSL/TLS encryption - Planned
- ❌ SCRAM-SHA-256 authentication - Planned
- ❌ COPY protocol - Planned

### Workarounds

**For prepared statements:**
Currently, use simple queries instead of prepared statements. Support for extended query protocol (Parse/Bind/Execute) is planned.

**For SSL:**
Currently, connections are unencrypted. Use SSH tunneling or VPN for secure remote connections until SSL support is added.

```bash
# SSH tunnel example
ssh -L 5432:localhost:5432 user@remote-vibesql-server
```

## Troubleshooting

### Connection Refused

**Problem:** `Connection refused` or `Connection timed out`

**Solutions:**
1. Verify server is running: `lsof -i :5432`
2. Check firewall rules
3. Verify host/port in connection string
4. Ensure server is bound to correct interface (0.0.0.0 for all interfaces)

### Driver Not Found

**Problem:** `Driver not found` or `Can't find driver`

**Solutions:**
1. Verify ODBC/JDBC driver is installed
2. Check driver path in configuration
3. For JDBC: Ensure PostgreSQL JDBC jar is in classpath

### Authentication Failed

**Problem:** Authentication errors

**Solutions:**
1. Check authentication method in server config
2. For trust auth: Username can be anything
3. Verify credentials if using password auth

### SSL Not Supported

**Problem:** `SSL is not supported`

**Solutions:**
1. Add `sslmode=disable` to connection string:
   - ODBC: `SSLMode=disable`
   - JDBC: `?sslmode=disable`

### Query Syntax Errors

**Problem:** SQL syntax not recognized

**Solutions:**
1. VibeSQL implements SQL:1999 standard
2. Some PostgreSQL-specific extensions may not be supported
3. Check VibeSQL documentation for supported SQL features

## Performance Tips

1. **Connection Pooling**: Use connection pools (HikariCP, c3p0) for better performance
2. **Batch Operations**: Use batch inserts/updates when possible
3. **Prepared Statements**: Once extended query protocol is supported, use prepared statements for repeated queries
4. **Indexes**: Create indexes on frequently queried columns
5. **Query Optimization**: Use EXPLAIN to understand query execution plans

## Comparison: ODBC vs JDBC

| Feature | ODBC | JDBC |
|---------|------|------|
| **Platform** | Cross-platform (Windows, Linux, macOS) | Java/JVM only |
| **Languages** | C, C++, C#, Python, VB, etc. | Java, Kotlin, Scala, Groovy, etc. |
| **Performance** | Native code, very fast | JVM overhead, still fast |
| **Tools** | Excel, Power BI, Tableau, Crystal Reports | Spring Boot, Hibernate, JDBC tools |
| **Setup** | Requires driver installation | Maven/Gradle dependency |
| **Connection Pooling** | Application-specific | Built-in (HikariCP, c3p0) |

## Additional Resources

- **VibeSQL Server Documentation**: `crates/vibesql-server/README.md`
- **PostgreSQL Wire Protocol**: https://www.postgresql.org/docs/current/protocol.html
- **psqlODBC Documentation**: https://odbc.postgresql.org/
- **PostgreSQL JDBC Documentation**: https://jdbc.postgresql.org/documentation/

## Support

For issues or questions:
- GitHub Issues: https://github.com/rjwalters/vibesql/issues
- Tag with `odbc` or `jdbc` labels for driver-specific issues

## Future Enhancements

Planned improvements:
1. Extended query protocol (prepared statements)
2. SSL/TLS encryption
3. Advanced authentication (SCRAM-SHA-256)
4. Connection pooling in server
5. COPY protocol for bulk operations
6. Native VibeSQL drivers (if PostgreSQL compatibility proves insufficient)

---

**Last Updated:** 2025-11-19
**VibeSQL Version:** 0.1.0
**Server Version:** vibesql-server 0.1.0
