package com.xjtlu.bio.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;

/**
 * Owns one loopback-only mysqld process for opt-in HTTP integration tests.
 * The development database is only a read-only source of empty-table definitions;
 * all CREATE statements and all subsequent application writes use a new datadir.
 * Files deliberately survive shutdown so the test evidence can be inspected.
 */
final class ComparisonIsolatedMysql implements AutoCloseable {

    private static final Path MYSQLD = Path.of("/usr/sbin/mysqld");
    private static final Duration START_TIMEOUT = Duration.ofSeconds(45);
    private static final String SCHEMA = "bio";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");
    private static final Pattern EXTERNAL_STORAGE = Pattern.compile(
            "(?i)\\b(?:DATA\\s+DIRECTORY|INDEX\\s+DIRECTORY|TABLESPACE)\\b|\\bCONNECTION\\s*=");

    private final Path directory;
    private final Path data;
    private final Path socket;
    private final Path pid;
    private final Path log;
    private final Thread shutdownHook;
    private Process process;
    private int port;
    private boolean closed;

    private ComparisonIsolatedMysql(Path directory) {
        this.directory = directory;
        data = directory.resolve("data");
        socket = directory.resolve("server.sock");
        pid = directory.resolve("server.pid");
        log = directory.resolve("server.log");
        shutdownHook = new Thread(this::close, "comparison-isolated-mysql-shutdown");
    }

    static ComparisonIsolatedMysql start(Path root) throws Exception {
        if (!Files.isExecutable(MYSQLD)) {
            throw new IOException("Isolated integration requires an executable /usr/sbin/mysqld");
        }
        Path parent = Files.createDirectories(root.toAbsolutePath().normalize()).toRealPath();
        // CREATE_DIRECTORY must fail for an existing run: never initialize over an existing database.
        Path directory = Files.createDirectory(parent.resolve("mysql")).toRealPath();
        ComparisonIsolatedMysql server = new ComparisonIsolatedMysql(directory);
        Runtime.getRuntime().addShutdownHook(server.shutdownHook);
        try {
            if (server.socket.toString().getBytes(StandardCharsets.UTF_8).length > 107) {
                throw new IOException("Temporary MySQL socket path is too long; use a shorter test root: "
                        + server.socket);
            }
            SchemaSnapshot schema = readDevelopmentSchema();
            Files.writeString(directory.resolve("empty-schema.sql"), schema.dump(), StandardCharsets.UTF_8);
            server.initialize();
            server.launch();
            server.createEmptySchema(schema);
            return server;
        } catch (Exception | Error failure) {
            try {
                server.close();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    String jdbcUrl() {
        return connectionUrl(true);
    }

    String username() {
        return "root";
    }

    String password() {
        return "";
    }

    private String connectionUrl(boolean withSchema) {
        return "jdbc:mysql://127.0.0.1:" + port + "/" + (withSchema ? SCHEMA : "")
                + "?useSSL=false&allowPublicKeyRetrieval=true";
    }

    private Properties isolatedCredentials() {
        Properties credentials = new Properties();
        credentials.setProperty("user", username());
        credentials.setProperty("password", password());
        credentials.setProperty("connectTimeout", "1000");
        credentials.setProperty("socketTimeout", "10000");
        return credentials;
    }

    private void initialize() throws Exception {
        Files.createDirectory(data);
        Files.createDirectory(directory.resolve("tmp"));
        List<String> command = commonArguments();
        command.add("--initialize-insecure");
        command.add("--log-error=" + directory.resolve("initialize.log"));
        Files.writeString(directory.resolve("initialize-command.txt"), String.join("\n", command) + "\n");
        process = new ProcessBuilder(command).directory(directory.toFile()).redirectErrorStream(true)
                .redirectOutput(directory.resolve("initialize-console.log").toFile()).start();
        if (!process.waitFor(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
            throw new IOException("Isolated MySQL initialization timed out; see "
                    + directory.resolve("initialize-console.log"));
        }
        if (process.exitValue() != 0) {
            throw new IOException("Isolated MySQL initialization exited " + process.exitValue()
                    + "; see " + directory.resolve("initialize.log"));
        }
    }

    private List<String> commonArguments() {
        List<String> command = new ArrayList<>(List.of(MYSQLD.toString(), "--no-defaults",
                "--datadir=" + data, "--socket=" + socket, "--pid-file=" + pid,
                "--tmpdir=" + directory.resolve("tmp"), "--mysqlx=OFF", "--skip-log-bin",
                "--innodb-buffer-pool-size=33554432", "--performance-schema=OFF",
                "--general-log=OFF", "--slow-query-log=OFF"));
        if ("root".equals(System.getProperty("user.name"))) {
            // MySQL otherwise refuses to start when a test container itself runs as root.
            command.add("--user=root");
        }
        return command;
    }

    private void launch() throws Exception {
        try (ServerSocket reservation = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            port = reservation.getLocalPort();
        }
        List<String> command = commonArguments();
        command.addAll(List.of("--bind-address=127.0.0.1", "--port=" + port,
                "--log-error=" + log, "--max-connections=30"));
        Files.writeString(directory.resolve("server-command.txt"), String.join("\n", command) + "\n");
        process = new ProcessBuilder(command).directory(directory.toFile()).redirectErrorStream(true)
                .redirectOutput(directory.resolve("server-console.log").toFile()).start();
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IOException("Isolated MySQL exited " + process.exitValue() + "; see " + log);
            }
            try (Connection connection = DriverManager.getConnection(connectionUrl(false), isolatedCredentials())) {
                verifyOwnServer(connection);
                return;
            } catch (SQLException unavailable) {
                // Authentication/readiness errors are retried without printing credential-bearing details.
                TimeUnit.MILLISECONDS.sleep(150L);
            }
        }
        throw new IOException("Isolated MySQL was not ready within 45 seconds; see " + log);
    }

    private void verifyOwnServer(Connection connection) throws Exception {
        try (var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT @@datadir, @@port, @@socket, @@pid_file")) {
            if (!result.next() || !Path.of(result.getString(1)).toRealPath().equals(data.toRealPath())
                    || result.getInt(2) != port
                    || !Path.of(result.getString(3)).toAbsolutePath().normalize().equals(socket)
                    || !Path.of(result.getString(4)).toAbsolutePath().normalize().equals(pid)) {
                throw new IOException("Refusing to use a MySQL server outside this test's isolated datadir");
            }
        }
        if (!Files.isRegularFile(pid) || Long.parseLong(Files.readString(pid).trim()) != process.pid()) {
            throw new IOException("Isolated MySQL PID file does not match the process started by this test");
        }
    }

    private void createEmptySchema(SchemaSnapshot schema) throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionUrl(false), isolatedCredentials())) {
            verifyOwnServer(connection);
            try (var statement = connection.createStatement()) {
                statement.execute(schema.createDatabase());
            }
            connection.setCatalog(SCHEMA);
            // Connector/J binds a Statement to its catalog at construction time.
            try (var statement = connection.createStatement()) {
                statement.execute("SET FOREIGN_KEY_CHECKS=0");
                try {
                    for (String ddl : schema.tableDefinitions()) {
                        statement.execute(ddl);
                    }
                } finally {
                    statement.execute("SET FOREIGN_KEY_CHECKS=1");
                }
                try (var result = statement.executeQuery(
                        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='bio' AND TABLE_TYPE='BASE TABLE'")) {
                    if (!result.next() || result.getInt(1) != schema.tableDefinitions().size()) {
                        throw new IOException("Isolated schema table count does not match the source definitions");
                    }
                }
            }
        }
    }

    private static SchemaSnapshot readDevelopmentSchema() throws Exception {
        Map<?, ?> configuration;
        try (var input = Files.newInputStream(Path.of("src/main/resources/application-dev.yml"))) {
            configuration = new Yaml().load(input);
        }
        if (configuration == null || !(configuration.get("spring") instanceof Map<?, ?> spring)
                || !(spring.get("datasource") instanceof Map<?, ?> datasource)) {
            throw new IOException("Development datasource configuration is missing");
        }
        Object configuredUrl = datasource.get("url");
        if (!(configuredUrl instanceof String url) || !url.startsWith("jdbc:mysql://")) {
            throw new IOException("Development datasource must be a local MySQL URL");
        }
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        if (!List.of("localhost", "127.0.0.1", "[::1]").contains(endpoint.getHost())
                || !"/bio".equals(endpoint.getPath()) || endpoint.getUserInfo() != null) {
            throw new IOException("Only a local development bio schema may supply test table definitions");
        }
        Properties credentials = new Properties();
        Object username = datasource.get("username");
        Object password = datasource.get("password");
        if (!(username instanceof String) || password == null) {
            throw new IOException("Development datasource credentials are missing");
        }
        credentials.setProperty("user", (String) username);
        credentials.setProperty("password", String.valueOf(password));
        credentials.setProperty("connectTimeout", "5000");
        credentials.setProperty("socketTimeout", "10000");
        // Credentials never enter command lines, evidence files, URLs or log messages.
        try (Connection source = DriverManager.getConnection(url, credentials)) {
            source.setReadOnly(true);
            String charset;
            String collation;
            try (var statement = source.prepareStatement(
                    "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?")) {
                statement.setString(1, SCHEMA);
                try (var rows = statement.executeQuery()) {
                    if (!rows.next()) {
                        throw new IOException("Development bio schema was not found");
                    }
                    charset = rows.getString(1);
                    collation = rows.getString(2);
                }
            }
            if (!IDENTIFIER.matcher(charset).matches() || !IDENTIFIER.matcher(collation).matches()) {
                throw new IOException("Unexpected schema character set or collation identifier");
            }
            List<String> tables = new ArrayList<>();
            try (var statement = source.prepareStatement(
                    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")) {
                statement.setString(1, SCHEMA);
                try (var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        tables.add(rows.getString(1));
                    }
                }
            }
            if (tables.isEmpty()) {
                throw new IOException("Development bio schema has no base table definitions to copy");
            }
            List<String> definitions = new ArrayList<>();
            for (String table : tables) {
                try (var statement = source.createStatement();
                        var rows = statement.executeQuery("SHOW CREATE TABLE `bio`." + quote(table))) {
                    if (!rows.next()) {
                        throw new IOException("No CREATE TABLE definition returned for " + table);
                    }
                    String ddl = rows.getString(2);
                    if (ddl == null || !ddl.startsWith("CREATE TABLE ") || EXTERNAL_STORAGE.matcher(ddl).find()) {
                        throw new IOException("Cannot safely copy external-storage or unexpected table definition: " + table);
                    }
                    definitions.add(ddl);
                }
            }
            return new SchemaSnapshot(charset, collation, List.copyOf(definitions));
        } catch (SQLException failure) {
            // Do not attach driver exception text: it may include configured account/URL details.
            throw new IOException("Read-only development schema inspection failed (SQLState="
                    + failure.getSQLState() + ", errorCode=" + failure.getErrorCode() + ")");
        } finally {
            credentials.clear();
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private record SchemaSnapshot(String charset, String collation, List<String> tableDefinitions) {
        private String createDatabase() {
            return "CREATE DATABASE `bio` CHARACTER SET " + charset + " COLLATE " + collation;
        }

        private String dump() {
            return "-- Empty-schema snapshot only; no development business records copied.\n"
                    + createDatabase() + ";\nUSE `bio`;\nSET FOREIGN_KEY_CHECKS=0;\n"
                    + String.join(";\n\n", tableDefinitions) + ";\nSET FOREIGN_KEY_CHECKS=1;\n";
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        boolean interrupted = false;
        try {
            if (process != null && process.isAlive()) {
                process.destroy();
                try {
                    if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Owned test MySQL process did not stop, pid=" + process.pid());
                        }
                    }
                } catch (InterruptedException failure) {
                    interrupted = true;
                    process.destroyForcibly();
                    try {
                        if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Owned test MySQL process did not stop, pid=" + process.pid());
                        }
                    } catch (InterruptedException secondInterruption) {
                        interrupted = true;
                    }
                }
                if (process.isAlive()) {
                    throw new IllegalStateException("Owned test MySQL process is still alive, pid=" + process.pid());
                }
            }
            closed = true;
        } finally {
            if (closed && Thread.currentThread() != shutdownHook) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException shutdownInProgress) {
                    // Shutdown hooks may already be executing; close is idempotent.
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
