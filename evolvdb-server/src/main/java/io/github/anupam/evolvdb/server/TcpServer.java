package io.github.anupam.evolvdb.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.core.QueryResult;
import io.github.anupam.evolvdb.types.Tuple;

/**
 * TCP server using Virtual Threads (one per connection). Each client sends SQL statements
 * line-by-line and receives results as text. Virtual threads allow scaling to thousands of
 * concurrent connections without OS thread exhaustion.
 */
public final class TcpServer {
    private static final int DEFAULT_PORT = 9742;

    private final Database database;
    private final int port;

    public TcpServer(Database database, int port) {
        this.database = database;
        this.port = port;
    }

    public void start() throws IOException {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
                ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("EvolvDB server listening on port " + port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket client = serverSocket.accept();
                executor.submit(() -> handleClient(client));
            }
        }
    }

    private void handleClient(Socket client) {
        try (client;
                var reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        client.getInputStream(), StandardCharsets.UTF_8));
                var writer =
                        new PrintWriter(client.getOutputStream(), true, StandardCharsets.UTF_8)) {
            writer.println("EvolvDB connected. Send SQL (one statement per line). 'quit' to exit.");
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;
                if (trimmed.equalsIgnoreCase("quit") || trimmed.equalsIgnoreCase("exit")) {
                    writer.println("Goodbye.");
                    break;
                }
                try {
                    QueryResult result = database.execute(trimmed);
                    writeResult(writer, result);
                } catch (Exception e) {
                    writer.println("ERROR: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            // Client disconnected
        }
    }

    private void writeResult(PrintWriter writer, QueryResult result) {
        if (result.message() != null) {
            writer.println(result.message());
            return;
        }
        List<Tuple> rows = result.rows();
        if (rows.isEmpty()) {
            writer.println("(0 rows)");
            return;
        }
        var schema = result.schema();
        StringBuilder header = new StringBuilder();
        for (int i = 0; i < schema.size(); i++) {
            if (i > 0) header.append(" | ");
            header.append(schema.columns().get(i).name());
        }
        writer.println(header);
        writer.println("-".repeat(header.length()));
        for (Tuple row : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < schema.size(); i++) {
                if (i > 0) sb.append(" | ");
                Object v = row.get(i);
                sb.append(v == null ? "NULL" : v);
            }
            writer.println(sb);
        }
        writer.println("(" + rows.size() + " rows)");
    }

    public static void main(String[] args) throws Exception {
        int port = DEFAULT_PORT;
        Path dataDir = Path.of("data");

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port", "-p" -> port = Integer.parseInt(args[++i]);
                case "--data", "-d" -> dataDir = Path.of(args[++i]);
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(1);
                }
            }
        }

        DbConfig config = DbConfig.builder().dataDir(dataDir).build();
        try (Database db = new Database(config)) {
            TcpServer server = new TcpServer(db, port);
            server.start();
        }
    }
}
