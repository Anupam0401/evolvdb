package io.github.anupam.evolvdb.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.common.TransactionContext;
import io.github.anupam.evolvdb.common.TxnScope;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.PhysicalPlanner;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.planner.analyzer.Analyzer;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.sql.ast.AstNode;
import io.github.anupam.evolvdb.sql.ast.CreateTable;
import io.github.anupam.evolvdb.sql.ast.DropTable;
import io.github.anupam.evolvdb.sql.ast.Statement;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.sql.validate.AstValidator;
import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/**
 * Database is the facade and composition root. Wires all layers from disk I/O through SQL
 * execution, providing a single {@link #execute(String)} entry point for SQL statements.
 */
public final class Database implements Closeable {
    private final DbConfig config;
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final CatalogManager catalogManager;
    private final AtomicLong txnIdGenerator = new AtomicLong(1);

    private final SqlParser parser = new SqlParser();
    private final AstValidator validator = new AstValidator();
    private final Analyzer analyzer = new Analyzer();
    private final PhysicalPlanner planner = new PhysicalPlanner();

    public Database(DbConfig config) throws IOException {
        this.config = config;
        this.diskManager = new NioDiskManager(config);
        this.bufferPool = new DefaultBufferPool(config, diskManager);
        this.catalogManager = new CatalogManager(diskManager, bufferPool, new SlottedPageFormat());
    }

    /**
     * Executes a single SQL statement and returns the result. Supports: CREATE TABLE, DROP TABLE,
     * INSERT, SELECT, UPDATE, DELETE. Each statement runs within a ScopedValue-bound
     * TransactionContext (foundation for M17).
     */
    public QueryResult execute(String sql) throws Exception {
        AstNode node = parser.parse(sql);
        Statement stmt = (Statement) node;

        if (stmt instanceof CreateTable ct) {
            return executeDdlCreate(ct);
        }
        if (stmt instanceof DropTable dt) {
            return executeDdlDrop(dt);
        }

        TransactionContext txnCtx =
            new TransactionContext(
                txnIdGenerator.getAndIncrement(),
                TransactionContext.IsolationLevel.READ_COMMITTED);

        return ScopedValue.where(TxnScope.CURRENT, txnCtx).call(() -> executeDml(stmt));
    }

    private QueryResult executeDml(Statement stmt) throws Exception {
        validator.validate(stmt, catalogManager);
        LogicalPlan logical = analyzer.analyze(stmt, catalogManager, List.of());
        ExecContext ctx = new ExecContext(catalogManager);
        PhysicalOperator op = planner.plan(logical, ctx);

        op.open();
        List<Tuple> rows = new ArrayList<>();
        for (Tuple t = op.next(); t != null; t = op.next()) {
            rows.add(t);
        }
        op.close();

        return QueryResult.ofRows(op.schema(), rows);
    }

    private QueryResult executeDdlCreate(CreateTable ct) throws IOException {
        List<ColumnMeta> cols = new ArrayList<>();
        for (var def : ct.columns()) {
            cols.add(new ColumnMeta(def.name(), def.type(), def.length()));
        }
        Schema schema = new Schema(cols);
        catalogManager.createTable(ct.tableName(), schema);
        return QueryResult.ofMessage("Table '" + ct.tableName() + "' created.");
    }

    private QueryResult executeDdlDrop(DropTable dt) throws IOException {
        var meta =
            catalogManager
                .getTable(dt.tableName())
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Unknown table: " + dt.tableName()));
        catalogManager.dropTable(meta.id());
        return QueryResult.ofMessage("Table '" + dt.tableName() + "' dropped.");
    }

    public DbConfig config() {
        return config;
    }

    public DiskManager disk() {
        return diskManager;
    }

    public BufferPool buffer() {
        return bufferPool;
    }

    public CatalogManager catalog() {
        return catalogManager;
    }

    @Override
    public void close() throws IOException {
        bufferPool.close();
        diskManager.close();
    }
}
