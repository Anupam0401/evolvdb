package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.optimizer.stats.impl.InMemoryStatsProvider;
import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JoinReorderingRuleTest {

    private Schema factSchema() {
        return new Schema(List.of(
                new ColumnMeta("f.id", Type.INT, null),
                new ColumnMeta("f.fval", Type.INT, null)
        ));
    }
    private Schema dim1Schema() {
        return new Schema(List.of(
                new ColumnMeta("d1.fid", Type.INT, null),
                new ColumnMeta("d1.d1val", Type.INT, null)
        ));
    }
    private Schema dim2Schema() {
        return new Schema(List.of(
                new ColumnMeta("d2.fid", Type.INT, null),
                new ColumnMeta("d2.d2val", Type.INT, null)
        ));
    }

    @Test
    void greedy_left_deep_starts_with_smallest_table() {
        LogicalPlan fact = new LogicalScan("fact", "f", factSchema());
        LogicalPlan dim1 = new LogicalScan("dim1", "d1", dim1Schema());
        LogicalPlan dim2 = new LogicalScan("dim2", "d2", dim2Schema());

        // Joins: f.id = d1.fid, f.id = d2.fid
        Expr jd1 = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.EQ,
                new ColumnRef(new SourcePos(1,1), "f", "id"), new ColumnRef(new SourcePos(1,1), "d1", "fid"));
        Expr jd2 = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.EQ,
                new ColumnRef(new SourcePos(1,1), "f", "id"), new ColumnRef(new SourcePos(1,1), "d2", "fid"));

        // Build initial order: ((f ⋈ d2) ⋈ d1)
        LogicalJoin j1 = new LogicalJoin(fact, dim2, LogicalJoin.JoinType.INNER, jd2, fact.schema());
        LogicalJoin root = new LogicalJoin(j1, dim1, LogicalJoin.JoinType.INNER, jd1, fact.schema());

        InMemoryStatsProvider stats = new InMemoryStatsProvider()
                .putTable("fact", 1_000_000)
                .putTable("dim1", 1_000)
                .putTable("dim2", 10_000);

        LogicalPlan rewritten = new LogicalRewriter(stats).rewrite(root);
        // Find the top-most join and walk to its left-most scan
        LogicalJoin rj = findJoin(rewritten);
        assertNotNull(rj, "Join should exist after reordering");

        LogicalScan leftmost = leftmostScan(rj);
        assertNotNull(leftmost, "Left-most scan exists");
        assertEquals("dim1", leftmost.tableName(), "Smallest table should be left-most in left-deep tree");
    }

    private static LogicalJoin findJoin(LogicalPlan p) {
        if (p instanceof LogicalJoin j) return j;
        for (LogicalPlan c : p.children()) {
            LogicalJoin r = findJoin(c);
            if (r != null) return r;
        }
        return null;
    }

    private static LogicalScan leftmostScan(LogicalPlan p) {
        if (p instanceof LogicalScan s) return s;
        if (p.children().isEmpty()) return null;
        return leftmostScan(p.children().get(0));
    }
}
