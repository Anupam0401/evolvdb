package io.github.anupam.evolvdb.optimizer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CostModelTest {
    @Test
    void default_costs_ordering() {
        DefaultCostModel m = new DefaultCostModel();
        Schema s = new Schema(List.of(new ColumnMeta("id", Type.INT, null)));
        Cost scan = m.costSeqScan("t", s);
        Cost filtered = m.costFilter(scan);
        Cost projected = m.costProject(filtered);

        assertTrue(filtered.total() <= scan.total()); // fewer rows -> lower total
        assertTrue(projected.total() >= filtered.total()); // slight cpu added

        Cost nlj = m.costNestedLoopJoin(scan, scan);
        Cost hj = m.costHashJoin(scan, scan);
        Cost smj = m.costSortMergeJoin(scan, scan);
        assertTrue(hj.total() < smj.total());
        assertTrue(smj.total() < nlj.total());
    }
}
