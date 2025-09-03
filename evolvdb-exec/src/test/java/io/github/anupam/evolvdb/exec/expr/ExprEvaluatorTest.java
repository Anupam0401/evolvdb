package io.github.anupam.evolvdb.exec.expr;

import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ExprEvaluatorTest {
    private ExprEvaluator evaluator;
    private SourcePos pos;
    
    @BeforeEach
    void setup() {
        evaluator = new ExprEvaluator();
        pos = new SourcePos(1, 1);
    }
    
    @Test
    void eval_literal() {
        assertEquals(42, evaluator.eval(new Literal(pos, 42), null, null));
        assertEquals("hello", evaluator.eval(new Literal(pos, "hello"), null, null));
        assertEquals(true, evaluator.eval(new Literal(pos, true), null, null));
        assertEquals(3.14f, evaluator.eval(new Literal(pos, 3.14f), null, null));
    }
    
    @Test
    void eval_column_ref() {
        Schema schema = new Schema(List.of(
            new ColumnMeta("id", Type.INT, null),
            new ColumnMeta("name", Type.STRING, null),
            new ColumnMeta("age", Type.INT, null)
        ));
        Tuple tuple = new Tuple(schema, List.of(1, "Alice", 25));
        
        assertEquals(1, evaluator.eval(new ColumnRef(pos, null, "id"), tuple, schema));
        assertEquals("Alice", evaluator.eval(new ColumnRef(pos, null, "name"), tuple, schema));
        assertEquals(25, evaluator.eval(new ColumnRef(pos, null, "age"), tuple, schema));
    }
    
    @Test
    void eval_arithmetic() {
        // Integer arithmetic
        Expr add = new BinaryExpr(pos, BinaryExpr.Op.ADD, 
                                  new Literal(pos, 10), 
                                  new Literal(pos, 5));
        assertEquals(15, evaluator.eval(add, null, null));
        
        Expr sub = new BinaryExpr(pos, BinaryExpr.Op.SUB,
                                  new Literal(pos, 10),
                                  new Literal(pos, 3));
        assertEquals(7, evaluator.eval(sub, null, null));
        
        Expr mul = new BinaryExpr(pos, BinaryExpr.Op.MUL,
                                  new Literal(pos, 4),
                                  new Literal(pos, 5));
        assertEquals(20, evaluator.eval(mul, null, null));
        
        Expr div = new BinaryExpr(pos, BinaryExpr.Op.DIV,
                                  new Literal(pos, 20),
                                  new Literal(pos, 4));
        assertEquals(5, evaluator.eval(div, null, null));
        
        // Float arithmetic
        Expr floatAdd = new BinaryExpr(pos, BinaryExpr.Op.ADD,
                                       new Literal(pos, 3.5f),
                                       new Literal(pos, 2.5f));
        assertEquals(6.0f, evaluator.eval(floatAdd, null, null));
        
        // Mixed types (int + float = float)
        Expr mixed = new BinaryExpr(pos, BinaryExpr.Op.ADD,
                                   new Literal(pos, 10),
                                   new Literal(pos, 2.5f));
        assertEquals(12.5f, evaluator.eval(mixed, null, null));
    }
    
    @Test
    void eval_string_concat() {
        Expr concat = new BinaryExpr(pos, BinaryExpr.Op.CONCAT,
                                     new Literal(pos, "Hello"),
                                     new Literal(pos, " World"));
        assertEquals("Hello World", evaluator.eval(concat, null, null));
        
        // Concat with numbers
        Expr concatNum = new BinaryExpr(pos, BinaryExpr.Op.CONCAT,
                                        new Literal(pos, "Value: "),
                                        new Literal(pos, 42));
        assertEquals("Value: 42", evaluator.eval(concatNum, null, null));
    }
    
    @Test
    void eval_comparison() {
        // Integer comparisons
        Expr eq = new ComparisonExpr(pos, ComparisonExpr.Op.EQ,
                                     new Literal(pos, 10),
                                     new Literal(pos, 10));
        assertEquals(true, evaluator.eval(eq, null, null));
        
        Expr neq = new ComparisonExpr(pos, ComparisonExpr.Op.NEQ,
                                      new Literal(pos, 10),
                                      new Literal(pos, 5));
        assertEquals(true, evaluator.eval(neq, null, null));
        
        Expr lt = new ComparisonExpr(pos, ComparisonExpr.Op.LT,
                                     new Literal(pos, 5),
                                     new Literal(pos, 10));
        assertEquals(true, evaluator.eval(lt, null, null));
        
        Expr lte = new ComparisonExpr(pos, ComparisonExpr.Op.LTE,
                                      new Literal(pos, 10),
                                      new Literal(pos, 10));
        assertEquals(true, evaluator.eval(lte, null, null));
        
        Expr gt = new ComparisonExpr(pos, ComparisonExpr.Op.GT,
                                     new Literal(pos, 15),
                                     new Literal(pos, 10));
        assertEquals(true, evaluator.eval(gt, null, null));
        
        Expr gte = new ComparisonExpr(pos, ComparisonExpr.Op.GTE,
                                      new Literal(pos, 10),
                                      new Literal(pos, 10));
        assertEquals(true, evaluator.eval(gte, null, null));
        
        // String comparisons
        Expr strEq = new ComparisonExpr(pos, ComparisonExpr.Op.EQ,
                                        new Literal(pos, "abc"),
                                        new Literal(pos, "abc"));
        assertEquals(true, evaluator.eval(strEq, null, null));
        
        Expr strLt = new ComparisonExpr(pos, ComparisonExpr.Op.LT,
                                        new Literal(pos, "abc"),
                                        new Literal(pos, "def"));
        assertEquals(true, evaluator.eval(strLt, null, null));
    }
    
    @Test
    void eval_logical() {
        // AND
        Expr andTrue = new LogicalExpr(pos, LogicalExpr.Op.AND,
                                       new Literal(pos, true),
                                       new Literal(pos, true));
        assertEquals(true, evaluator.eval(andTrue, null, null));
        
        Expr andFalse = new LogicalExpr(pos, LogicalExpr.Op.AND,
                                        new Literal(pos, true),
                                        new Literal(pos, false));
        assertEquals(false, evaluator.eval(andFalse, null, null));
        
        // OR
        Expr orTrue = new LogicalExpr(pos, LogicalExpr.Op.OR,
                                     new Literal(pos, true),
                                     new Literal(pos, false));
        assertEquals(true, evaluator.eval(orTrue, null, null));
        
        Expr orFalse = new LogicalExpr(pos, LogicalExpr.Op.OR,
                                      new Literal(pos, false),
                                      new Literal(pos, false));
        assertEquals(false, evaluator.eval(orFalse, null, null));
        
        // NOT
        Expr notTrue = new LogicalExpr(pos, LogicalExpr.Op.NOT,
                                      new Literal(pos, true),
                                      null);
        assertEquals(false, evaluator.eval(notTrue, null, null));
        
        Expr notFalse = new LogicalExpr(pos, LogicalExpr.Op.NOT,
                                       new Literal(pos, false),
                                       null);
        assertEquals(true, evaluator.eval(notFalse, null, null));
    }
    
    @Test
    void eval_complex_expression() {
        // (id + 10) * 2 > 50
        Schema schema = new Schema(List.of(
            new ColumnMeta("id", Type.INT, null)
        ));
        Tuple tuple = new Tuple(schema, List.of(25));
        
        Expr complex = new ComparisonExpr(pos, ComparisonExpr.Op.GT,
            new BinaryExpr(pos, BinaryExpr.Op.MUL,
                new BinaryExpr(pos, BinaryExpr.Op.ADD,
                    new ColumnRef(pos, null, "id"),
                    new Literal(pos, 10)
                ),
                new Literal(pos, 2)
            ),
            new Literal(pos, 50)
        );
        
        // (25 + 10) * 2 = 70 > 50 = true
        assertEquals(true, evaluator.eval(complex, tuple, schema));
    }
    
    @Test
    void eval_qualified_column_in_join() {
        // Test qualified column resolution in join context
        Schema leftSchema = new Schema(List.of(
            new ColumnMeta("u.id", Type.INT, null),
            new ColumnMeta("u.name", Type.STRING, null)
        ));
        Schema rightSchema = new Schema(List.of(
            new ColumnMeta("o.id", Type.INT, null),
            new ColumnMeta("o.user_id", Type.INT, null)
        ));
        
        Tuple leftTuple = new Tuple(leftSchema, List.of(1, "Alice"));
        Tuple rightTuple = new Tuple(rightSchema, List.of(100, 1));
        
        Set<String> leftQuals = Set.of("u");
        Set<String> rightQuals = Set.of("o");
        
        // u.id
        assertEquals(1, evaluator.eval(
            new ColumnRef(pos, "u", "id"),
            leftTuple, leftSchema,
            rightTuple, rightSchema,
            leftQuals, rightQuals
        ));
        
        // o.id
        assertEquals(100, evaluator.eval(
            new ColumnRef(pos, "o", "id"),
            leftTuple, leftSchema,
            rightTuple, rightSchema,
            leftQuals, rightQuals
        ));
    }
}
