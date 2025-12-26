package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UpdateDeleteParserTest {

    @Test
    void givenSimpleUpdate_whenParse_thenCorrectAst() {
        String sql = "UPDATE users SET age = 30";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        assertEquals("users", update.tableName());
        assertEquals(1, update.assignments().size());
        assertTrue(update.assignments().containsKey("age"));
        assertNull(update.where());
    }

    @Test
    void givenUpdateMultipleColumns_whenParse_thenAllAssignments() {
        String sql = "UPDATE users SET age = 30, name = 'Alice', active = true";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        assertEquals("users", update.tableName());
        assertEquals(3, update.assignments().size());
        assertTrue(update.assignments().containsKey("age"));
        assertTrue(update.assignments().containsKey("name"));
        assertTrue(update.assignments().containsKey("active"));
    }

    @Test
    void givenUpdateWithWhere_whenParse_thenWhereClausePresent() {
        String sql = "UPDATE users SET age = 30 WHERE id = 1";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        assertEquals("users", update.tableName());
        assertNotNull(update.where());
        assertInstanceOf(ComparisonExpr.class, update.where());
    }

    @Test
    void givenUpdateWithExpression_whenParse_thenExpressionInAssignment() {
        String sql = "UPDATE products SET price = price + 10";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        assertTrue(update.assignments().containsKey("price"));
        assertInstanceOf(BinaryExpr.class, update.assignments().get("price"));
    }

    @Test
    void givenUpdateWithComplexWhere_whenParse_thenWhereCorrect() {
        String sql = "UPDATE users SET status = 'inactive' WHERE age > 18 AND active = true";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        assertNotNull(update.where());
        assertInstanceOf(LogicalExpr.class, update.where());
    }

    @Test
    void givenSimpleDelete_whenParse_thenCorrectAst() {
        String sql = "DELETE FROM users";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Delete.class, stmt);
        Delete delete = (Delete) stmt;
        assertEquals("users", delete.tableName());
        assertNull(delete.where());
    }

    @Test
    void givenDeleteWithWhere_whenParse_thenWhereClausePresent() {
        String sql = "DELETE FROM users WHERE age < 18";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Delete.class, stmt);
        Delete delete = (Delete) stmt;
        assertEquals("users", delete.tableName());
        assertNotNull(delete.where());
        assertInstanceOf(ComparisonExpr.class, delete.where());
    }

    @Test
    void givenDeleteWithComplexWhere_whenParse_thenWhereCorrect() {
        String sql = "DELETE FROM users WHERE age < 18 OR status = 'inactive'";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Delete.class, stmt);
        Delete delete = (Delete) stmt;
        assertNotNull(delete.where());
        assertInstanceOf(LogicalExpr.class, delete.where());
    }

    @Test
    void givenUpdateWithoutSet_whenParse_thenThrowsException() {
        String sql = "UPDATE users WHERE id = 1";
        SqlParser parser = new SqlParser();

        assertThrows(RuntimeException.class, () -> parser.parse(sql));
    }

    @Test
    void givenUpdateDuplicateColumn_whenParse_thenThrowsException() {
        String sql = "UPDATE users SET age = 30, age = 40";
        SqlParser parser = new SqlParser();

        assertThrows(RuntimeException.class, () -> parser.parse(sql));
    }

    @Test
    void givenDeleteWithoutFrom_whenParse_thenThrowsException() {
        String sql = "DELETE users WHERE id = 1";
        SqlParser parser = new SqlParser();

        assertThrows(RuntimeException.class, () -> parser.parse(sql));
    }

    @Test
    void givenUpdateCaseInsensitive_whenParse_thenSuccess() {
        String sql = "update users set age = 30 where id = 1";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
    }

    @Test
    void givenDeleteCaseInsensitive_whenParse_thenSuccess() {
        String sql = "delete from users where id = 1";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Delete.class, stmt);
    }

    @Test
    void givenUpdateWithArithmeticExpr_whenParse_thenCorrectAst() {
        String sql = "UPDATE counters SET count = count + 1 WHERE name = 'visitors'";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        Expr countExpr = update.assignments().get("count");
        assertInstanceOf(BinaryExpr.class, countExpr);
        BinaryExpr binExpr = (BinaryExpr) countExpr;
        assertEquals(BinaryExpr.Op.ADD, binExpr.op());
    }

    @Test
    void givenUpdateWithStringLiteral_whenParse_thenCorrectValue() {
        String sql = "UPDATE users SET name = 'John Doe' WHERE id = 1";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        Expr nameExpr = update.assignments().get("name");
        assertInstanceOf(Literal.class, nameExpr);
    }

    @Test
    void givenUpdateWithNullAssignment_whenParse_thenCorrectAst() {
        String sql = "UPDATE users SET email = NULL WHERE id = 1";
        SqlParser parser = new SqlParser();

        Statement stmt = (Statement) parser.parse(sql);

        assertInstanceOf(Update.class, stmt);
        Update update = (Update) stmt;
        assertTrue(update.assignments().containsKey("email"));
    }
}
