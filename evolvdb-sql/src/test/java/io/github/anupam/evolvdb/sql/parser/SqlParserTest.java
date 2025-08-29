package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlParserTest {

    @Test
    void testParseCreateTable_basic() {
            String sql = "CREATE TABLE users (id INT, name VARCHAR(10), active BOOLEAN)";
            SqlParser p = new SqlParser();
            AstNode node = p.parse(sql);
            assertTrue(node instanceof CreateTable);
            CreateTable ct = (CreateTable) node;
            assertEquals("users", ct.tableName());
            assertEquals(3, ct.columns().size());
            assertEquals("id", ct.columns().get(0).name());
            assertEquals(io.github.anupam.evolvdb.types.Type.INT, ct.columns().get(0).type());
            assertEquals("name", ct.columns().get(1).name());
            assertEquals(io.github.anupam.evolvdb.types.Type.VARCHAR, ct.columns().get(1).type());
            assertEquals(10, ct.columns().get(1).length());
    }

    @Test
    void testParseInsert_basic() {
        String sql = "INSERT INTO users VALUES (1, 'Alice')";
        SqlParser p = new SqlParser();
        AstNode n = p.parse(sql);
        assertTrue(n instanceof Insert);
        Insert ins = (Insert) n;
        assertEquals("users", ins.tableName());
        assertTrue(ins.columns().isEmpty());
        assertEquals(1, ins.rows().size());
        assertEquals(2, ins.rows().get(0).size());
    }

    @Test
    void testParseSelect_basic() {
        String sql = "SELECT name FROM users WHERE id >= 10";
        SqlParser p = new SqlParser();
        AstNode n = p.parse(sql);
        assertTrue(n instanceof Select);
        Select s = (Select) n;
        assertEquals("users", s.from().tableName());
        assertEquals(1, s.items().size());
        assertNotNull(s.where());
    }

    @Test
    void testInvalidSyntax_reportsErrorPosition() {
        String sql = "SELECT FROM";
        SqlParser p = new SqlParser();
        SqlParseException ex = assertThrows(SqlParseException.class, () -> p.parse(sql));
        assertTrue(ex.getMessage().contains("Unexpected token"));
    }
}
