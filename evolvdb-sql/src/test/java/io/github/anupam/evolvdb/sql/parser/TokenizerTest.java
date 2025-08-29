package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.SourcePos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TokenizerTest {

    @Test
    void givenSelectWithLiterals_whenTokenize_thenTokensSequenceMatches() {
        String sql = "SELECT * FROM users WHERE id >= 10 AND name = 'O''Reilly'";
        Tokenizer tz = new Tokenizer(sql);
        Token[] tokens = new Token[] {
                tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next(), tz.next()
        };
        assertEquals(TokenType.SELECT, tokens[0].type());
        assertEquals(TokenType.STAR, tokens[1].type());
        assertEquals(TokenType.FROM, tokens[2].type());
        assertEquals(TokenType.IDENT, tokens[3].type());
        assertEquals("users", tokens[3].lexeme());
        assertEquals(TokenType.WHERE, tokens[4].type());
        assertEquals(TokenType.IDENT, tokens[5].type());
        assertEquals("id", tokens[5].lexeme());
        assertEquals(TokenType.GTE, tokens[6].type());
        assertEquals(TokenType.NUMBER, tokens[7].type());
        assertEquals("10", tokens[7].lexeme());
        assertEquals(TokenType.AND, tokens[8].type());
        assertEquals(TokenType.IDENT, tokens[9].type());
        assertEquals("name", tokens[9].lexeme());
        assertEquals(TokenType.EQ, tokens[10].type());
        assertEquals(TokenType.STRING, tokens[11].type());
        assertEquals("O'Reilly", tokens[11].lexeme());
        assertEquals(TokenType.EOF, tokens[12].type());
    }

    @Test
    void givenUnterminatedString_whenTokenize_thenThrowsWithPosition() {
        String sql = "SELECT 'abc";
        Tokenizer tz = new Tokenizer(sql);
        // consume SELECT
        tz.next();
        SqlParseException ex = assertThrows(SqlParseException.class, tz::next);
        assertTrue(ex.getMessage().contains("Unterminated string literal"));
    }

    @Test
    void givenInvalidCharacter_whenTokenize_thenThrows() {
        String sql = "SELECT @";
        Tokenizer tz = new Tokenizer(sql);
        // consume SELECT
        tz.next();
        SqlParseException ex = assertThrows(SqlParseException.class, tz::next);
        assertTrue(ex.getMessage().contains("Unexpected character"));
    }
}
