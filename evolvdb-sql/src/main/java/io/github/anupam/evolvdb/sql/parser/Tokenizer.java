package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.SourcePos;

import java.util.HashMap;
import java.util.Map;

/** Minimal SQL tokenizer with keyword recognition (case-insensitive). */
final class Tokenizer {
    private final String src;
    private int index = 0;
    private int line = 1;
    private int col = 1;

    private static final Map<String, TokenType> KEYWORDS = new HashMap<>();
    static {
        // statements
        KEYWORDS.put("CREATE", TokenType.CREATE);
        KEYWORDS.put("TABLE", TokenType.TABLE);
        KEYWORDS.put("DROP", TokenType.DROP);
        KEYWORDS.put("INSERT", TokenType.INSERT);
        KEYWORDS.put("INTO", TokenType.INTO);
        KEYWORDS.put("VALUES", TokenType.VALUES);
        KEYWORDS.put("SELECT", TokenType.SELECT);
        KEYWORDS.put("FROM", TokenType.FROM);
        KEYWORDS.put("WHERE", TokenType.WHERE);
        KEYWORDS.put("AS", TokenType.AS);
        // booleans / logical
        KEYWORDS.put("AND", TokenType.AND);
        KEYWORDS.put("OR", TokenType.OR);
        KEYWORDS.put("NOT", TokenType.NOT);
        KEYWORDS.put("TRUE", TokenType.TRUE);
        KEYWORDS.put("FALSE", TokenType.FALSE);
        // types
        KEYWORDS.put("INT", TokenType.INT);
        KEYWORDS.put("BIGINT", TokenType.BIGINT);
        KEYWORDS.put("BOOLEAN", TokenType.BOOLEAN);
        KEYWORDS.put("FLOAT", TokenType.FLOAT);
        KEYWORDS.put("STRING", TokenType.STRING_T);
        KEYWORDS.put("VARCHAR", TokenType.VARCHAR);
    }

    Tokenizer(String src) { this.src = src == null ? "" : src; }

    Token next() {
        skipWhitespaceAndComments();
        if (eof()) return new Token(TokenType.EOF, null, pos());
        char c = peekChar();
        // punctuation
        if (c == '(') { advance(); return token(TokenType.LPAREN, "("); }
        if (c == ')') { advance(); return token(TokenType.RPAREN, ")"); }
        if (c == ',') { advance(); return token(TokenType.COMMA, ","); }
        if (c == '.') { advance(); return token(TokenType.DOT, "."); }
        if (c == ';') { advance(); return token(TokenType.SEMI, ";"); }
        if (c == '*') { advance(); return token(TokenType.STAR, "*"); }
        // operators
        if (c == '+') { advance(); return token(TokenType.PLUS, "+"); }
        if (c == '-') {
            // could be comment start "--"
            if (peekAhead(1) == '-') {
                // shouldn't happen because we skip comments earlier, but handle anyway
                skipLine();
                return next();
            }
            advance();
            return token(TokenType.MINUS, "-");
        }
        if (c == '/') { advance(); return token(TokenType.SLASH, "/"); }
        if (c == '=') { advance(); return token(TokenType.EQ, "="); }
        if (c == '<') {
            if (peekAhead(1) == '=') { advance(); advance(); return token(TokenType.LTE, "<="); }
            if (peekAhead(1) == '>') { advance(); advance(); return token(TokenType.NEQ, "<>"); }
            advance(); return token(TokenType.LT, "<");
        }
        if (c == '>') {
            if (peekAhead(1) == '=') { advance(); advance(); return token(TokenType.GTE, ">="); }
            advance(); return token(TokenType.GT, ">");
        }
        if (c == '!') {
            if (peekAhead(1) == '=') { advance(); advance(); return token(TokenType.NEQ, "!="); }
            throw error("Unexpected '!'", pos());
        }
        // string literal
        if (c == '\'') {
            return readString();
        }
        // number (integers only for now)
        if (isDigit(c)) {
            return readNumber();
        }
        // identifier or keyword
        if (isIdentStart(c)) {
            return readIdentOrKeyword();
        }
        throw error("Unexpected character '" + c + "'", pos());
    }

    private void skipWhitespaceAndComments() {
        while (!eof()) {
            char c = peekChar();
            if (Character.isWhitespace(c)) { advance(); continue; }
            // line comment -- ... EOL
            if (c == '-' && peekAhead(1) == '-') { skipLine(); continue; }
            break;
        }
    }

    private void skipLine() {
        while (!eof()) {
            char c = advance();
            if (c == '\n') break;
        }
    }

    private Token readIdentOrKeyword() {
        int startLine = line, startCol = col;
        StringBuilder sb = new StringBuilder();
        sb.append(advance());
        while (!eof() && isIdentPart(peekChar())) sb.append(advance());
        String ident = sb.toString();
        TokenType kw = KEYWORDS.get(ident.toUpperCase());
        if (kw != null) return new Token(kw, ident, new SourcePos(startLine, startCol));
        return new Token(TokenType.IDENT, ident, new SourcePos(startLine, startCol));
    }

    private Token readNumber() {
        int startLine = line, startCol = col;
        StringBuilder sb = new StringBuilder();
        while (!eof() && isDigit(peekChar())) sb.append(advance());
        return new Token(TokenType.NUMBER, sb.toString(), new SourcePos(startLine, startCol));
    }

    private Token readString() {
        int startLine = line, startCol = col;
        advance(); // consume opening quote
        StringBuilder sb = new StringBuilder();
        while (!eof()) {
            char c = advance();
            if (c == '\'') {
                // doubled single-quote -> escaped quote
                if (!eof() && peekChar() == '\'') { advance(); sb.append('\''); continue; }
                // end string
                return new Token(TokenType.STRING, sb.toString(), new SourcePos(startLine, startCol));
            }
            sb.append(c);
        }
        throw error("Unterminated string literal", new SourcePos(startLine, startCol));
    }

    private boolean eof() { return index >= src.length(); }
    private char peekChar() { return src.charAt(index); }
    private char peekAhead(int off) { int i = index + off; return i < src.length() ? src.charAt(i) : '\0'; }
    private char advance() {
        char c = src.charAt(index++);
        if (c == '\n') { line++; col = 1; } else { col++; }
        return c;
    }
    private SourcePos pos() { return new SourcePos(line, col); }
    private Token token(TokenType t, String lex) { return new Token(t, lex, new SourcePos(line, col - (lex == null ? 0 : lex.length()))); }

    private static boolean isIdentStart(char c) { return Character.isLetter(c) || c == '_'; }
    private static boolean isIdentPart(char c) { return Character.isLetterOrDigit(c) || c == '_'; }
    private static boolean isDigit(char c) { return c >= '0' && c <= '9'; }

    private static SqlParseException error(String msg, SourcePos pos) { return new SqlParseException(msg, pos); }
}
