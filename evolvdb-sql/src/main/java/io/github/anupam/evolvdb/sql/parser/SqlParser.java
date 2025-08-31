package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.AstNode;
import io.github.anupam.evolvdb.sql.ast.BinaryExpr;
import io.github.anupam.evolvdb.sql.ast.ColumnDef;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.ComparisonExpr;
import io.github.anupam.evolvdb.sql.ast.CreateTable;
import io.github.anupam.evolvdb.sql.ast.DropTable;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.sql.ast.Insert;
import io.github.anupam.evolvdb.sql.ast.Literal;
import io.github.anupam.evolvdb.sql.ast.LogicalExpr;
import io.github.anupam.evolvdb.sql.ast.Select;
import io.github.anupam.evolvdb.sql.ast.SelectItem;
import io.github.anupam.evolvdb.sql.ast.SourcePos;
import io.github.anupam.evolvdb.sql.ast.Statement;
import io.github.anupam.evolvdb.sql.ast.TableRef;
import io.github.anupam.evolvdb.types.Type;

/**
 * SQL parser entrypoint. Produces a typed AST from SQL text.
 *
 * Implementation will be a hand-rolled recursive descent parser over a token stream.
 */
public final class SqlParser {

    private Tokenizer tz;
    private Token cur;

    /** Parses the given SQL string into an AST. */
    public AstNode parse(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql");
        this.tz = new Tokenizer(sql);
        this.cur = tz.next();
        Statement stmt = parseStatement();
        // optional trailing semicolon
        if (match(TokenType.SEMI)) { /* consume */ }
        expect(TokenType.EOF, "end of input");
        return stmt;
    }

    private Statement parseStatement() {
        return switch (cur.type()) {
            case CREATE -> parseCreateTable();
            case DROP -> parseDropTable();
            case INSERT -> parseInsert();
            case SELECT -> parseSelect();
            default -> throw error("Expected a statement (CREATE/DROP/INSERT/SELECT)");
        };
    }

    private CreateTable parseCreateTable() {
        SourcePos pos = cur.pos();
        expect(TokenType.CREATE, "CREATE");
        expect(TokenType.TABLE, "TABLE");
        String name = expectIdent("table name");
        expect(TokenType.LPAREN, "(");
        java.util.List<ColumnDef> cols = new java.util.ArrayList<>();
        do {
            String colName = expectIdent("column name");
            Type type = parseType();
            Integer len = null;
            if (type == Type.VARCHAR) {
                expect(TokenType.LPAREN, "(");
                String n = take(TokenType.NUMBER, "varchar length").lexeme();
                try { len = Integer.parseInt(n); } catch (NumberFormatException e) { throw error("Invalid varchar length"); }
                expect(TokenType.RPAREN, ")");
            }
            cols.add(new ColumnDef(colName, type, len));
        } while (match(TokenType.COMMA));
        expect(TokenType.RPAREN, ")");
        return new CreateTable(pos, name, cols);
    }

    private DropTable parseDropTable() {
        SourcePos pos = cur.pos();
        expect(TokenType.DROP, "DROP");
        expect(TokenType.TABLE, "TABLE");
        String name = expectIdent("table name");
        return new DropTable(pos, name);
    }

    private Insert parseInsert() {
        SourcePos pos = cur.pos();
        expect(TokenType.INSERT, "INSERT");
        expect(TokenType.INTO, "INTO");
        String table = expectIdent("table name");
        java.util.List<String> cols = new java.util.ArrayList<>();
        if (match(TokenType.LPAREN)) {
            do {
                cols.add(expectIdent("column name"));
            } while (match(TokenType.COMMA));
            expect(TokenType.RPAREN, ")");
        }
        expect(TokenType.VALUES, "VALUES");
        java.util.List<java.util.List<Expr>> rows = new java.util.ArrayList<>();
        do {
            expect(TokenType.LPAREN, "(");
            java.util.List<Expr> vals = new java.util.ArrayList<>();
            do {
                vals.add(parseExpr());
            } while (match(TokenType.COMMA));
            expect(TokenType.RPAREN, ")");
            rows.add(vals);
        } while (match(TokenType.COMMA));
        return new Insert(pos, table, cols, rows);
    }

    private Select parseSelect() {
        SourcePos pos = cur.pos();
        expect(TokenType.SELECT, "SELECT");
        java.util.List<SelectItem> items = new java.util.ArrayList<>();
        if (match(TokenType.STAR)) {
            items.add(SelectItem.star(cur.pos()));
        } else {
            items.add(parseSelectItem());
            while (match(TokenType.COMMA)) items.add(parseSelectItem());
        }
        expect(TokenType.FROM, "FROM");
        java.util.List<TableRef> froms = new java.util.ArrayList<>();
        froms.add(parseTableRef());
        while (match(TokenType.COMMA)) froms.add(parseTableRef());
        Expr where = null;
        if (match(TokenType.WHERE)) where = parseExpr();
        java.util.List<Expr> groupBy = java.util.List.of();
        if (match(TokenType.GROUP)) {
            expect(TokenType.BY, "BY");
            java.util.List<Expr> groups = new java.util.ArrayList<>();
            groups.add(parseExpr());
            while (match(TokenType.COMMA)) groups.add(parseExpr());
            groupBy = groups;
        }
        return new Select(pos, items, froms, where, groupBy);
    }

    private SelectItem parseSelectItem() {
        Expr expr = parseExpr();
        String alias = null;
        if (match(TokenType.AS)) alias = expectIdent("alias");
        else if (cur.type() == TokenType.IDENT) { // implicit alias
            alias = expectIdent("alias");
        }
        return new SelectItem(expr.pos(), expr, false, alias);
    }

    private TableRef parseTableRef() {
        SourcePos pos = cur.pos();
        String name = expectIdent("table name");
        String alias = null;
        if (match(TokenType.AS)) alias = expectIdent("alias");
        else if (cur.type() == TokenType.IDENT) alias = expectIdent("alias");
        return new TableRef(pos, name, alias);
    }

    private Expr parseExpr() { return parseOr(); }
    private Expr parseOr() {
        Expr left = parseAnd();
        while (cur.type() == TokenType.OR) { advance(); left = new LogicalExpr(cur.pos(), LogicalExpr.Op.OR, left, parseAnd()); }
        return left;
    }
    private Expr parseAnd() {
        Expr left = parseNot();
        while (cur.type() == TokenType.AND) { advance(); left = new LogicalExpr(cur.pos(), LogicalExpr.Op.AND, left, parseNot()); }
        return left;
    }
    private Expr parseNot() {
        if (cur.type() == TokenType.NOT) { SourcePos pos = cur.pos(); advance(); return new LogicalExpr(pos, LogicalExpr.Op.NOT, parseNot(), null); }
        return parseComparison();
    }
    private Expr parseComparison() {
        Expr left = parseAdd();
        switch (cur.type()) {
            case EQ -> { advance(); return new ComparisonExpr(cur.pos(), ComparisonExpr.Op.EQ, left, parseAdd()); }
            case NEQ -> { advance(); return new ComparisonExpr(cur.pos(), ComparisonExpr.Op.NEQ, left, parseAdd()); }
            case LT -> { advance(); return new ComparisonExpr(cur.pos(), ComparisonExpr.Op.LT, left, parseAdd()); }
            case LTE -> { advance(); return new ComparisonExpr(cur.pos(), ComparisonExpr.Op.LTE, left, parseAdd()); }
            case GT -> { advance(); return new ComparisonExpr(cur.pos(), ComparisonExpr.Op.GT, left, parseAdd()); }
            case GTE -> { advance(); return new ComparisonExpr(cur.pos(), ComparisonExpr.Op.GTE, left, parseAdd()); }
            default -> { return left; }
        }
    }
    private Expr parseAdd() {
        Expr left = parseMul();
        while (cur.type() == TokenType.PLUS || cur.type() == TokenType.MINUS) {
            SourcePos pos = cur.pos();
            if (match(TokenType.PLUS)) left = new BinaryExpr(pos, BinaryExpr.Op.ADD, left, parseMul());
            else { advance(); left = new BinaryExpr(pos, BinaryExpr.Op.SUB, left, parseMul()); }
        }
        return left;
    }
    private Expr parseMul() {
        Expr left = parsePrimary();
        while (cur.type() == TokenType.STAR || cur.type() == TokenType.SLASH) {
            SourcePos pos = cur.pos();
            if (match(TokenType.STAR)) left = new BinaryExpr(pos, BinaryExpr.Op.MUL, left, parsePrimary());
            else { advance(); left = new BinaryExpr(pos, BinaryExpr.Op.DIV, left, parsePrimary()); }
        }
        return left;
    }
    private Expr parsePrimary() {
        switch (cur.type()) {
            case NUMBER -> {
                SourcePos pos = cur.pos();
                String lex = cur.lexeme(); advance();
                try {
                    long l = Long.parseLong(lex);
                    Object v = (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? (int) l : l;
                    return new Literal(pos, v);
                } catch (NumberFormatException e) { throw error("Invalid number"); }
            }
            case STRING -> { SourcePos pos = cur.pos(); String s = cur.lexeme(); advance(); return new Literal(pos, s); }
            case TRUE -> { SourcePos pos = cur.pos(); advance(); return new Literal(pos, Boolean.TRUE); }
            case FALSE -> { SourcePos pos = cur.pos(); advance(); return new Literal(pos, Boolean.FALSE); }
            case IDENT -> {
                SourcePos pos = cur.pos();
                String first = cur.lexeme(); advance();
                if (match(TokenType.DOT)) {
                    String col = expectIdent("column");
                    return new ColumnRef(pos, first, col);
                }
                if (cur.type() == TokenType.LPAREN) {
                    // function call
                    advance(); // consume '('
                    java.util.List<Expr> args = new java.util.ArrayList<>();
                    boolean starArg = false;
                    if (match(TokenType.STAR)) {
                        starArg = true;
                    } else if (cur.type() != TokenType.RPAREN) {
                        args.add(parseExpr());
                        while (match(TokenType.COMMA)) args.add(parseExpr());
                    }
                    expect(TokenType.RPAREN, ")");
                    return new io.github.anupam.evolvdb.sql.ast.FuncCall(pos, first, args, starArg);
                }
                return new ColumnRef(pos, null, first);
            }
            case LPAREN -> { advance(); Expr e = parseExpr(); expect(TokenType.RPAREN, ")"); return e; }
            default -> throw error("Unexpected token in expression: " + cur.type());
        }
    }

    private Type parseType() {
        return switch (cur.type()) {
            case INT -> { advance(); yield Type.INT; }
            case BIGINT -> { advance(); yield Type.BIGINT; }
            case BOOLEAN -> { advance(); yield Type.BOOLEAN; }
            case FLOAT -> { advance(); yield Type.FLOAT; }
            case STRING_T -> { advance(); yield Type.STRING; }
            case VARCHAR -> { advance(); yield Type.VARCHAR; }
            default -> throw error("Expected a type name");
        };
    }

    private void expect(TokenType t, String what) {
        if (cur.type() != t) throw error("Expected " + what + ", found " + cur.type());
        advance();
    }
    private Token take(TokenType t, String what) {
        if (cur.type() != t) throw error("Expected " + what + ", found " + cur.type());
        Token tok = cur;
        advance();
        return tok;
    }
    private String expectIdent(String what) {
        if (cur.type() != TokenType.IDENT) throw error("Expected " + what);
        String s = cur.lexeme();
        advance();
        return s;
    }
    private boolean match(TokenType t) { if (cur.type() == t) { advance(); return true; } return false; }
    private void advance() { cur = tz.next(); }
    private SqlParseException error(String msg) { return new SqlParseException(msg, cur.pos()); }
}
