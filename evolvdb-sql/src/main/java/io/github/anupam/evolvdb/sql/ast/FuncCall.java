package io.github.anupam.evolvdb.sql.ast;

import java.util.List;
import java.util.Objects;

/** Function call expression. Supports COUNT(*), COUNT(expr), SUM/AVG/MIN/MAX(expr). */
public final class FuncCall extends Expr {
    private final String name; // function name, as parsed
    private final List<Expr> args; // empty if starArg
    private final boolean starArg; // true only for COUNT(*)

    public FuncCall(SourcePos pos, String name, List<Expr> args, boolean starArg) {
        super(pos);
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name");
        this.name = name;
        this.args = List.copyOf(Objects.requireNonNull(args, "args"));
        this.starArg = starArg;
    }

    public String name() { return name; }
    public List<Expr> args() { return args; }
    public boolean starArg() { return starArg; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFuncCall(this, context);
    }
}
