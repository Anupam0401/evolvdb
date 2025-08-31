package io.github.anupam.evolvdb.sql.ast;

/** Visitor for AST nodes. */
public interface AstVisitor<R, C> {
    default R visitNode(AstNode node, C context) { return null; }

    default R visitCreateTable(CreateTable node, C context) { return visitNode(node, context); }
    default R visitDropTable(DropTable node, C context) { return visitNode(node, context); }
    default R visitInsert(Insert node, C context) { return visitNode(node, context); }
    default R visitSelect(Select node, C context) { return visitNode(node, context); }

    default R visitLiteral(Literal node, C context) { return visitNode(node, context); }
    default R visitColumnRef(ColumnRef node, C context) { return visitNode(node, context); }
    default R visitBinaryExpr(BinaryExpr node, C context) { return visitNode(node, context); }
    default R visitLogicalExpr(LogicalExpr node, C context) { return visitNode(node, context); }
    default R visitComparisonExpr(ComparisonExpr node, C context) { return visitNode(node, context); }
    default R visitFuncCall(FuncCall node, C context) { return visitNode(node, context); }

    default R visitTableRef(TableRef node, C context) { return visitNode(node, context); }
    default R visitSelectItem(SelectItem node, C context) { return visitNode(node, context); }
}
