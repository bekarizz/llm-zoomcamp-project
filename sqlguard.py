import re
import sqlglot
from sqlglot import exp

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|call|copy|vacuum|analyze)\b",
    re.I,
)

def is_single_statement(sql: str) -> bool:
    try:
        statements = sqlglot.parse(sql, read="postgres")
        return len(statements) == 1
    except Exception:
        return False

def is_select_ast(sql: str) -> bool:
    try:
        ast = sqlglot.parse_one(sql, read="postgres")
        if ast is None:
            return False
        # Accept SELECT, UNION/UNION ALL (top-level set ops between selects), and subquery wrapping
        if isinstance(ast, exp.Select):
            return True
        if isinstance(ast, (exp.Union, exp.Except, exp.Intersect)):
            return isinstance(ast.left, exp.Select) and isinstance(ast.right, exp.Select)
        if isinstance(ast, exp.Subquery) and isinstance(ast.this, exp.Select):
            return True
        return False
    except Exception:
        return False

def is_safe(sql: str) -> bool:
    s = sql.strip()
    # reject multiple statements
    if not is_single_statement(s):
        return False
    # must start with SELECT (fast path)
    if not s.lower().lstrip().startswith("select"):
        return False
    # forbid dangerous keywords anywhere
    if FORBIDDEN.search(s):
        return False
    # parse & ensure it's a SELECT-like AST
    return is_select_ast(s)
