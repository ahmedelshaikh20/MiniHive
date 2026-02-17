import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import DML, Keyword, String, Number, Literal, Whitespace, Punctuation

import radb.ast as ra
from radb.parse import RAParser as sym


def get_select_items(stmt):
    s = False
    for t in stmt.tokens:
        if t.ttype is DML and t.value.lower() == "select":
            s = True
            continue
        if s:
            if t.ttype is Keyword and t.value.lower() == "distinct":
                continue
            if isinstance(t, IdentifierList):
                return list(t.get_identifiers())
            if isinstance(t, Identifier):
                return [t]
            if t.ttype not in (Whitespace, Punctuation):
                return [t]
    return []


def get_from_items(stmt):
    f = False
    for t in stmt.tokens:
        if t.ttype is Keyword and t.value.lower() == "from":
            f = True
            continue
        if f:
            if isinstance(t, IdentifierList):
                return list(t.get_identifiers())
            if isinstance(t, Identifier):
                return [t]
            if t.ttype is Keyword:
                break
    return []


def get_where_part(stmt):
    for t in stmt.tokens:
        if isinstance(t, Where):
            return t
    return None


def get_where_comparisons(w):
    if not w:
        return []
    out = []
    for t in w.tokens:
        if isinstance(t, Comparison):
            out.append(t)
    return out


def make_attr(x):
    if isinstance(x, Identifier):
        r = x.get_parent_name()
        c = x.get_real_name()
        return ra.AttrRef(r, c)
    v = x.value.strip()
    if "." in v:
        r, c = v.split(".", 1)
        return ra.AttrRef(r, c)
    return ra.AttrRef(None, v)


def make_val(x):
    if isinstance(x, Identifier):
        return make_attr(x)
    t = x.ttype
    v = x.value.strip()
    if t in String or t in Literal.String:
        return ra.RAString(v)
    if t in Number:
        return ra.RANumber(v)
    if "." in v:
        r, c = v.split(".", 1)
        return ra.AttrRef(r, c)
    return ra.AttrRef(None, v)


def one_comparison(c):
    toks = [t for t in c.tokens if not t.is_whitespace]
    for i in range(len(toks)):
        if toks[i].value == "=":
            left = toks[i - 1]
            right = toks[i + 1]
            return ra.ValExprBinaryOp(make_val(left), sym.EQ, make_val(right))
    return None


def combine_all(comps):
    if len(comps) == 0:
        return None
    if len(comps) == 1:
        return one_comparison(comps[0])
    x = ra.ValExprBinaryOp(one_comparison(comps[0]), sym.AND, one_comparison(comps[1]))
    for c in comps[2:]:
        x = ra.ValExprBinaryOp(x, sym.AND, one_comparison(c))
    return x


def build_from(stmt):
    items = get_from_items(stmt)
    rels = []
    for it in items:
        name = it.get_real_name()
        alias = it.get_alias()
        r = ra.RelRef(name)
        if alias:
            r = ra.Rename(alias, ["*"], r)
        rels.append(r)
    cur = rels[0]
    for r in rels[1:]:
        cur = ra.Cross(cur, r)
    return cur


def translate(stmt):
    base = build_from(stmt)
    w = get_where_part(stmt)
    comps = get_where_comparisons(w)
    cond = combine_all(comps)
    if cond:
        base = ra.Select(cond, base)
    sel = get_select_items(stmt)
    if len(sel) == 1 and sel[0].value.strip() == "*":
        return base
    attrs = []
    for s in sel:
        attrs.append(make_attr(s))
    return ra.Project(attrs, base)
