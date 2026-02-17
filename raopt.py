import radb
import radb.ast
import radb.parse


# ----------------------------------------------------------------------
# Rule 1: Break up conjunctive selections
# σ_{A and B}(R) → σ_A(σ_B(R))
# ----------------------------------------------------------------------

def rule_break_up_selections(expr):

    if isinstance(expr, radb.ast.Select):
        rewritten_input = rule_break_up_selections(expr.inputs[0])

        if isinstance(expr.cond, radb.ast.ValExprBinaryOp) and expr.cond.op == radb.ast.sym.AND:
            left_cond, right_cond = expr.cond.inputs
            inner = radb.ast.Select(right_cond, rewritten_input)
            outer = radb.ast.Select(left_cond, inner)
            return rule_break_up_selections(outer)

        return radb.ast.Select(expr.cond, rewritten_input)

    if isinstance(expr, radb.ast.Project):
        return radb.ast.Project(expr.attrs, rule_break_up_selections(expr.inputs[0]))

    if isinstance(expr, radb.ast.Rename):
        return radb.ast.Rename(expr.relname, expr.attrnames,
                               rule_break_up_selections(expr.inputs[0]))

    if isinstance(expr, radb.ast.Cross):
        return radb.ast.Cross(rule_break_up_selections(expr.inputs[0]),
                              rule_break_up_selections(expr.inputs[1]))

    if isinstance(expr, radb.ast.Join):
        return radb.ast.Join(rule_break_up_selections(expr.inputs[0]),
                             expr.cond,
                             rule_break_up_selections(expr.inputs[1]))

    return expr


# ----------------------------------------------------------------------
# Helper utilities for push-down
# ----------------------------------------------------------------------

def extract_condition_attrs(cond):
    attrs = set()

    if isinstance(cond, radb.ast.AttrRef):
        name = str(cond)
        attrs.add(name)
        if "." in name:
            attrs.add(name.split(".", 1)[1])

    elif isinstance(cond, radb.ast.ValExprBinaryOp):
        attrs |= extract_condition_attrs(cond.inputs[0])
        attrs |= extract_condition_attrs(cond.inputs[1])

    return attrs


def extract_expr_attrs(expr, dd):
    attrs = set()

    if isinstance(expr, radb.ast.RelRef):
        for a in dd.get(expr.rel, {}):
            attrs.add(a)
            attrs.add(expr.rel + "." + a)

    elif isinstance(expr, radb.ast.Rename):
        inner = extract_expr_attrs(expr.inputs[0], dd)
        for a in inner:
            if "." in a:
                _, base = a.split(".", 1)
                attrs.add(base)
                attrs.add(expr.relname + "." + base)
            else:
                attrs.add(a)
                attrs.add(expr.relname + "." + a)

    elif isinstance(expr, (radb.ast.Select, radb.ast.Project)):
        attrs |= extract_expr_attrs(expr.inputs[0], dd)

    elif isinstance(expr, (radb.ast.Cross, radb.ast.Join)):
        attrs |= extract_expr_attrs(expr.inputs[0], dd)
        attrs |= extract_expr_attrs(expr.inputs[1], dd)

    return attrs


def can_push_down(cond, expr, dd):
    return extract_condition_attrs(cond).issubset(extract_expr_attrs(expr, dd))


def is_join_condition(cond, left_attrs, right_attrs):
    cond_attrs = extract_condition_attrs(cond)

    qualified = {a for a in cond_attrs if "." in a}
    if not qualified:
        return bool(cond_attrs & left_attrs) and bool(cond_attrs & right_attrs)

    left_prefixes = {a.split(".")[0] for a in left_attrs if "." in a}
    right_prefixes = {a.split(".")[0] for a in right_attrs if "." in a}
    cond_prefixes = {a.split(".")[0] for a in qualified}

    return bool(cond_prefixes & left_prefixes) and bool(cond_prefixes & right_prefixes)


# ----------------------------------------------------------------------
# Rule 2: Push down selections
# ----------------------------------------------------------------------

def rule_push_down_selections(expr, dd):

    if isinstance(expr, radb.ast.Select):
        child = rule_push_down_selections(expr.inputs[0], dd)

        # Case 1: selection over cross
        if isinstance(child, radb.ast.Cross):
            left, right = child.inputs
            left_attrs = extract_expr_attrs(left, dd)
            right_attrs = extract_expr_attrs(right, dd)

            if is_join_condition(expr.cond, left_attrs, right_attrs):
                return radb.ast.Select(expr.cond, child)

            if can_push_down(expr.cond, left, dd):
                return radb.ast.Cross(
                    rule_push_down_selections(radb.ast.Select(expr.cond, left), dd),
                    right
                )

            if can_push_down(expr.cond, right, dd):
                return radb.ast.Cross(
                    left,
                    rule_push_down_selections(radb.ast.Select(expr.cond, right), dd)
                )

            return radb.ast.Select(expr.cond, child)

        # Case 2: selection over selection over cross  ⭐ FIXED CASE ⭐
        if isinstance(child, radb.ast.Select) and isinstance(child.inputs[0], radb.ast.Cross):
            cross = child.inputs[0]
            left, right = cross.inputs

            left_attrs = extract_expr_attrs(left, dd)
            right_attrs = extract_expr_attrs(right, dd)

            outer_join = is_join_condition(expr.cond, left_attrs, right_attrs)
            inner_join = is_join_condition(child.cond, left_attrs, right_attrs)

            if not outer_join and inner_join:
                if can_push_down(expr.cond, left, dd):
                    return radb.ast.Select(
                        child.cond,
                        radb.ast.Cross(
                            rule_push_down_selections(
                                radb.ast.Select(expr.cond, left), dd
                            ),
                            right
                        )
                    )

                if can_push_down(expr.cond, right, dd):
                    return radb.ast.Select(
                        child.cond,
                        radb.ast.Cross(
                            left,
                            rule_push_down_selections(
                                radb.ast.Select(expr.cond, right), dd
                            )
                        )
                    )

        return radb.ast.Select(expr.cond, child)

    if isinstance(expr, radb.ast.Project):
        return radb.ast.Project(expr.attrs,
                                rule_push_down_selections(expr.inputs[0], dd))

    if isinstance(expr, radb.ast.Rename):
        return radb.ast.Rename(expr.relname, expr.attrnames,
                               rule_push_down_selections(expr.inputs[0], dd))

    if isinstance(expr, radb.ast.Cross):
        return radb.ast.Cross(
            rule_push_down_selections(expr.inputs[0], dd),
            rule_push_down_selections(expr.inputs[1], dd)
        )

    if isinstance(expr, radb.ast.Join):
        return radb.ast.Join(
            rule_push_down_selections(expr.inputs[0], dd),
            expr.cond,
            rule_push_down_selections(expr.inputs[1], dd)
        )

    return expr




def rule_push_down_projections(expr, dd):
    # 1. Recurse down to the leaves first to handle nested Joins
    if hasattr(expr, 'inputs'):
        expr.inputs = [rule_push_down_projections(i, dd) for i in expr.inputs]

    if isinstance(expr, radb.ast.Project):
        child = expr.inputs[0]
        
        if isinstance(child, radb.ast.Join):
            # 2. Get attributes needed for the JOIN condition
            cond_attrs = extract_condition_attrs(child.cond)
            # 3. Get attributes needed for the final SELECT/OUTPUT
            final_attrs = {str(a) for a in expr.attrs}
            
            # Combine them: these are the ONLY columns allowed to pass
            required_attrs = cond_attrs | final_attrs
            
            # 4. Determine which attributes belong to which branch
            left_all = extract_expr_attrs(child.inputs[0], dd)
            right_all = extract_expr_attrs(child.inputs[1], dd)
            
            def to_attr_ref(name):
                if "." in name:
                    rel, col = name.split(".", 1)
                    return radb.ast.AttrRef(rel, col)
                return radb.ast.AttrRef(None, name)

            # 5. Create new Projects for left and right
            left_needed = [to_attr_ref(a) for a in required_attrs if a in left_all]
            right_needed = [to_attr_ref(a) for a in required_attrs if a in right_all]
            
            # 6. Only add the Project if it actually prunes columns
            new_left = child.inputs[0]
            if len(left_needed) < len(left_all):
                new_left = radb.ast.Project(left_needed, child.inputs[0])
                
            new_right = child.inputs[1]
            if len(right_needed) < len(right_all):
                new_right = radb.ast.Project(right_needed, child.inputs[1])
            
            # Replace the child of the original project with the pruned join
            return radb.ast.Project(expr.attrs, radb.ast.Join(new_left, child.cond, new_right))
            
    return expr

# ----------------------------------------------------------------------
# Rule 3: Merge selections
# σ_A(σ_B(R)) → σ_{A and B}(R)
# ----------------------------------------------------------------------

def rule_merge_selections(expr):

    if isinstance(expr, radb.ast.Select):
        conditions = []
        cur = expr

        while isinstance(cur, radb.ast.Select):
            conditions.append(cur.cond)
            cur = cur.inputs[0]

        base = rule_merge_selections(cur)

        if len(conditions) == 1:
            return radb.ast.Select(conditions[0], base)

        merged = conditions[0]
        i = 1
        while i < len(conditions):
            merged = radb.ast.ValExprBinaryOp(merged, radb.ast.sym.AND, conditions[i])
            i += 1

        return radb.ast.Select(merged, base)

    if isinstance(expr, radb.ast.Project):
        return radb.ast.Project(expr.attrs, rule_merge_selections(expr.inputs[0]))

    if isinstance(expr, radb.ast.Rename):
        return radb.ast.Rename(expr.relname, expr.attrnames,
                               rule_merge_selections(expr.inputs[0]))

    if isinstance(expr, radb.ast.Cross):
        return radb.ast.Cross(rule_merge_selections(expr.inputs[0]),
                              rule_merge_selections(expr.inputs[1]))

    if isinstance(expr, radb.ast.Join):
        return radb.ast.Join(rule_merge_selections(expr.inputs[0]),
                             expr.cond,
                             rule_merge_selections(expr.inputs[1]))

    return expr


# ----------------------------------------------------------------------
# Rule 4: Introduce joins
# ----------------------------------------------------------------------

def extract_relations(expr):
    rels = set()

    if isinstance(expr, radb.ast.RelRef):
        rels.add(expr.rel)

    elif isinstance(expr, radb.ast.Rename):
        rels.add(expr.relname)

    elif isinstance(expr, (radb.ast.Select, radb.ast.Project)):
        rels |= extract_relations(expr.inputs[0])

    elif isinstance(expr, (radb.ast.Cross, radb.ast.Join)):
        rels |= extract_relations(expr.inputs[0])
        rels |= extract_relations(expr.inputs[1])

    return rels


def extract_condition_prefixes(cond):
    prefixes = set()

    if isinstance(cond, radb.ast.AttrRef):
        name = str(cond)
        if "." in name:
            prefixes.add(name.split(".", 1)[0])

    elif isinstance(cond, radb.ast.ValExprBinaryOp):
        prefixes |= extract_condition_prefixes(cond.inputs[0])
        prefixes |= extract_condition_prefixes(cond.inputs[1])

    return prefixes


def rule_introduce_joins(expr):

    if isinstance(expr, radb.ast.Select):
        rewritten = rule_introduce_joins(expr.inputs[0])

        if isinstance(rewritten, radb.ast.Cross):
            left, right = rewritten.inputs
            cond_prefixes = extract_condition_prefixes(expr.cond)

            if cond_prefixes & extract_relations(left) and cond_prefixes & extract_relations(right):
                return radb.ast.Join(left, expr.cond, right)

        return radb.ast.Select(expr.cond, rewritten)

    if isinstance(expr, radb.ast.Project):
        return radb.ast.Project(expr.attrs,
                                rule_introduce_joins(expr.inputs[0]))

    if isinstance(expr, radb.ast.Rename):
        return radb.ast.Rename(expr.relname, expr.attrnames,
                               rule_introduce_joins(expr.inputs[0]))

    if isinstance(expr, radb.ast.Cross):
        return radb.ast.Cross(rule_introduce_joins(expr.inputs[0]),
                              rule_introduce_joins(expr.inputs[1]))

    if isinstance(expr, radb.ast.Join):
        return radb.ast.Join(rule_introduce_joins(expr.inputs[0]),
                             expr.cond,
                             rule_introduce_joins(expr.inputs[1]))

    return expr
