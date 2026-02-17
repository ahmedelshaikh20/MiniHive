from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

'''
Control where the input data comes from, and where output data should go.
'''


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


'''
Switches between different execution environments and file systems.
'''


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''


def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if isinstance(raquery, ChainedOp):
        # A ChainedOp is a single step that combines multiple operations
        return 1 + count_steps(raquery.inputs[0])

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)
    
    '''
    Optimization flag to enable chain folding
    '''
    optimize = luigi.BoolParameter(default=False)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''


def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False):
    assert (isinstance(raquery, radb.ast.Node))

    if optimize:
        # Try to fold chains of Select-Project-Rename operations
        raquery = try_fold_chain(raquery)

    if isinstance(raquery, ChainedOp):
        return ChainedTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class ChainedOp(radb.ast.RelExpr):
    """
    Represents a chain of Select/Project/Rename operations folded into one.

    IMPORTANT: __str__ must emit radb-parseable syntax, including the special
    rename-with-star form: \rename_{P:*} (R)
    """
    def __init__(self, operations, input_node):
        super().__init__()
        self.operations = operations          # inner -> outer (as produced by try_fold_chain after reverse())
        self.inputs = [input_node]

    def __str__(self):
        # Base input: radb expects the base relation in parentheses: (Person)
        base = str(self.inputs[0])
        if isinstance(self.inputs[0], radb.ast.RelRef):
            base = f"({base})"

        # Emit as a prefix operator chain (outer -> inner), radb-style:
        # \select_{...} \rename_{P:*} (Person)
        ops = []
        for op_type, params in reversed(self.operations):  # outer -> inner
            if op_type == 'select':
                ops.append(f"\\select_{{{params}}}")

            elif op_type == 'project':
                # Prefer radb-style without extra spaces
                attr_str = ",".join(str(a) for a in params)
                ops.append(f"\\project_{{{attr_str}}}")

            elif op_type == 'rename':
                relname, attrnames = params

                # Debug: handle all possible forms of '*' in attrnames
                # The radb parser might store it as: '*', ['*'], ('*',), or other forms
                
                # Check if this represents the "all attributes" wildcard
                is_wildcard = False
                if attrnames == '*':
                    is_wildcard = True
                elif isinstance(attrnames, str) and attrnames.strip() == '*':
                    is_wildcard = True
                elif isinstance(attrnames, (list, tuple)):
                    if len(attrnames) == 1:
                        elem = attrnames[0]
                        if elem == '*' or (isinstance(elem, str) and elem.strip() == '*'):
                            is_wildcard = True
                    # Also check the exact list/tuple forms
                    elif attrnames == ['*'] or attrnames == ('*',):
                        is_wildcard = True
                
                if is_wildcard:
                    ops.append(f"\\rename_{{{relname}:*}}")
                elif attrnames is None or attrnames == [] or attrnames == ():
                    # Alias-only rename: \rename_{P}
                    ops.append(f"\\rename_{{{relname}}}")
                else:
                    # Explicit attribute list: \rename_{P:a,b,c}
                    if isinstance(attrnames, str):
                        ops.append(f"\\rename_{{{relname}:{attrnames}}}")
                    else:
                        attr_strs = [str(a) for a in attrnames]
                        ops.append(f"\\rename_{{{relname}:{','.join(attr_strs)}}}")

        return " ".join(ops + [base])


def try_fold_chain(raquery):
    """Try to fold chains of Select/Project/Rename into a single ChainedOp."""
    operations = []
    current = raquery
    
    # Collect operations that can be folded
    while isinstance(current, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
        if isinstance(current, radb.ast.Select):
            operations.append(('select', current.cond))
        elif isinstance(current, radb.ast.Project):
            operations.append(('project', current.attrs))
        elif isinstance(current, radb.ast.Rename):
            operations.append(('rename', (current.relname, current.attrnames)))
        current = current.inputs[0]
    
    # If we collected multiple operations, create a ChainedOp
    if len(operations) >= 2:
        operations.reverse()  # Apply in original order
        return ChainedOp(operations, current)
    
    # Otherwise, recursively process children
    if isinstance(raquery, radb.ast.Select):
        return radb.ast.Select(raquery.cond, try_fold_chain(raquery.inputs[0]))
    elif isinstance(raquery, radb.ast.Project):
        return radb.ast.Project(raquery.attrs, try_fold_chain(raquery.inputs[0]))
    elif isinstance(raquery, radb.ast.Rename):
        return radb.ast.Rename(raquery.relname, raquery.attrnames, try_fold_chain(raquery.inputs[0]))
    elif isinstance(raquery, radb.ast.Join):
        return radb.ast.Join(try_fold_chain(raquery.inputs[0]), raquery.cond, try_fold_chain(raquery.inputs[1]))
    
    return raquery



class ChainedTask(RelAlgQueryTask):
    """Executes a chain of Select / Project / Rename in a single MapReduce job."""

    def requires(self):
        raquery = try_fold_chain(
            radb.parse.one_statement_from_string(self.querystring)
        )
        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
                optimize=self.optimize
            )
        ]

    def mapper(self, line):
        # Emit exactly one touch key per mapper instance
        if not hasattr(self, "_touched"):
            self._touched = True
            yield ("__touch__", None)

        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        raquery = try_fold_chain(
            radb.parse.one_statement_from_string(self.querystring)
        )

        def atom_value(x, tup):
            if isinstance(x, radb.ast.AttrRef):
                a = str(x)
                if a in tup:
                    return tup[a]
                suf = a.split('.')[-1]
                for k, v in tup.items():
                    if k.endswith('.' + suf) or k == suf:
                        return v
                return None

            v = x.val
            if isinstance(v, str) and len(v) >= 2 and (
                (v[0] == "'" and v[-1] == "'") or (v[0] == '"' and v[-1] == '"')
            ):
                return v[1:-1]

            if isinstance(x, radb.ast.RANumber):
                try:
                    return float(v) if '.' in str(v) else int(v)
                except:
                    return v

            return v

        def eval_cond(c, tup):
            if isinstance(c, radb.ast.ValExprBinaryOp):
                l = eval_cond(c.inputs[0], tup)
                r = eval_cond(c.inputs[1], tup)
                op = c.op
                if op == radb.ast.sym.EQ:
                    return l == r
                if op == radb.ast.sym.NE:
                    return l != r
                if op == radb.ast.sym.LT:
                    return l < r
                if op == radb.ast.sym.LE:
                    return l <= r
                if op == radb.ast.sym.GT:
                    return l > r
                if op == radb.ast.sym.GE:
                    return l >= r
                if op == radb.ast.sym.AND:
                    return l and r
                if op == radb.ast.sym.OR:
                    return l or r
                return False

            return atom_value(c, tup)

        current_tuple = json_tuple
        current_rel = relation

        for op_type, params in raquery.operations:

            # --- SELECT ---
            if op_type == 'select':
                if not eval_cond(params, current_tuple):
                    return

            # --- PROJECT ---
            elif op_type == 'project':
                out = {}
                for a in params:
                    a_name = a.name
                    for k, v in current_tuple.items():
                        if k.endswith("." + a_name) or k == a_name:
                            out[k] = v
                            break
                current_tuple = out

            # --- RENAME (FIXED) ---
            elif op_type == 'rename':
                relname, _ = params

                # If relname is None, do NOT rewrite keys
                if relname is not None:
                    renamed = {}
                    for k, v in current_tuple.items():
                        suffix = k.split('.', 1)[-1]
                        renamed[relname + "." + suffix] = v
                    current_tuple = renamed
                    current_rel = relname

        yield (current_rel, json.dumps(current_tuple, sort_keys=True))

    def reducer(self, key, values):
        # Ignore touch key
        if key == "__touch__":
            return

        seen = set()
        emitted = False

        for v in values:
            if v not in seen:
                seen.add(v)
                emitted = True
                yield (key, v)

        # Emit exactly one empty tuple if nothing passed the mapper
        if not emitted:
            yield ("__empty__", json.dumps({}))




# === snip: everything BELOW ChainedTask stays EXACTLY the same ===


class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment, optimize=self.optimize)

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond


        def lookup(attrref, tup):
            # try fully qualified first, else match by suffix
            a = str(attrref)
            if a in tup:
                return tup[a]
            suf = a.split('.')[-1]
            for k, v in tup.items():
                if k.endswith('.' + suf) or k == suf:
                    return v
            return None

        def collect_eq_pairs(c):
            if isinstance(c, radb.ast.ValExprBinaryOp):
                if c.op == radb.ast.sym.EQ:
                    l, r = c.inputs
                    if isinstance(l, radb.ast.AttrRef) and isinstance(r, radb.ast.AttrRef):
                        return [(l, r)]
                if c.op == radb.ast.sym.AND:
                    return collect_eq_pairs(c.inputs[0]) + collect_eq_pairs(c.inputs[1])
            return []

        vals = []
        for l, r in collect_eq_pairs(condition):
            v = lookup(l, json_tuple)
            if v is None:
                v = lookup(r, json_tuple)
            if v is not None:
                vals.append(str(v))

        # stable key for conjunctions
        if len(vals) == 1:
            k = vals[0]
        elif len(vals) > 1:
            k = json.dumps(vals, separators=(',', ':'))
        else:
            k = None

        if k is not None:
            yield (k, (relation, json_tuple))


    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)


        def atom_value(x, tup):
            # AttrRef
            if isinstance(x, radb.ast.AttrRef):
                a = str(x)
                if a in tup:
                    return tup[a]
                suf = a.split('.')[-1]
                for k, v in tup.items():
                    if k.endswith('.' + suf) or k == suf:
                        return v
                return None

            # literals / numbers (strip quotes like 'mushroom')
            v = x.val
            if isinstance(v, str) and len(v) >= 2 and ((v[0] == "'" and v[-1] == "'") or (v[0] == '"' and v[-1] == '"')):
                return v[1:-1]
            if isinstance(x, radb.ast.RANumber):
                try:
                    return float(v) if '.' in str(v) else int(v)
                except:
                    return v
            return v

        def eval_cond(c, tup):
            if isinstance(c, radb.ast.ValExprBinaryOp):
                l = eval_cond(c.inputs[0], tup)
                r = eval_cond(c.inputs[1], tup)
                op = c.op
                if op == radb.ast.sym.EQ:
                    return l == r
                if op == radb.ast.sym.NE:
                    return l != r
                if op == radb.ast.sym.LT:
                    return l < r
                if op == radb.ast.sym.LE:
                    return l <= r
                if op == radb.ast.sym.GT:
                    return l > r
                if op == radb.ast.sym.GE:
                    return l >= r
                if op == radb.ast.sym.AND:
                    return l and r
                if op == radb.ast.sym.OR:
                    return l or r
                return False
            # base
            return atom_value(c, tup)

        def rels(node):
            if isinstance(node, radb.ast.RelRef):
                return {node.rel}
            if isinstance(node, radb.ast.Rename):
                return {node.relname}
            if hasattr(node, 'inputs'):
                s = set()
                for i in node.inputs:
                    s |= rels(i)
                return s
            return set()

        left_r = rels(raquery.inputs[0])
        right_r = rels(raquery.inputs[1])

        left_t, right_t = [], []

        for rel, tup in values:
            # first use relation label if it matches
            if rel in left_r:
                left_t.append(tup)
                continue
            if rel in right_r:
                right_t.append(tup)
                continue

            # fallback by prefixes
            prefixes = {k.split('.')[0] for k in tup if '.' in k}
            if prefixes & left_r:
                left_t.append(tup)
            if prefixes & right_r:
                right_t.append(tup)

        if not left_t or not right_t:
            return

        out_rel = next(iter(left_r), "joined")
        seen = set()

        for l in left_t:
            for r in right_t:
                merged = {**l, **r}
                if eval_cond(raquery.cond, merged):
                    s = json.dumps(merged, sort_keys=True)
                    if s not in seen:
                        seen.add(s)
                        yield (out_rel, s)



class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond


        def atom_value(x, tup):
            if isinstance(x, radb.ast.AttrRef):
                a = str(x)
                if a in tup:
                    return tup[a]
                suf = a.split('.')[-1]
                for k, v in tup.items():
                    if k.endswith('.' + suf) or k == suf:
                        return v
                return None

            v = x.val
            if isinstance(v, str) and len(v) >= 2 and ((v[0] == "'" and v[-1] == "'") or (v[0] == '"' and v[-1] == '"')):
                return v[1:-1]
            if isinstance(x, radb.ast.RANumber):
                try:
                    return float(v) if '.' in str(v) else int(v)
                except:
                    return v
            return v

        def eval_cond(c):
            if isinstance(c, radb.ast.ValExprBinaryOp):
                l = eval_cond(c.inputs[0])
                r = eval_cond(c.inputs[1])
                op = c.op
                if op == radb.ast.sym.EQ:
                    return l == r
                if op == radb.ast.sym.NE:
                    return l != r
                if op == radb.ast.sym.LT:
                    return l < r
                if op == radb.ast.sym.LE:
                    return l <= r
                if op == radb.ast.sym.GT:
                    return l > r
                if op == radb.ast.sym.GE:
                    return l >= r
                if op == radb.ast.sym.AND:
                    return l and r
                if op == radb.ast.sym.OR:
                    return l or r
                return False
            return atom_value(c, json_tuple)

        if eval_cond(condition):
            yield (relation, json.dumps(json_tuple))



class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)


        new_name = raquery.relname
        renamed = {}
        for k, v in json_tuple.items():
            suffix = k.split('.', 1)[-1]
            renamed[new_name + "." + suffix] = v
        yield (new_name, json.dumps(renamed))



class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs


        out = {}
        for a in attrs:
            a_name = a.name if hasattr(a, "name") else str(a)
            if str(a) in json_tuple:
                out[str(a)] = json_tuple[str(a)]
                continue
            # match by suffix
            for k, v in json_tuple.items():
                if k.endswith("." + a_name) or k == a_name:
                    out[k] = v
                    break

        yield (json.dumps(out, sort_keys=True), None)


    def reducer(self, key, values):

        tup = json.loads(key)
        rel = "result"
        for k in tup:
            if "." in k:
                rel = k.split(".", 1)[0]
                break
        yield (rel, key)




if __name__ == '__main__':
    luigi.run()