import ibis
import functools
import ibis.expr.operations as ops
import ibis.expr.datatypes as dt


from egg import EGraph, Rewrite, Var, vars


zero = ops.Literal(0, dtype=dt.int64)
one = ops.Literal(1, dtype=dt.int64)
two = ops.Literal(2, dtype=dt.float64)

x_, y_, z_, table, preds1, preds2, distinct = vars("x y z table preds1 preds2 distinct")


rules = [
    Rewrite(ops.Add.pattern(zero, x_), x_, name="add-0"),
    Rewrite(
        ops.Selection.pattern(
            table=table,
            selections=x_,
            predicates=ops.NodeList.pattern(),
            sort_keys=ops.NodeList.pattern(),
        ),
        table,
        name="selection-0",
    ),
    Rewrite(ops.NodeList.pattern(x_, y_, z_), x_, name="node-list"),
]


def pina(klass, args):
    print(type(klass), args)


def simplify(expr, iters=7):
    assert isinstance(expr, ops.Node), "nodes only fool"
    egraph = EGraph(pina)
    egraph.add(expr)
    egraph.run(rules, iters)
    best = egraph.extract(expr)
    return best


def test_ibis():
    assert simplify(ops.Add(zero, two)) == two
    assert simplify(ops.Add(zero, ops.Add(zero, two))) == two

    print(simplify(ops.NodeList(zero, one, two)))


def test_union_all_to_or():
    t = ibis.table(dict(a="int"), name="t")
    expr = t[t]
    print(expr.op())
    result = simplify(expr.op())
    print(result)
    # assert result == t.op()
