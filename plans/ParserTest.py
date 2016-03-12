import PlanNode as pn
import Parser as ps


def RelationTest():
    test_schema = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 3]
    test_tuple_2 = [4, 5, 6]
    test_tuple_3 = [7, 8, 9]

    test_tuples_1 = [test_tuple_1, test_tuple_2, test_tuple_3]

    test_relation_1 = pn.Relation(test_schema, test_tuples_1, "test1")

    test_parser = ps.Parser({"test1": test_relation_1})

    test_parser.parse("test1").printOut()


def SelectTest():
    test_query = "SELECT [a < c] (test1)"
    print test_query
    print "input table:"

    test_schema_1 = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 1]
    test_tuple_2 = [1, 2, 3]
    test_tuple_3 = [2, 2, 3]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2, test_tuple_3], "test1")
    test_relation_1.printOut()
    test_parser = ps.Parser({"test1": test_relation_1})

    print "output table:"
    test_parser.parse(test_query).execute().printOut()

def ProjectTest():
    test_query = "PROJECT [a + 2 AS a, b] (test1)"
    print test_query
    print "input table:"

    test_schema_1 = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 1]
    test_tuple_2 = [1, 2, 3]
    test_tuple_3 = [2, 2, 3]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2, test_tuple_3], "test1")
    test_relation_1.printOut()
    test_parser = ps.Parser({"test1": test_relation_1})

    print "output table:"
    test_parser.parse(test_query).execute().printOut()

def JoinTest():
    test_query = "(PROJECT [a + 2 AS a,b] (test1)) CROSSJOIN test2"
    test_schema_1 = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 1]
    test_tuple_2 = [1, 2, 3]
    test_tuple_3 = [2, 2, 3]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2, test_tuple_3], "test1")
    test_relation_2 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2, test_tuple_3], "test2")
    test_parser = ps.Parser({"test1": test_relation_1, "test2": test_relation_2})

    test_parser.parse(test_query).execute().printOut()

def SimpleAggregationTest():
    test_query = "AGGREGATE [sum(b) AS total_b] (test1)"
    test_schema_1 = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 1]
    test_tuple_2 = [1, 2, 3]
    test_tuple_3 = [2, 2, 3]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2, test_tuple_3], "test1")
    test_parser = ps.Parser({"test1": test_relation_1})

    test_parser.parse(test_query).execute().printOut()

def GroupingTest():
    test_query = "GROUPBY [c] AGGREGATE [sum(a) AS total_a] (test1)"
    test_schema_1 = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 1]
    test_tuple_2 = [1, 2, 3]
    test_tuple_3 = [2, 2, 3]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2, test_tuple_3], "test1")
    test_parser = ps.Parser({"test1": test_relation_1})

    test_parser.parse(test_query).execute().printOut()

RelationTest()
SelectTest()
ProjectTest()
JoinTest()
SimpleAggregationTest()
GroupingTest()