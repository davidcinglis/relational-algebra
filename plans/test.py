import PlanNode as pn

# Runs a list of tests, printing the number of successes and the number of total tests
def runTests():
    total_tests = 0
    successes = 0
    tests = [SetDifferenceTest, LeftOuterJoinTest, RightOuterJoinTest, UnionTest, \
             IntersectionTest, CartesianProductTest, ProjectTest, SelectTest]
    for t in tests:
        total_tests += 1
        successes += t()

    print "\nran %d tests" % total_tests
    print "passed %d tests" % successes


# Executes a given plan node and prints a comparison of its output against the expected output
def test(test_name, node, expected_output):
    print "Running test: " + test_name
    print "Expected output is: "
    expected_output.printOut()
    print "Actual output is: "
    output = node.execute()
    output.printOut()
    output.tuples.sort()
    expected_output.tuples.sort()
    if output.tuples == expected_output.tuples and output.schema == expected_output.schema:
        print "The outputs match!\n"
        return True
    else:
        print "The outputs do not match.\n"
        return False


def IntersectionTest():
    test_schema = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 3]
    test_tuple_2 = [4, 5, 6]
    test_tuple_3 = [7, 8, 9]

    test_tuples_1 = [test_tuple_1, test_tuple_2]
    test_tuples_2 = [test_tuple_2, test_tuple_3]

    test_relation_1 = pn.Relation(test_schema, test_tuples_1, "test1")
    test_relation_2 = pn.Relation(test_schema, test_tuples_2, "test2")
    expected_output_relation = pn.Relation(test_schema, [test_tuple_2], "expected_output")
    test_node = pn.IntersectionNode(test_relation_1, test_relation_2)
    return test("Intersection Test 1", test_node, expected_output_relation)


def SetDifferenceTest():
    test_schema = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 3]
    test_tuple_2 = [4, 5, 6]
    test_tuple_3 = [7, 8, 9]

    test_tuples_1 = [test_tuple_1, test_tuple_2]
    test_tuples_2 = [test_tuple_3]

    test_relation_1 = pn.Relation(test_schema, test_tuples_1, "test1")
    test_relation_2 = pn.Relation(test_schema, test_tuples_2, "test2")
    expected_output_relation = pn.Relation(test_schema, [test_tuple_2, test_tuple_1], "expected_output")
    test_node = pn.SetDifferenceNode(test_relation_1, test_relation_2)
    return test("Set Difference Test 1", test_node, expected_output_relation)


def UnionTest():
    test_schema = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 3]
    test_tuple_2 = [4, 5, 6]
    test_tuple_3 = [7, 8, 9]

    test_tuples_1 = [test_tuple_1, test_tuple_2]
    test_tuples_2 = [test_tuple_2, test_tuple_3]

    test_relation_1 = pn.Relation(test_schema, test_tuples_1, "test1")
    test_relation_2 = pn.Relation(test_schema, test_tuples_2, "test2")
    expected_output_relation = pn.Relation(test_schema, [test_tuple_1, test_tuple_2, test_tuple_3], "expected_output")
    test_node = pn.UnionNode(test_relation_1, test_relation_2)
    return test("Union Test 1", test_node, expected_output_relation)

def CartesianProductTest():
    test_schema_1 = ["a", "b"]
    test_schema_2 = ["c", "d"]

    test_tuple_1 = [1, 2]
    test_tuple_2 = [3, 4]
    test_tuple_3 = ["hello", "world"]
    test_tuple_4 = ["goodnight", "moon"]

    test_tuples_1 = [test_tuple_1, test_tuple_2]
    test_tuples_2 = [test_tuple_3, test_tuple_4]

    test_relation_1 = pn.Relation(test_schema_1, test_tuples_1, "t1")
    test_relation_2 = pn.Relation(test_schema_2, test_tuples_2, "t2")
    expected_output_relation = pn.Relation(["t1.a", "t1.b", "t2.c", "t2.d"], [[1, 2, "hello", "world"], [1, 2, "goodnight", "moon"], \
                                                [3, 4, "hello", "world"], [3, 4, "goodnight", "moon"]], "expected_output")
    test_node = pn.CartesianProductNode(test_relation_1, test_relation_2)
    return test("Cartesian Product Test 1", test_node, expected_output_relation)

def LeftOuterJoinTest():
    test_schema_1 = ["a", "b"]
    test_schema_2 = ["b", "c"]
    test_tuple_1 = [1, 2]
    test_tuple_2 = [2, 3]
    test_tuple_3 = [5, 6]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_3], "test1")
    test_relation_2 = pn.Relation(test_schema_2, [test_tuple_2], "test2")
    expected_output_relation = pn.Relation(["a", "b", "c"], [[1, 2, 3], [5, 6, None]], "expected_output")
    test_node = pn.NaturalJoinNode(test_relation_1, test_relation_2, True, False)
    return test("Left Outer Join Test 1", test_node, expected_output_relation)


def RightOuterJoinTest():
    test_schema_1 = ["a", "b"]
    test_schema_2 = ["b", "c"]
    test_tuple_1 = [1, 2]
    test_tuple_2 = [2, 3]
    test_tuple_3 = [5, 6]
    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_3], "test1")
    test_relation_2 = pn.Relation(test_schema_2, [test_tuple_2], "test2")
    expected_output_relation = pn.Relation(["b", "c", "a"], [[2, 3, 1], [6, None, 5]], "expected_output")
    test_node = pn.NaturalJoinNode(test_relation_2, test_relation_1, False, True)
    return test("Right Outer Join Test 1", test_node, expected_output_relation)

def ProjectTest():
    test_schema_1 = ["a", "b", "c"]
    project_schema = ["a", "c"]
    project_lambda = lambda x: x * 2

    test_tuple_1 = [1, 2, 3]

    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1], "test1")
    expected_output_relation = pn.Relation(["a", "c"], [[2, 6]], "expected_output")
    test_node = pn.ProjectNode(project_schema, test_relation_1, [project_lambda, project_lambda], [["a"], ["c"]])
    return test("Generalized Project Test 1", test_node, expected_output_relation)


def SelectTest():
    test_schema_1 = ["a", "b", "c"]
    test_tuple_1 = [1, 2, 1]
    test_tuple_2 = [1, 2, 3]
    condition_lambda = lambda x, y: x == y

    test_relation_1 = pn.Relation(test_schema_1, [test_tuple_1, test_tuple_2], "test1")
    expected_output_relation = pn.Relation(["a", "b", "c"], [test_tuple_1], "expected_output")
    test_node = pn.SelectNode(condition_lambda, "a", "c", test_relation_1)
    return test("Select Test 1", test_node, expected_output_relation)

runTests()

