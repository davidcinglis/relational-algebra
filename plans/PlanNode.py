from abc import ABCMeta, abstractmethod


class Plan:
    def __init__(self, head_node):
        self.head_node = head_node

    def execute(self):
        return self.head_node.execute()


class Relation:
    def __init__(self, schema, tuples):
        self.schema = schema
        self.tuples = tuples

    def addTuple(self, tuple):
        self.tuples.append(tuple)


class PlanNode:
    __metaclass__ = ABCMeta

    @abstractmethod

    def execute(self):
        pass


class ProjectNode(PlanNode):
    def __init__(self, schema, left_child):
        self.schema = schema
        self.left_child = left_child

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation(self.schema, [])
        for in_tuple in left_relation.tuples:
            out_tuple = []
            for attribute in self.schema:
                out_tuple.append(in_tuple[left_relation.schema.index(attribute)])
            out_relation.tuples.append(out_tuple)
        return out_relation


class SelectNode(PlanNode):
    def __init__(self, condition, arg1, arg2, left_child):
        self.condition = condition
        self.arg1 = arg1
        self.arg2 = arg2
        self.left_child = left_child

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation(left_relation.schema, [])
        arg1 = self.arg1
        arg2 = self.arg2
        for tuple in left_relation.tuples:
            if self.arg1 in left_relation.schema:
                arg1 = tuple[left_relation.schema.index(self.arg1)]
            if self.arg2 in left_relation.schema:
                arg2 = tuple[left_relation.schema.index(self.arg2)]
            if self.condition(arg1, arg2):
                out_relation.tuples.append(tuple)
        return out_relation


f = lambda x, y: x == y
test_schema = ["a", "b", "c"]
test_tuple_1 = ["1a", "1b", "1c"]
test_tuple_2 = ["2a", "2b", "2c"]
test_tuples = [test_tuple_1, test_tuple_2]

test_relation = Relation(test_schema, test_tuples)

project_schema = ["c"]
test_node = SelectNode(f, "a", "b")

print test_node.execute(test_relation, None).tuples