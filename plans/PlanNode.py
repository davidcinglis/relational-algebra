from abc import ABCMeta, abstractmethod
import copy


class Plan:
    def __init__(self, head_node):
        self.head_node = head_node

    def execute(self):
        return self.head_node.execute()


class Relation:
    def __init__(self, schema, tuples, name):
        self.schema = schema
        self.tuples = tuples
        self.name = name

    def addTuple(self, tuple):
        self.tuples.append(tuple)

    def execute(self):
        return self

    # prints the +--+--+--+ delimiter between output sections
    def _print_delimiter(self, lengths):
        for length in lengths:
            print '+',
            print '-' * (length + 1),
        print '+'

    # prints a line of content, either the attribute headers or a tuple
    def _print_content(self, values, lengths):
        for i in range(len(values)):
            print "|",
            print values[i],
            print ' ' * (lengths[i] - len(str(values[i]))),
        print '|'

    def printOut(self):
        # calculating the length needed for each section
        lengths = []
        for i in range(len(self.schema)):
            max_length = len(str(self.schema[i]))
            for tuple in self.tuples:
                max_length = max(max_length, len(str(tuple[i])))
            lengths.append(max_length)

        # print out the attribute header
        self._print_delimiter(lengths)
        self._print_content(self.schema, lengths)
        self._print_delimiter(lengths)

        # print out all the tuples
        for tuple in self.tuples:
            self._print_content(tuple, lengths)

        self._print_delimiter(lengths)


class PlanNode:
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute(self):
        pass


class CartesianProductNode(PlanNode):
    def __init__(self, left_child, right_child):
        self.left_child = left_child
        self.right_child = right_child

    def execute(self):
        left_relation = self.left_child.execute()
        right_relation = self.right_child.execute()

        # schema is just the combination of the two schemas, prefixed with their respective relation names
        schema = ["%s.%s" % (left_relation.name, column) for column in left_relation.schema]
        schema.extend(["%s.%s" % (right_relation.name, column) for column in right_relation.schema])
        out_relation = Relation(schema, [], "%s_%s" % (left_relation.name, right_relation.name))

        # generate every combination of tuples and add to output
        for left_tuple in left_relation.tuples:
            for right_tuple in right_relation.tuples:
                out_relation.tuples.append(left_tuple + right_tuple)

        return out_relation


class NaturalJoinNode(PlanNode):
    def __init__(self, left_child, right_child):
        self.left_child = left_child
        self.right_child = right_child

    def execute(self):
        left_relation = self.left_child.execute()
        right_relation = self.right_child.execute()

        # we figure out the names of the attributes shared by both children
        common_schema = [] # the attributes shared by both children
        duplicate_indices = [] # the indices of the duplicate attributes in the right schema
        for attribute in left_relation.schema:
            if attribute in right_relation.schema:
                common_schema.append(attribute)
                duplicate_indices.append(right_relation.schema.index(attribute))

        # our new schema is the left schema and all the non-shared attributes of the right schema
        new_right_schema = [x for x in right_relation.schema if x not in common_schema]
        out_relation = Relation(left_relation.schema + new_right_schema, [], "%s_%s" % (left_relation.name, right_relation.name))

        for left_tuple in left_relation.tuples:
            for right_tuple in right_relation.tuples:
                # check if every attribute in the common schema is equal across the two tuples
                if all(left_tuple[left_relation.schema.index(attr)] == right_tuple[right_relation.schema.index(attr)] for attr in common_schema):
                    # if so add that tuple to the output, excluding the attributes at the duplicate indices
                    out_tuple = left_tuple + [right_tuple[i] for i in range(len(right_tuple)) if i not in duplicate_indices]
                    out_relation.tuples.append(out_tuple)
        return out_relation


class ProjectNode(PlanNode):
    def __init__(self, schema, left_child, projections, args_lists):
        self.schema = schema
        self.left_child = left_child
        self.projections = projections
        self.args_lists = args_lists

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation(self.schema, [], left_relation.name)
        for in_tuple in left_relation.tuples:
            out_tuple = []
            for attribute, projection, args in zip(self.schema, self.projections, self.args_lists):

                # if the argument is an attribute name, replace that argument with the tuple's value for that attribute
                args_copy = copy.copy(args)
                for i in range(len(args)):
                    if args[i] in left_relation.schema:
                        args_copy[i] = in_tuple[left_relation.schema.index(args[i])]

                # apply the projection lambda to the arguments and add the result to the output tuple
                out_tuple.append(projection(*args_copy))

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
        arg1 = copy.copy(self.arg1)
        arg2 = copy.copy(self.arg2)
        for tuple in left_relation.tuples:

            # if the argument is an attribute name, replace that argument with the tuple's value for that attribute
            if self.arg1 in left_relation.schema:
                arg1 = tuple[left_relation.schema.index(self.arg1)]
            if self.arg2 in left_relation.schema:
                arg2 = tuple[left_relation.schema.index(self.arg2)]

            # pass those arguments into the condition lambda, and add the tuple if the condition is fulfilled
            if self.condition(arg1, arg2):
                out_relation.tuples.append(tuple)

        return out_relation


class UnionNode(PlanNode):
    def __init__(self, left_child, right_child):
        self.left_child = left_child
        self.right_child = right_child

    def execute(self):
        left_relation = self.left_child.execute()
        right_relation = self.right_child.execute()

        out_relation = Relation(left_relation.schema, [], "%s_%s" % (left_relation.name, right_relation.name))
        out_relation.tuples += left_relation.tuples

        # add the tuples from the right relation that haven't already been added
        out_relation.tuples += [tuple for tuple in right_relation.tuples if tuple not in left_relation.tuples]
        return out_relation


class IntersectionNode(PlanNode):
    def __init__(self, left_child, right_child):
        self.left_child = left_child
        self.right_child = right_child

    def execute(self):
        left_relation = self.left_child.execute()
        right_relation = self.right_child.execute()

        out_relation = Relation(left_relation.schema, [], "%s_%s" % (left_relation.name, right_relation.name))

        # add the tuples from the left relation that are also in the right relation
        out_relation.tuples += [tuple for tuple in left_relation.tuples if tuple in right_relation.tuples]
        return out_relation

class SetDifferenceNode(PlanNode):
    def __init__(self, left_child, right_child):
        self.left_child = left_child
        self.right_child = right_child

    def execute(self):
        left_relation = self.left_child.execute()
        right_relation = self.right_child.execute()

        out_relation = Relation(left_relation.schema, [], "%s_%s" % (left_relation.name, right_relation.name))

        # add the tuples from the left relation that are not in the right relation
        out_relation.tuples += [tuple for tuple in left_relation.tuples if tuple not in right_relation.tuples]
        return out_relation


# temporary test code
f = lambda x, y: x + y
g = lambda x: x
test_schema_1 = ["a", "b", "c"]
test_schema_2 = ["b", "c", "f"]
test_tuple_1 = [1, 2, 3]
test_tuple_x = [4, 5, 7]
test_tuple_2 = [2, 3, 68]
test_tuple_3 = [5, 7, 69]
test_tuples = [test_tuple_1, test_tuple_x]
test_tuples2 = [test_tuple_2, test_tuple_1]

test_relation = Relation(test_schema_1, test_tuples, "test1")
test_relation2 = Relation(test_schema_1, test_tuples2, "test2")

node = SetDifferenceNode(test_relation, test_relation2).execute()
node.printOut()
