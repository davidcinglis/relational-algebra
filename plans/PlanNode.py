from abc import ABCMeta, abstractmethod
import copy

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

    def attribute_value(self, attribute, tup):
        return tup[self.schema.index(attribute)]

class PlanNode:
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute(self):
        pass

    def _attribute_value(self, schema, attribute, tup):
        return tup[schema.index(attribute)]

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
    def __init__(self, left_child, right_child, is_left_outer, is_right_outer):
        self.left_child = left_child
        self.right_child = right_child
        self.is_left_outer = is_left_outer
        self.is_right_outer = is_right_outer

    def execute(self):
        left_relation = self.left_child.execute()
        right_relation = self.right_child.execute()

        # we figure out the names of the attributes shared by both children
        common_schema = []  # the attributes shared by both children
        duplicate_indices = []  # the indices of the duplicate attributes in the right schema
        for attribute in left_relation.schema:
            if attribute in right_relation.schema:
                common_schema.append(attribute)
                duplicate_indices.append(right_relation.schema.index(attribute))

        # our new schema is the left schema and all the non-shared attributes of the right schema
        new_right_schema = [x for x in right_relation.schema if x not in common_schema]
        out_relation = Relation(left_relation.schema + new_right_schema, [], "%s_%s" % (left_relation.name, right_relation.name))

        used_right_tuples = []
        used_left_tuples = []

        for left_tuple in left_relation.tuples:
            for right_tuple in right_relation.tuples:
                # check if every attribute in the common schema is equal across the two tuples
                if all(left_tuple[left_relation.schema.index(attr)] == right_tuple[right_relation.schema.index(attr)] for attr in common_schema):

                    used_right_tuples.append(right_tuple)
                    used_left_tuples.append(left_tuple)
                    # if so add that tuple to the output, excluding the attributes at the duplicate indices
                    out_tuple = left_tuple + [right_tuple[i] for i in range(len(right_tuple)) if i not in duplicate_indices]
                    out_relation.tuples.append(out_tuple)

        # add any tuples from the right relation that weren't already added
        if self.is_right_outer:
            unused_right_tuples = [tup for tup in right_relation.tuples if tup not in used_right_tuples]
            for tup in unused_right_tuples:
                out_tup = []
                for attribute in out_relation.schema:
                    out_tup.append(right_relation.attribute_value(attribute, tup) if attribute in right_relation.schema else None)
                out_relation.tuples.append(out_tup)

        # add any tuples from the left relation that weren't already added
        if self.is_left_outer:
            unused_left_tuples = [tup for tup in left_relation.tuples if tup not in used_left_tuples]
            for tup in unused_left_tuples:
                out_relation.tuples.append(tup + [None] * len(new_right_schema))

        return out_relation

class ProjectNode(PlanNode):
    # projections is a list of lambdas
    # args_lists is a list of lists of arguments, these will get passed into the corresponding lambda
    def __init__(self, schema, left_child, projections, args_lists):
        assert len(schema) == len(projections)
        assert len(schema) == len(args_lists)

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
    # condition is a lambda of two arguments that returns a boolean
    # arg1 and arg2 can be strings or numbers
    def __init__(self, condition, arg1, arg2, left_child):
        self.condition = condition
        self.arg1 = arg1
        self.arg2 = arg2
        self.left_child = left_child

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation(left_relation.schema, [], left_relation.name)
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

        assert (left_relation.schema == right_relation.schema)

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

        assert (left_relation.schema == right_relation.schema)

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

        assert (left_relation.schema == right_relation.schema)

        out_relation = Relation(left_relation.schema, [], "%s_%s" % (left_relation.name, right_relation.name))

        # add the tuples from the left relation that are not in the right relation
        out_relation.tuples += [tuple for tuple in left_relation.tuples if tuple not in right_relation.tuples]
        return out_relation
