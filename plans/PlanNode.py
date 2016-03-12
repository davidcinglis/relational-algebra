from abc import ABCMeta, abstractmethod
import copy
import re
import sys

class Aggregation:
    """
    This class defines an aggregation object, used for storing and evaluating an aggregate function within a query.
    """

    def _aggregate_sum(self, input_relation, attribute):
        return sum(filter(None, [input_relation.attribute_value(attribute, tup) for tup in input_relation.tuples]))

    def _aggregate_min(self, input_relation, attribute):
        arr = filter(None, [input_relation.attribute_value(attribute, tup) for tup in input_relation.tuples])
        if arr:
            return min(filter(None, arr))
        else:
            return None

    def _aggregate_max(self, input_relation, attribute):
        arr = filter(None, [input_relation.attribute_value(attribute, tup) for tup in input_relation.tuples])
        if arr:
            return max(filter(None, arr))
        else:
            return None

    def _aggregate_count(self, input_relation, attribute):
        return len(filter(None, [input_relation.attribute_value(attribute, tup) for tup in input_relation.tuples]))

    def _aggregate_avg(self, input_relation, attribute):
        arr = filter(None, [input_relation.attribute_value(attribute, tup) for tup in input_relation.tuples])
        if arr:
            return sum(arr) / float(len(arr))

    function_mappings = {
        'sum': _aggregate_sum,
        'max': _aggregate_max,
        'min': _aggregate_min,
        'count': _aggregate_count,
        'avg': _aggregate_avg
    }

    def __init__(self, agg_function, attribute, result_name):
        """
        Aggregation object constructor
        :param agg_function: A string representing the aggregate function (sum, max, etc.)
        :param attribute: The attribute to be aggregated over
        :param result_name: The string name to give to the result of the aggregation function
        """
        self.agg_function = self.function_mappings[agg_function]
        self.attribute = attribute
        self.result_name = result_name

class Relation:
    """
    This class defines a relation object, used for storing and print out relations.
    """
    def __init__(self, schema, tuples, name):
        """
        Relation object constructor
        :param schema: array of strings, representing the name of each column in the relation
        :param tuples: array of arrays, representing the tuples of the relation
        :param name: string, the name of the relation
        """
        self.schema = schema
        self.tuples = tuples
        self.name = name

    def execute(self):
        return self

    def _print_delimiter(self, lengths):
        """
        prints the +--+--+--+ delimiter between output sections
        :param lengths: an array of ints, representing the length of each subsection
        """
        for length in lengths:
            print '+',
            print '-' * (length + 1),
        print '+'

    def _print_content(self, values, lengths):
        """
        prints a line of content, either the attribute headers or a tuple
        :param values: array of strings, the values to print on a line
        :param lengths: array of ints, the length of each section
        :return:
        """
        for i in range(len(values)):
            print "|",
            print values[i],
            print ' ' * (lengths[i] - len(str(values[i]))),
        print '|'


    def printOut(self):
        """
        Prints out the relation to the console
        """
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
        """
        Given an attribute and a tuple, returns the value of that attribute in the tuple
        """
        if attribute not in self.schema:
            sys.exit("Attribute value %s could not be found in schema" % attribute)

        return tup[self.schema.index(attribute)]

class PlanNode:
    """
    Abstract class for plan nodes.
    Right now it doesn't really do anything, but in theory you could put common code here, so it seems useful to have.
    """
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

        # relation name is the concatenation of the two input relation names
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
        # TODO: this method is pretty messy and hard to read, should be fixed up
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

        # keep track of which tuples are used, to make sure all tuples are used in an outer join
        used_right_tuples = []
        used_left_tuples = []

        # iterate through every tuple combination, checking for matches
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
    def __init__(self, schema, left_child, projections, args_lists):
        """

        :param schema: List of strings: The output schema
        :param left_child: Plan node: the left child
        :param projections: List of strings: the projection strings
        :param args_lists: List of lists of strings: the arguments to each projection
        """

        if len(schema) != len(projections) or len(schema) != len(args_lists):
            sys.exit("Invalid projection")

        self.schema = schema
        self.left_child = left_child
        self.projections = projections
        self.args_lists = args_lists

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation(self.schema, [], left_relation.name)

        # perform the projection for every tuple
        for in_tuple in left_relation.tuples:
            out_tuple = []

            # iterate through every projection in the input
            for attribute, projection, args in zip(self.schema, self.projections, self.args_lists):

                # iterate through every argument in the projection, replacing it with the tuple's attribute value
                for arg in args:

                    # regex checks for numbers, which must not be enclosed in quotes for exec
                    if re.match("[-+]?[0-9]*\.?[0-9]+", str(left_relation.attribute_value(arg, in_tuple))):
                        exec("%s = %s" % (arg, left_relation.attribute_value(arg, in_tuple)))

                    # but strings do need to be enclosed in quotes
                    else:
                        exec("%s = '%s'" % (arg, left_relation.attribute_value(arg, in_tuple)))

                # evaluate the projection and add it to the output tuple
                out_tuple.append(eval(projection))

            out_relation.tuples.append(out_tuple)

        return out_relation


class SelectNode(PlanNode):
    def __init__(self, predicate, args, left_child):
        """
        :param predicate: String: the raw select predicate
        :param args: List of strings: the arguments to the predicate
        :param left_child: Plan Node: the child node
        """
        self.predicate = predicate
        self.args = args
        self.left_child = left_child

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation(left_relation.schema, [], left_relation.name)

        for tup in left_relation.tuples:

            # replace all the arguments with the corresponding attribute value for the current tuple
            for arg in self.args:

                # regex checks for numbers, which must not be enclosed in quotes for exec
                if re.match("[-+]?[0-9]*\.?[0-9]+", str(left_relation.attribute_value(arg, tup))):
                    exec("%s = %s" % (arg, left_relation.attribute_value(arg, tup)))

                # strings must not be enclosed in quotes
                else:
                    exec("%s = '%s'" % (arg, left_relation.attribute_value(arg, tup)))

            # with the correct arguments assigned, evaluate the predicate
            if eval(self.predicate):
                out_relation.tuples.append(tup)

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

class AggregationNode(PlanNode):
    def __init__(self, left_child, grouping_attribute, aggregation):
        """
        :param left_child: Plan Node: The child node
        :param grouping_attribute: String: the attribute to be grouped on.
        :param aggregation: Aggregation: See above object definition
        """
        self.left_child = left_child
        self.aggregation = aggregation
        self.grouping_attribute = grouping_attribute

    def execute(self):
        left_relation = self.left_child.execute()
        out_relation = Relation([], [], left_relation.name)


        # simple aggregation case: just apply the aggregation function to the entire tuple set
        if not self.grouping_attribute:
            out_relation.schema = [self.aggregation.result_name]
            out_relation.tuples.append([self.aggregation.agg_function(self, left_relation, self.aggregation.attribute)])

        # make sure we have a valid grouping attribute
        elif self.grouping_attribute not in left_relation.schema:
            sys.exit("invalid grouping attribute given")

        # here we have to iterate over each distinct attribute value and apply the aggregate function to each one
        else:
            out_relation.schema = [self.grouping_attribute, self.aggregation.result_name]
            distinct_values = set([left_relation.attribute_value(self.grouping_attribute, tup) for tup in left_relation.tuples])

            for val in distinct_values:
                temp_relation = Relation(self.left_child.schema, [], self.left_child.name)
                temp_relation.tuples = [tup for tup in left_relation.tuples
                                        if left_relation.attribute_value(self.grouping_attribute, tup) == val]

                out_relation.tuples.append([val, self.aggregation.agg_function(self, temp_relation, self.aggregation.attribute)])

        return out_relation
