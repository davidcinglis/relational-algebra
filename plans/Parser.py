import PlanNode as pn
import re
import sys


class Indices:
    """This object stores a set of indices.
        Usually used for tracking the location of a substring within a larger string."""

    def __init__(self, start_index, end_index):
        """
        Class constructor.
        :param start_index: the starting index
        :param end_index: the ending index
        """

        self.start_index = start_index
        self.end_index = end_index


class Parser:
    """This object is used for parsing relational algebra input strings.
        It stores a dictionary of relations and uses these relations to parse an input into an execution plan."""

    def __init__(self, relations):
        """
        Class constructor.
        :param relations: A dictionary of relation names to relation objects (defined in PlanNode.py)
        """

        self.relations = relations

    @staticmethod
    def tokenize_string(input_string):
        """
        This method takes an input string and generates an array of all the variable names in the array.
         A variable name must start with a letter and can contain only letters, numbers, periods, and underscores.
        The keywords 'and' and 'or' are filtered from the results as these are reserved for evaluation.
        A variable cannot be named 'and' or 'or'

        :param input_string: This string to be tokenized
        :return: An array of the variable tokens
        """

        keywords = ["and", "or"]

        # get rid of everything within quotes inside the string
        input_string = re.sub("\'[^\']*\'", "", input_string)
        results = re.findall("[A-Za-z][A-Za-z1-9_\.]*", input_string)
        return [token for token in results if token not in keywords]

    @staticmethod
    def extract_token(open_char, close_char, input_string):
        """
        This method extracts a substring enclosed by the specified brackets from the input string.

        :param open_char: The open bracket character to extract with
        :param close_char: The closed bracket character to extract with
        :param input_string: The input string to extract from
        :return: The extracted string
        """

        # handle the weird case of a null input string
        if len(input_string) == 0:
            sys.exit("expecting %s character but could not find in string" % open_char)


        ret_token = Indices(0, 0)
        index = 0

        # find first occurrence of open char in input string
        while input_string[index] != open_char:
            index += 1

            # make sure we don't go out of bounds
            if index >= len(input_string):
                sys.exit("expecting %s character but could not find in string" % open_char)

        ret_token.start_index = index + 1
        count = 1

        # use count to perform bracket matching; continue until open bracket closed successfully
        while count > 0:
            index += 1

            # make sure we don't go out of bounds
            if index >= len(input_string):
                sys.exit("expecting %s character but could not find in string" % close_char)

            # increment/decrement based on the brackets hit
            if input_string[index] == open_char:
                count += 1
            elif input_string[index] == close_char:
                count -= 1

        ret_token.end_index = index
        return ret_token


    @staticmethod
    def strip_parentheses(input_string):
        """
        This method strips all enclosing parentheses from an input string: (((hello))) -> hello
        :param input_string: The string to be stripped
        :return: The stripped string
        """

        working_string = input_string
        while True:
            # null string case (this should never come up...)
            if len(working_string) == 0:
                return working_string

            # first ensure the first and last chars are parentheses
            if working_string[0] != '(' or working_string[-1] != ')':
                return working_string

            # then ensure the first paren matches the last paren
            indices = Parser.extract_token('(', ')', working_string)
            if indices.end_index != len(working_string) - 1:
                return working_string

            # if we get this far, strip the parens from the string
            working_string = working_string[1:-1]



    @staticmethod
    def parse_attribute_name(input_string):
        """
        This method determines what the attribute name for the input string should be.
        If the string contains the 'AS' keyword, the attribute name is everything following the keyword.
        Otherwise the attribute name is simply the entire string.

        :param input_string: The string to be parsed
        :return: The attribute name for the input string
        """
        input_string = input_string.strip()
        if " AS " in input_string:
            return input_string[input_string.find(" AS ") + len(" AS "):]
        else:
            return input_string

    @staticmethod
    def generate_aggregation_object(input_string):
        """
        This method takes a string of the form 'sum(a) AS b' and parses it into an aggregation object.
        :param input_string: The string to be parsed
        :return: The generated aggregation object
        """
        input_string = input_string.strip()
        valid_agg_functions = ['sum', 'max', 'min', 'avg', 'count']

        # make sure we have a valid aggregate function
        agg_function = input_string[:input_string.find('(')].lower()
        if agg_function not in valid_agg_functions:
            sys.exit("invalid aggregate function specified")

        # grab the aggregation attribute from inside the parentheses
        agg_attribute_indices = Parser.extract_token('(', ')', input_string)
        agg_attribute = input_string[agg_attribute_indices.start_index:agg_attribute_indices.end_index]

        # make sure a result name was specified
        if " AS " not in input_string:
            sys.exit("aggregation result must be renamed")
        result_name = input_string[input_string.find(" AS ") + len(" AS "):].strip()

        return pn.Aggregation(agg_function, agg_attribute, result_name)

    def parse_select(self, input_string):
        """
        This method parses a select query into a plan node.

        :param input_string: The query string. Assumed to be a select query.

        :return: A SelectNode containing the parsed query
        """

        # parse the predicate first, chucking it and all previous text from the working string
        pred_token = self.extract_token('[', ']', input_string)
        pred_str = input_string[pred_token.start_index:pred_token.end_index]
        working_string = input_string[pred_token.end_index:]

        # next parse the relation from the working string
        relation_token = self.extract_token('(', ')', working_string)
        relation_str = working_string[relation_token.start_index:relation_token.end_index]
        relation_object = self.parse(relation_str)

        # use the predicate and relation to construct a select node
        return pn.SelectNode(pred_str, self.tokenize_string(pred_str), relation_object)

    def parse_project(self, input_string):
        """
        This method parses a project query into a plan node.
        :param input_string: The query string. Assumed to be a project query.
        :return: A ProjectNode containing the parsed query.
        """

        # grab the projection arguments from inside the square brackets
        projection_token = self.extract_token('[', ']', input_string)
        projection_str = input_string[projection_token.start_index:projection_token.end_index]

        # grab the input relation from inside the parentheses
        relation_token = self.extract_token('(', ')', input_string[projection_token.end_index:])
        relation_str = (input_string[projection_token.end_index:])[relation_token.start_index:relation_token.end_index]
        relation_object = self.parse(relation_str)

        # tokenize the projection arguments into an array and use this to generate the schema
        projection_attributes = projection_str.split(',')
        schema = [self.parse_attribute_name(attr) for attr in projection_attributes]

        # once the schema has been generated, strip the 'AS' clause from each argument
        for i in range(len(projection_attributes)):
            if ' AS ' in projection_attributes[i]:
                projection_attributes[i] = projection_attributes[i][:projection_attributes[i].find(' AS ')]

        # generate a list of arguments for each projection in the array
        projection_args = [self.tokenize_string(attribute) for attribute in projection_attributes]

        return pn.ProjectNode(schema, relation_object, projection_attributes, projection_args)

    def parse_aggregation(self, input_string):
        """
        This method parses an aggregate query.
        :param input_string: The input query. Assumed to be an aggregate query.
        :return: An AggregationNode object built from the query
        """

        # grab the aggregate function
        aggregate_indices = self.extract_token('[', ']', input_string)
        aggregate_expression = input_string[aggregate_indices.start_index:aggregate_indices.end_index]
        working_string = input_string[aggregate_indices.end_index + 1:]

        # grab the relation
        relation_indices = self.extract_token('(', ')', working_string)
        relation_expression = working_string[relation_indices.start_index:relation_indices.end_index]
        relation_object = self.parse(relation_expression)

        # build and return the object
        aggregation = Parser.generate_aggregation_object(aggregate_expression)
        return pn.AggregationNode(relation_object, None, aggregation)

    def parse_grouping(self, input_string):
        """
        This method parses a grouping/aggregation query.
        :param input_string: The grouping query.
        :return: An AggregatioNode object built from the query
        """
        # TODO: combine common code from parse_aggregation into a helper function

        # grab the grouping attributes
        grouping_indices = self.extract_token('[', ']', input_string)
        grouping_attribute = input_string[grouping_indices.start_index:grouping_indices.end_index]
        working_string = input_string[grouping_indices.end_index + 1:]

        # grab the aggregate function
        aggregate_indices = self.extract_token('[', ']', working_string)
        aggregate_expression = working_string[aggregate_indices.start_index:aggregate_indices.end_index]
        working_string = working_string[aggregate_indices.end_index + 1:]

        # grab the relation
        relation_indices = self.extract_token('(', ')', working_string)
        relation_expression = working_string[relation_indices.start_index:relation_indices.end_index]
        relation_object = self.parse(relation_expression)

        # build and return the object
        aggregation = Parser.generate_aggregation_object(aggregate_expression)
        return pn.AggregationNode(relation_object, grouping_attribute, aggregation)


    def parse_rename(self, input_string):
        # TODO
        return

    def parse_union(self, input_string):

        args = self.parse_infix(input_string)
        left_relation = self.parse(args[0])
        right_relation = self.parse(args[2])

        return pn.UnionNode(left_relation, right_relation)

    def parse_intersect(self, input_string):

        args = self.parse_infix(input_string)
        left_relation = self.parse(args[0])
        right_relation = self.parse(args[2])

        return pn.IntersectionNode(left_relation, right_relation)

    def parse_setdiff(self, input_string):

        args = self.parse_infix(input_string)
        left_relation = self.parse(args[0])
        right_relation = self.parse(args[2])

        return pn.SetDifferenceNode(left_relation, right_relation)

    def parse_crossjoin(self, input_string):
        args = self.parse_infix(input_string)
        left_relation = self.parse(args[0])
        right_relation = self.parse(args[2])

        return pn.CartesianProductNode(left_relation, right_relation)

    def parse_thetajoin(self, input_string):
        # TODO
        return

    def natural_join_helper(self, input_string, is_left_outer, is_right_outer):
        args = self.parse_infix(input_string)
        left_relation = self.parse(args[0])
        right_relation = self.parse(args[2])

        return pn.NaturalJoinNode(left_relation, right_relation, is_left_outer, is_right_outer)

    def parse_leftouter(self, input_string):
        return self.natural_join_helper(input_string, True, False)

    def parse_rightouter(self, input_string):
        return self.natural_join_helper(input_string, False, True)

    def parse_fullouter(self, input_string):
        return self.natural_join_helper(input_string, True, True)

    def parse_naturaljoin(self, input_string):
        return self.natural_join_helper(input_string, False, False)

    def parse_assignment(self, input_string):
        args = self.parse_infix(input_string)
        self.relations[args[0]] = self.parse(args[2]).execute()
        return self.relations[args[0]]

    # this maps prefix operator strings to the appropriate parser function
    prefix_parsers = {
        "SELECT": parse_select,
        "PROJECT": parse_project,
        "GROUPBY": parse_grouping,
        "RENAME": parse_rename,
        "AGGREGATE": parse_aggregation
    }

    # this maps infix operator strings to the appropriate parser function
    infix_parsers = {
        "UNION": parse_union,
        "CROSSJOIN": parse_crossjoin,
        "THETAJOIN": parse_thetajoin,
        "NATURALJOIN": parse_naturaljoin,
        "LEFTOUTERJOIN": parse_leftouter,
        "RIGHTOUTERJOIN": parse_rightouter,
        "FULLOUTERJOIN": parse_fullouter,
        "SETDIFF": parse_setdiff,
        "INTERSECT": parse_intersect,
        "<--": parse_assignment
    }

    def parse_infix(self, input_string):
        """
        This helper function parses an input string known to be in infix form into an [arg1, operator, arg2] array
        :param input_string: The string to be parsed
        :return: The array of argument tokens as strings: [arg1, operator, arg2]
        """
        output_strings = []
        working_string = input_string

        # nested first arg
        if input_string[0] == '(':
            indices = self.extract_token('(', ')', input_string)
            first_arg = input_string[indices.start_index: indices.end_index]
            output_strings.append(first_arg)
            working_string = input_string[indices.end_index + 1:]

        # simple first arg
        else:
            tokens = input_string.split(' ')
            first_arg = tokens[0]
            if first_arg not in self.relations:
                sys.exit("invalid relation: %s" % first_arg)
            output_strings.append(first_arg)
            working_string = working_string[working_string.find(' ') + 1:]

        # operator parsing
        working_string = working_string.strip()
        tokens = working_string.split(' ')
        operator = tokens[0]
        if operator not in self.infix_parsers:
            sys.exit("invalid infix operator: %s" % operator)
        output_strings.append(operator)

        # second arg parsing
        second_arg = working_string[working_string.find(' ') + 1:].strip()
        output_strings.append(second_arg)

        return output_strings

    def parse(self, input_string):
        """
        This is the entry function into the parser object. It takes an input string and passes it off
        to the appropriate parser.
        :param input_string: The query string to be parsed.
        :return: An execution plan for the query.
        """

        # strip spaces and redundant parentheses
        input_string = input_string.strip()
        input_string = Parser.strip_parentheses(input_string)

        tokens = input_string.split(' ')

        # simple relation case
        if len(tokens) == 1:
            if tokens[0] in self.relations:
                return self.relations[tokens[0]]
            else:
                sys.exit("invalid relation: %s" % tokens[0])

        # check for a nested relation expression (e.g. left side of a join)
        # in this case we know we must have an infix operator
        elif input_string[0] == '(':
            infix_args = self.parse_infix(input_string)
            return self.infix_parsers[infix_args[1]](self, input_string)

        # check for a prefix operator
        elif tokens[0] in self.prefix_parsers:
            return self.prefix_parsers[tokens[0]](self, input_string)

        # check for an infix operator
        elif tokens[1] in self.infix_parsers:
            return self.infix_parsers[tokens[1]](self, input_string)

        # every valid input will have either a prefix or infix operator, so at this point the input is invalid
        else:
            sys.exit("Could not match to a prefix or infix operator - check parentheses?")



