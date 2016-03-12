import PlanNode as pn
import Parser as ps

employee_schema = ["person_name", "street", "city"]
works_schema = ["person_name", "company_name", "salary"]
company_schema = ["company_name", "city"]
manages_schema = ["person_name", "manager_name"]


employee_tuples = [["Brad Pitt", "First Avenue", "Pasadena"],
                   ["Jennifer Lawrence", "Penny Lane", "Hollywood"],
                   ["George Clooney", "Abbey Road", "Seattle"],
                   ["George Lucas", "Lake Avenue", "Dallas"]]

works_tuples = [["Brad Pitt", "First Bank Corporation", 20000],
                ["Jennifer Lawrence", "First Bank Corporation", 5000],
                ["George Lucas", "Lucasfilm", 1000000],
                ["George Clooney", "Google", 12345]]

company_tuples = [["First Bank Corporation", "Pasadena"],
                  ["Lucasfilm", "Dallas"],
                  ["Google", "Mountain View"]]

manages_tuples = [["Brad Pitt", "Jennifer Lawrence"],
                  ["George Lucas", "Brad Pitt"]]

employee_relation = pn.Relation(employee_schema, employee_tuples, "employee")
works_relation = pn.Relation(works_schema, works_tuples, "works")
company_relation = pn.Relation(company_schema, company_tuples, "company")
manages_relation = pn.Relation(manages_schema, manages_tuples, "manages")

test_parser = ps.Parser({"employee" : employee_relation,
                         "works" : works_relation,
                         "company" : company_relation,
                         "manages" : manages_relation})


def run_query(query_string):
    print ""
    print query_string
    test_parser.parse(query_string).execute().printOut()

p1_a = "PROJECT [person_name] (SELECT [company_name == 'First Bank Corporation'] (works))"
p1_b = "PROJECT [person_name, city] (SELECT [company_name == 'First Bank Corporation'] (works NATURALJOIN employee))"
p1_c = "PROJECT [person_name, street, city] (SELECT [company_name == 'First Bank Corporation' and salary > 10000] (works NATURALJOIN employee))"
p1_d = "PROJECT [person_name] ((employee NATURALJOIN works) NATURALJOIN company)"

p2_a = "works <-- (PROJECT [person_name, company_name, salary * 1.1 AS salary] (SELECT [company_name == 'First Bank Corporation'] (works))) UNION (SELECT [company_name != 'First Bank Corporation'] (works))"
#p2_a = "((SELECT [company_name == 'First Bank Corporation'] (works)) UNION (SELECT [company_name != 'First Bank Corporation'] (works)))"
p2_b = "works <-- (PROJECT [person_name, company_name, salary * 1.1 AS salary] (SELECT [salary * 1.1 <= 100000] (manages NATURALJOIN works))) UNION (PROJECT [person_name, company_name, salary * 1.03 AS salary] (SELECT [salary * 1.1 > 100000] (manages NATURALJOIN works)))"



test_query = "GROUPBY [company_name] AGGREGATE [sum(salary) AS sal_sum] (works)"

run_query("employee")
run_query("works")
run_query("company")
run_query("manages")

run_query(p1_a)
run_query(p1_c)
run_query(p1_d)
run_query(test_query)
run_query(p2_a)
run_query("works")

