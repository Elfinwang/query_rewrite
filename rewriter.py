
import os
import sqlparse
import numpy as np
from rules import rule_list, pair_loop_rule, expression_rule, padding_length, abandon_list_loop_new, error_rules
from database import bad_cost
from mcts import MCTS_node

# BEGIN JAVA ENV CONFIG

import jpype as jp
from jpype.types import *
import jpype.imports

# Configure JAVA environment for JPype
base_dir = os.path.abspath(os.curdir)
local_lib_dir = os.path.join(base_dir, 'libs')

# following command should be executed outside the program
# _ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()
classpath = open(os.path.join(base_dir, 'classpath.txt'),
                 'r').readline().split(':')
classpath.extend([os.path.join(local_lib_dir, jar)
                  for jar in os.listdir(local_lib_dir)])

if not jp.isJVMStarted():
    jp.startJVM(jp.getDefaultJVMPath(), classpath=classpath)

#from org.apache.calcite.sql.fun import SqlMyOperatorTable
from org.apache.calcite.sql.fun import SqlStdOperatorTable
from java.util import Properties
from org.apache.calcite.config import CalciteConnectionProperty
from org.apache.calcite.rel.rules import FilterJoinRule, AggregateExtractProjectRule, FilterMergeRule
from org.apache.calcite.sql.dialect import PostgresqlSqlDialect, MysqlSqlDialect
from org.apache.calcite.plan.hep import HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder, HepRelVertex
from java.util import Iterator
from java.lang import String
from java.io import PrintWriter, StringWriter
from org.apache.calcite.rel.externalize import RelWriterImpl
from org.apache.calcite.rel import RelWriter
from org.apache.calcite.util import SourceStringReader
from org.apache.calcite.tools import FrameworkConfig, Frameworks, Planner, RelBuilderFactory
from org.apache.calcite.sql import SqlNode, SqlDialect
from org.apache.calcite.schema import SchemaPlus
from org.apache.calcite.rel.rel2sql import RelToSqlConverter
from org.apache.calcite.rel import RelRoot, RelNode
from org.apache.calcite.plan import RelOptUtil, RelOptRule
from org.apache.calcite.jdbc import CalciteConnection
from org.apache.calcite.adapter.jdbc import JdbcSchema
import org.apache.calcite.rel.rules as R
from org.postgresql import Driver as PostgreSQLDriver
from java.util import ArrayList, List
from java.sql import Connection, DriverManager
from javax.sql import DataSource

# END JAVA ENV CONFIG

# rel_node -> string
def get_string_ra(rel_node):
    return str(RelOptUtil.toString(rel_node))

# print rel_node
def print_ra(rel_node):
    print(get_string_ra(rel_node))

# print sql, info is a prefix string
def print_sql(info, sql, printed=False):
    if not printed:
        print(info, sql)

# print cost
def print_cost(cost, prefix=''):
    print(prefix+'cost: ', cost)

# parse PG SQL to Calcite SQL
def parse_quote(sql):
    new_sql = ""
    sql = str(sql)
    for token in sqlparse.parse(sql)[0].flatten():
        if token.ttype is sqlparse.tokens.Name and token.parent and not isinstance(token.parent.parent,
                                                                                   sqlparse.sql.Function):
            new_sql += '\"' + token.value + '\"'
        elif token.value != ';':
            new_sql += token.value
    return new_sql

# check ljust(padding_length)
def check_string(raw_sql):
    ss = []
    for token in sqlparse.parse(raw_sql)[0].flatten():
        if not (token.ttype is sqlparse.tokens.Name and token.parent and not isinstance(token.parent.parent,
                                                                                   sqlparse.sql.Function)):
            s = token.value
            if s[0] == "'" and s[-1] == "'":
                ss.append(s[1:-1])
    for s1 in ss:
        for s2 in ss:
            if s1 != s2 and s1.ljust(padding_length) == s2.ljust(padding_length):
                return None
    raw_strings = dict()
    for s in ss:
        raw_strings["'"+s.ljust(padding_length)+"'"] = "'"+s+"'"
    return raw_strings

# PG SQL -> Calcite RA -> Calcite SQL -> execute 'EXPLAIN ...' in PG (check)
def sql2ra2sql(db, rewriter, raw_sql):
    format_sql = parse_quote(raw_sql)
    old_ra = rewriter.SQL2RA(format_sql)
    old_sql = rewriter.RA2SQL(old_ra)
    old_cost = db.cost_estimation(old_sql)
    return old_sql, old_cost

# rewriter
class Rewriter():
    # refer to main.py to see how to set db and driver
    # raw_strings are generated in check_string()
    def __init__(self, db, driver, raw_strings):
        p = Properties()
        p.put(CalciteConnectionProperty.FUN, "postgresql")
        conn = DriverManager.getConnection('jdbc:calcite:', p)
        calcite_conn = conn.unwrap(CalciteConnection)
        root_schema = calcite_conn.getRootSchema()
        # database config
        data_source = JdbcSchema.dataSource("jdbc:postgresql://"+db.host+':'+str(db.port)+'/'+db.dbname,
                                            driver, db.user, db.passwd)
        schema = root_schema.add(db.dbname, JdbcSchema.create(
            root_schema, db.dbname, data_source, None, None))

        # You can replace SqlStdOperatorTable with SqlMyOperatorTable
        config = Frameworks.newConfigBuilder().defaultSchema(
            schema).operatorTable(SqlStdOperatorTable.instance()).build()

        planner = Frameworks.getPlanner(config)
        dialect = PostgresqlSqlDialect.DEFAULT

        # rule list, do not delete it!
        ruledir = jp.JPackage('org.apache.calcite.rel.rules')

        self.planner = planner
        self.dialect = dialect
        self.db = db
        self.sqlcost = {}
        self.raw_strings = raw_strings

    # SQL (after parse_quote()) -> Calcite RA
    def SQL2RA(self, sql):
        self.planner.close()
        self.planner.reset()
        sql_node = self.planner.parse(SourceStringReader(sql))
        sql_node = self.planner.validate(sql_node)
        rel_root = self.planner.rel(sql_node)
        rel_node = rel_root.project()
        return rel_node

    # Calcite RA -> SQL
    def RA2SQL(self, ra):
        converter = RelToSqlConverter(self.dialect)
        result = converter.visitRoot(ra)
        sql = str(result.asStatement().toSqlString(self.dialect).getSql())

        # string replacement to avoid bugs
        sql = sql.replace('MIN(TRUE)', 'TRUE')
        for s in self.raw_strings:
            sql = sql.replace(s, self.raw_strings[s])

        return sql

    # check some rules to avoid bugs
    def check_rule(self, old_sql, new_sql, rule):
        if rule in expression_rule:
            if old_sql.count("'") != new_sql.count("'"):
                return False
        if old_sql.count("CAST") != new_sql.count("CAST"):
            return False
        return True

    # rewrite the vertex with the rule
    # old_sql for check_rule()
    def vertex_rewrite(self, hep_planner, vertex, rule, old_sql):
        # rule list, do not delete it!
        ruledir = jp.JPackage('org.apache.calcite.rel.rules')

        try:
            rule_use = eval(rule)
            newVertex = hep_planner.applyRule(rule_use, vertex, True)
            if newVertex == None:
                return 0, None, None, None
            else:
                new_ra = hep_planner.buildFinalPlan()
                new_sql = self.RA2SQL(new_ra)
                
                if not self.check_rule(old_sql, new_sql, rule):
                    return 0, None, None, None
                    
                # The following checks are necessary for MySQL (optional for PG)
                if 'LATERAL' in new_sql:
                    # print('there is "LATERAL"!')
                    return 0, None, None, None
                if '* AS' in new_sql:
                    # print('there is "* AS"!')
                    return 0, None, None, None
                if 'IN ()' in new_sql:
                    # print('there is "IN ()"!')
                    return 0, None, None, None
                    
                # reuse the results of appeared SQLs
                if new_sql in self.sqlcost:
                    new_cost = self.sqlcost[new_sql]
                else:
                    new_cost = self.db.cost_estimation(new_sql)
                    self.sqlcost[new_sql] = new_cost
                    
                # cost estimation failure
                if new_cost == bad_cost:
                    return 0, None, None, None
                return 1, new_ra, new_sql, new_cost
        except:
            return 0, None, None, None


# Calcite rewrite
def default_rewrite(rewriter, old_sql):
    old_ra = rewriter.SQL2RA(old_sql)
    rule_collection = []
    ruledir = jp.JPackage('org.apache.calcite.rel.rules')
    program_builder = HepProgram.builder()
    rule_list_use = [rule for rule in rule_list if not rule in abandon_list_loop_new + error_rules]
    for rule in rule_list_use:
        rule_use = eval(rule)
        rule_collection.append(rule_use)
    program_builder.addMatchLimit(100)
    program_builder.addRuleCollection(rule_collection)
    hep_planner = HepPlanner(program_builder.build())
    hep_planner.setRoot(old_ra)
    new_ra = hep_planner.findBestExp()
    return rewriter.RA2SQL(new_ra)

# MCTS
def mcts_rewrite(rewriter, old_sql, old_cost, verbose, results=None, steps=30, gamma=30):
    program_builder = HepProgram.builder()

    mcts_rule_list = rule_list

    # init state
    old_ra = rewriter.SQL2RA(old_sql)
    root = MCTS_node(old_ra, old_sql, old_cost, None, -1, 0)
    nodes = dict()
    ra_str = get_string_ra(old_ra)
    nodes[ra_str] = root
    visited = set([ra_str])


    # Each step of MCTS
    for _ in range(steps):
        trace = [root]
        leaf = root.find_leaf(gamma, trace)
        if leaf is None:
            break

        # current state
        current_ra = leaf.ra
        current_sql = leaf.sql
        current_cost = leaf.cost
        
        last_rule_id = leaf.rules[0]
        if last_rule_id == -1:
            last_rule = ""
        else:
            last_rule = mcts_rule_list[last_rule_id]

        hep_planner = HepPlanner(program_builder.build())
        hep_planner.setRoot(current_ra)
        vertex_list = hep_planner.getGraphList()
        vertex_cnt = vertex_list.size()
        current_rules = []

        # enumerate all rewrites
        for i, rule in enumerate(mcts_rule_list):
            for j in range(vertex_cnt):
                
                hep_planner = HepPlanner(program_builder.build())
                hep_planner.setRoot(current_ra)
                vertex_list = hep_planner.getGraphList()
                vertex = vertex_list.get(j)

                is_rewritten, new_ra, new_sql, new_cost = rewriter.vertex_rewrite(
                    hep_planner, vertex, rule, current_sql)

                if is_rewritten == 1:
                    current_rules.append(i)
                    ra_str = get_string_ra(new_ra)
                    if not ra_str in nodes:
                        visited.add(ra_str)
                        # check loop
                        if new_cost < current_cost or not(rule == last_rule or set([rule, last_rule]) in pair_loop_rule):
                            # record it
                            current_node = MCTS_node(new_ra, new_sql, new_cost, leaf, i, max(old_cost-new_cost, 0))
                            nodes[ra_str] = current_node
                            leaf.children.append(current_node)
                    else:
                        if ra_str in nodes:
                            current_node = nodes[ra_str]
                            leaf.children.append(current_node)
                            current_node.parents.append(leaf)
                            current_node.rules.append(i)

        leaf.value = max([node.value for node in leaf.children] + [leaf.value])
        leaf.back_prop()
        for node in trace:
            node.visited += 1

    # find the best in history
    best = root
    for node in nodes.values():
        if node.cost < best.cost:
            best = node

    if verbose > 0:
        print('#nodes:', len(nodes))

    # record the trace
    if not results is None:
        q = best.back_prop(needq=True)
        result = []
        for id, info in enumerate(q):
            if info[0] == root:
                i = id
                break
        while i != -1:
            node = q[i][0]
            result.append((get_string_ra(node.ra), node.sql, node.cost, q[i][2]))
            i = q[i][1]
        results.append(result)

        if verbose > 0:
            print('depth:', len(result))

    return best.sql
    
# verbose: 0 no print, 1 print result, 2 print ra
# return: result, new_sql
# result for rewrite: 0 better, 1 no change, 2 error (return 2, raw_sql)
def rewrite(db, driver, raw_sql, policy, param, verbose=0, results=None):
    # check strings
    raw_strings = check_string(raw_sql)
    if raw_strings is None:
        if verbose > 0:
            print(raw_sql)
            print('Detected different strings with the same ljust('+str(padding_length)+'), not supported in Calcite!')
        return 2, raw_sql

    # validate sql on PG
    raw_cost = db.cost_estimation(raw_sql)
    if raw_cost == bad_cost:
        if verbose > 0:
            print(raw_sql)
            print('SQL not supported in PostgreSQL!')
        return 2, raw_sql

    rewriter = Rewriter(db, driver, raw_strings)

    # start rewrite
    raw_printed = False

    # to calcite default sql
    try:
        old_sql, old_cost = sql2ra2sql(db, rewriter, raw_sql)
        if old_cost == bad_cost:
            if verbose > 0:
                print_sql('PG: ', raw_sql, raw_printed)
                raw_printed = True
                print_sql('calcite: ', old_sql)
                print('calcite version not supported in PG!')
            return 2, raw_sql
    except:
        if verbose > 0:
            print('SQL not supported for Calcite!')
        return 2, old_sql

    if verbose > 0:
        if old_cost != raw_cost:
            print_sql('PG: ', raw_sql, raw_printed)
            raw_printed = True
            print_cost(raw_cost)
            print_sql('calcite: ', old_sql)
            print_cost(old_cost)
            print()
    if verbose > 1:
        print_sql('raw sql: ', raw_sql, raw_printed)
        raw_printed = True
        print_ra(rewriter.SQL2RA(old_sql))
        print_cost(old_cost)
        print()

    # rewrite, get new_sql
    if policy == 'default':
        new_sql = default_rewrite(rewriter, old_sql)
    elif policy == 'mcts':
        new_sql = mcts_rewrite(rewriter, old_sql, old_cost, verbose, 
            steps=param.mctssteps, gamma=param.mctsgamma,
            results=results)
    else:
        print(policy, "policy is not supported!")
        exit()

    # check new_sql
    try:
        new_cost = db.cost_estimation(new_sql)
        if new_cost == bad_cost:
            if verbose > 0:
                print('New SQL not supported in PostgreSQL!')
            return 2, raw_sql
    except:
        if verbose > 0:
            print('New SQL not supported in PostgreSQL!')
        return 2, raw_sql

    if new_cost < raw_cost:
        if verbose > 0:
            print_sql('raw sql: ', raw_sql, raw_printed)
            raw_printed = True
            print_sql('new sql: ', new_sql)
            if verbose > 1:
                print_ra(rewriter.SQL2RA(new_sql))
            print_cost(raw_cost, 'raw ')
            print_cost(new_cost, 'new ')
        return 0, new_sql
    else:
        if verbose > 0:
            print('NO CHANGE')
        return 1, old_sql
