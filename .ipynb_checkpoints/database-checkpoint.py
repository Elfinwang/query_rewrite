import psycopg2
import sys
import json
import time
bad_cost = sys.maxsize

class PsqlDatabase():

    def __init__(self, args, verbose=0):
        self.conn = psycopg2.connect(database=args.dbname,  # tpch1x (0.1m, 10m), tpch100m (100m)
                                     user=args.user,
                                     password=args.password,
                                     host=args.host,
                                     port=args.port)
        self.host = args.host
        self.user = args.user
        self.passwd = args.password
        self.port = args.port
        self.dbname = args.dbname
        self.verbose = verbose

    # if is_actual_execute==False then only EXPLAIN
    def execute_sql(self, sql, is_actual_execute=False):
        fail = 1
        cur = self.conn.cursor()
        i = 0
        cnt = 3
        while fail == 1 and i < cnt:
            try:
                fail = 0
                if is_actual_execute:
                    cur.execute(sql)
                else:
                    cur.execute('explain (FORMAT JSON) '+sql)
            except Exception as e:
                if self.verbose > 0:
                    print(e)
                fail = 1
            res = []
            if fail == 0:
                res = cur.fetchall()
            i = i + 1

        self.conn.rollback()
        if fail == 1:
            if self.verbose > 0:
                print("SQL Execution Fatal!!")
            return 0, ''
        elif fail == 0:
            return 1, res

    # reserved
    def return_cursor(self):
        return self.conn.cursor()

    # query cost estimated by the optimizer
    def cost_estimation(self, sql):
        success, res = self.execute_sql(sql)
        # print(success, res)
        if success == 1:
            cost = res[0][0][0]['Plan']['Total Cost']
            return cost
        else:
            return bad_cost

    # reserved
    def get_result(self, sql):
        success, res = self.execute_sql(sql, True)
        # print(success, res)
        if success == 1:
            return res
        else:
            if self.verbose > 0:
                print('error when get result')
            return []
