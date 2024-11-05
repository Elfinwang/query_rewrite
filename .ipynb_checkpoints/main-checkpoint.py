#!/usr/bin/env python

import os
from configs import parse_cmd_args
from database import PsqlDatabase as Database
#from database import Database
from rewriter import rewrite
import time
import pickle

# Load the SQL dataset
def load_sqls(fname, starter, howmany):
    f = open(fname, 'r')
    sql_list = [l.strip() for l in f.readlines()]
    f.close()
    print('SQL file:', fname)

    if howmany < 0:
        print('Total sql number:', len(sql_list))
        return sql_list[starter:]

    print('Starter:', starter)
    print('How many:', howmany)
    if starter+howmany < len(sql_list):
        return sql_list[starter:starter+howmany]
    else:
        print('Total sql number (< starter + howmany):', len(sql_list))
        return sql_list[starter:]

# database config (avoid passing args to rewrite())
class DBConfig():
    def __init__(self, host, user, password, port, dbname):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.dbname = dbname

# parameters for rewrite algorithms (avoid passing args to rewrite())
class Parameters():
    def __init__(self, mctssteps, mctsgamma):
        self.mctssteps = mctssteps
        self.mctsgamma = mctsgamma

def main():
    # parse args
    args = parse_cmd_args()

    # initialize DB
    # not print error msg. (not use args.verbose)
    dbconfig = DBConfig(host=args.host, user=args.user, password=args.password, 
        port=args.port, dbname=args.dbname)
    driver = args.driver
    db = Database(dbconfig, verbose=0)

    # do not delete the following lines (for JAVA)
    base_dir = os.path.abspath(os.curdir)
    local_lib_dir = os.path.join(base_dir, 'libs')

    # load dataset
    sql_list = load_sqls(args.sqls, args.starter, args.howmany)
    print('#SQLs to rewrite:', len(sql_list))

    # print policy and parameters
    policy = args.policy
    param = Parameters(mctssteps=args.mctssteps, mctsgamma=args.mctsgamma)

    print('Policy:', policy)
    if policy == 'mcts':
        print('Steps', param.mctssteps)
        print('Gamma', param.mctsgamma)

    changed_sqls = []
    wrong_sqls = []
    results = None
    saveresult = args.saveresult
    if saveresult != '':
        results = []
    verbose = int(args.verbose)
    records = args.records
    recording = []

    # start rewrite
    print('======================')
    begin_time = time.time()

    for cnt, sql in enumerate(sql_list):  
        print('# ', cnt)
        begin_time_sql = time.time()
        result, new_sql = rewrite(db=db, driver=driver, raw_sql=sql, policy=policy, param=param, 
            verbose=verbose, results=results)
        end_time_sql = time.time()
        if records != '':
            if result == 0:
                raw_cost = db.cost_estimation(sql)
                new_cost = db.cost_estimation(new_sql)
                recording.append((sql, new_sql, raw_cost, new_cost))
            else:
                recording.append(('', '', -1, -1))
        print('SQL rewrite time:', end_time_sql-begin_time_sql)
        if result == 0:
            changed_sqls.append(cnt)
        elif result == 2:
            wrong_sqls.append(cnt)
        print('======================')
    
    end_time = time.time()
    print('Total execution time:', end_time-begin_time)

    if saveresult != '':
        if len(results) > 0:
            pickle.dump(results, open(saveresult, 'wb'))
    
    if records != '':
        pickle.dump(recording, open(records, 'wb'))

    print('changed queries:', len(changed_sqls))
    print(changed_sqls)
    print('wrong queries:', len(wrong_sqls))
    print(wrong_sqls)

if __name__ == "__main__":
    main()