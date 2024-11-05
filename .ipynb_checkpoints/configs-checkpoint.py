# -*- coding: utf-8 -*-

import argparse

def parse_cmd_args():

    parser = argparse.ArgumentParser(description='Query Rewrite (Policy Tree Search)')

    # database
    parser.add_argument('--host', type=str, default='162.105.146.1', help='Host IP Address')
    parser.add_argument('--dbname', type=str, default='xxx', help='Database Name')
    parser.add_argument('--port', type=int, default=5432, help='Host Port Number')
    parser.add_argument('--user', type=str, default='xxx', help='Database User Name')
    parser.add_argument('--password', type=str, default='xxx', help='Database Password')
    parser.add_argument('--driver', type=str, default='org.postgresql.Driver', help='Calcite adapter')

    # dataset
    parser.add_argument('--sqls', type=str, default='dataset/valid_sqls.log', help='Input query file')
    parser.add_argument('--starter', type=int, default=0, help='Which id to start')
    parser.add_argument('--howmany', type=int, default=-1, help='How many ids to use, -1 to use all')

    # rewrite policy
    parser.add_argument('--policy', type=str, default='mcts', help='Policy: [default, mcts]')

    # parameters
    parser.add_argument('--mctssteps', type=int, default=100, help="MCTS #simulations")
    parser.add_argument('--mctsgamma', type=float, default=30, help='MCTS rate of explorations')

    # others
    parser.add_argument('--saveresult', type=str, default='', help='Result file path for analysis, default no file')
    parser.add_argument('--verbose', type=int, default=1, help='0 no print, 1 print result, 2 print ra')
    parser.add_argument('--records', type=str, default='', help='Recording the rewrite results')

    args = parser.parse_args()

    return args
