"""
This module reproduces following error:
OperationalError: (sqlite3.OperationalError) database is locked
when 2 separate threads issues select statements,
there is no locking issues.

when at least one of the threads issues modification (DML) statement,
database becomes locked.
"""
import datetime
import logging
import os
import sqlalchemy as sa
import multiprocessing as mp
import pandas as pd

engine = sa.create_engine('sqlite://')  # Default to an in-memory DB
meta = sa.MetaData()

tableA = sa.Table('A', meta,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('age', sa.Integer, nullable=False))

tableB = sa.Table('B', meta,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('salary', sa.Integer))

def initialize_db(db_url):
  global engine
  logging.info("db dir is {0}".format(os.getcwd()))
  logging.info("db name is {0}".format(db_url))
  engine = sa.create_engine(db_url, connect_args={'timeout': 0})
  meta.create_all(engine)


def run_first():
    x = 1
    while x <= 1000:
        pd.DataFrame({'id':[x], 'age':[x*10]}).to_sql(
            'A',
            engine,
            if_exists='append',
            index=False)
        x += 1

def run_second():
    x = 1
    while x <= 1000:
        pd.read_sql('B', engine)
        x += 1

initialize_db('sqlite:////tmp/tempo.db')
server_process = mp.Process(target=run_first)
worker_process = mp.Process(target=run_second)

server_process.start()
worker_process.start()
server_process.join()
worker_process.join()
