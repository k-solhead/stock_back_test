"""insert_divide_to_db(db_file_name)
    株式分割・併合情報をデータベースに登録する
    
    「株マップ.com」株式分割・併合予定銘柄一覧（https://jp.kabumap.com/servlets/kabumap/Action?SRC=splitSch/base）
    から証券コードと権利落ち日をスクレイピングで入手し、divide_union_dataテーブルに登録する。
    
    <divide_union_dataテーブル>
        CREATE TABLE "divide_union_data" (
	        "code"	TEXT,
	        "date_of_right_allotment"	TEXT,
	        "before"	REAL,
	        "after"	REAL,
	        PRIMARY KEY("code","date_of_right_allotment")
        )

    Args:
        db_file_name (str): データベース（SQLite3）のファイル名

"""
from pyquery import PyQuery
import datetime
import sqlite3
import sys
import re

def new_divide_generator():
    url = 'https://jp.kabumap.com/servlets/kabumap/Action?SRC=splitSch/base'
    q = PyQuery(url)
    for c, d, r in zip(q.find('tbody > tr:nth-child(n + 1) > td:nth-child(2)').contents(),
                    q.find('tbody > tr:nth-child(n + 1) > td:nth-child(6)'),
                    q.find('tbody > tr:nth-child(n + 1) > td:nth-child(7)').contents()):
        print(c, d, r)
        date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
        rate = re.findall(r"\d+", r)
        before = rate[0]
        after = rate[1]
        print(c, date, before, after)
        yield (c, date, before, after)

def insert_divide_to_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = 'INSERT INTO divide_union_data(code,date_of_right_allotment,before,after) VALUES(?,?,?,?)'
        conn.executemany(sql, new_divide_generator())


if __name__ == "__main__":
  #args = sys.argv

  #insert_divide_to_db(args[1])
  new_divide_generator()