"""insert_divide_to_db(db_file_name)
    新しい株式分割・併合情報をデータベースに登録する
    
    「松井証券」の銘柄情報－株式分割（https://ca.image.jp/matsui/）
                 銘柄情報－株式併合(https://ca.image.jp/matsui/?type=5)
    から証券コードと権利付最終日、比率をスクレイピングで入手し、divide_union_dataテーブルに登録する。
    
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

def new_divide_generator():
  url1 = 'https://ca.image.jp/matsui/'
  q = PyQuery(url1)
  for dd, dc, db, da in zip(q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(1)'),
                            q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(2)').contents(),
                            q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(5)').contents(),
                            q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(7)').contents()):
    ddate = datetime.datetime.strptime(dd.text, '%Y-%m-%d').date()
    db = float(db)
    da = float(da)
    print(dc, ddate, db, da)
    yield (dc, ddate, db, da)

def new_union_generator():
  url2 = 'https://ca.image.jp/matsui/?type=5'
  q = PyQuery(url2)
  for ud, uc, ub, ua in zip(q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(1)'),
                            q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(2)').contents(),
                            q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(5)').contents(),
                            q.find('table.commontbl > tbody > tr:nth-child(n + 1) > td:nth-child(7)').contents()):
    udate = datetime.datetime.strptime(ud.text, '%Y-%m-%d').date()
    ub = float(ub)
    ua = float(ua)
    print(uc, udate, ub, ua)
    yield (uc, udate, ub, ua)


def insert_divide_to_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        try:
            sql = 'INSERT INTO divide_union_data(code,date_of_right_allotment,before,after) VALUES(?,?,?,?)'
            conn.executemany(sql, new_divide_generator())
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
        conn.commit
    with conn:
        try:
            sql = 'INSERT INTO divide_union_data(code,date_of_right_allotment,before,after) VALUES(?,?,?,?)'
            conn.executemany(sql, new_union_generator())
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
        conn.commit
    
        


if __name__ == "__main__":
  args = sys.argv

  insert_divide_to_db(args[1])