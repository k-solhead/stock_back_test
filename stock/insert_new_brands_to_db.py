"""insert_new_brands_to_db(db_file_name)
    新規上場銘柄のコードと上場日をデータベースに登録する
    
    日本取引所グループの新規上場会社情報HP（https://www.jpx.co.jp/listing/stocks/new/index.html）
    から上場日と証券コードをスクレイピングで入手し、new_brandsテーブルに登録する。
    
    <new_brandsテーブル>
    CREATE TABLE new_brands (   --上場情報
        code TEXT,         -- 銘柄コード
        date TEXT,         -- 上場日
        PRIMARY KEY(code, date)
    );

    Args:
        db_file_name (str): データベース（SQLite3）のファイル名

"""
from pyquery import PyQuery
import datetime
import sqlite3
import sys

def new_brands_generator():
    url = 'https://www.jpx.co.jp/listing/stocks/new/index.html'
    q = PyQuery(url)
    for d, i in zip(q.find('tbody > tr:even > td:eq(0)'),
                    q.find('tbody > tr:even span')):
        date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
        yield (i.get('id'), date)

def insert_new_brands_to_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        try:
            sql = 'INSERT INTO new_brands(code,date) VALUES(?,?)'
            conn.executemany(sql, new_brands_generator())
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
            pass

if __name__ == "__main__":
  args = sys.argv

  insert_new_brands_to_db(args[1])