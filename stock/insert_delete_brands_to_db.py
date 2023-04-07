"""insert_delete_brands_to_db(db_file_name)
    上場廃止銘柄のコードと廃止日をデータベースに登録する
    
    日本取引所グループの上場廃止銘柄一覧（https://www.jpx.co.jp/listing/stocks/delisted/index.html）
    から上場廃止日と証券コードをスクレイピングで入手し、delete_brandsテーブルに登録する。

    <dekete_brandsテーブル>
    CREATE TABLE delete_brands (   --廃止情報
        code TEXT,         -- 銘柄コード
        date TEXT,         -- 廃止日
        PRIMARY KEY(code, date)
    );

    Args:
        db_file_name (str): データベース（SQLite3）のファイル名

"""
from pyquery import PyQuery
import datetime
import sqlite3
import sys

def delete_brands_generator():
    url = 'https://www.jpx.co.jp/listing/stocks/delisted/index.html'
    q = PyQuery(url)
    for d, i in zip(q.find('tbody > tr > td:eq(0)'),
                    q.find('tbody > tr > td:eq(2)')):
        date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
        print(i.text, date)
        yield (i.text, date)

def insert_delete_brands_to_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        for delete_brands in delete_brands_generator():
            try:
                sql = 'INSERT INTO delete_brands(code,date) VALUES(?,?)'
                conn.execute(sql, delete_brands)
            except sqlite3.Error as e:
                print('sqlite3.Error occurred:', e.args[0])
                pass

if __name__ == "__main__":
  args = sys.argv

  insert_delete_brands_to_db(args[1])