# -*- coding: utf-8 -*-
"""daily_get_kabutan.py
  全銘柄の最新の日足データを「株探」からスクレイピングしデータベース「raw_prices」と「prices」に登録
  
  brands_generator()関数でSQLite3ファイル「stocks.db」のテーブル「brands」に
  登録済みの銘柄情報のcodeを順次取り出し、その銘柄のページにアクセスし日足データ
  を取り出し、daily_get_kabutan()関数の引数db_file_nameで指定するSQLite3ファイル
  に登録する。

  Args:
    db_file_name (str): カレントディレクトリのSQLite3ファイル名

  
  ・get_price_kabutan(code)関数の返り値
        code (str)  証券コード
        d (str)     日付（年/月/日）0詰めなし
        o (float)   始値
        h (float)   高値
        l (float)   安値
        c (float)   終値
        v (integer) 出来高

  ・db_file_name(SQLite3)
      CREATE TABLE "raw_prices" (
	    "code"	TEXT,
	    "date"	TEXT,
	    "open"	REAL,
	    "high"	REAL,
	    "low"	REAL,
	    "close"	REAL,
	    "volume"	INTEGER,
	    PRIMARY KEY("code","date")
      )

"""
from pyquery import PyQuery
import datetime
import time
import sqlite3
import sys
import traceback

def get_price_kabutan(code):
  # サイト「株探」 code=[証券コード（4桁）]
  url = 'https://kabutan.jp/stock/?code={}'.format(code)

  q = PyQuery(url)

  if len(q.find('div.company_block')) == 0:
    return None

  try:
    # 日付 "2023-02-24"
    ts = q.find('#kobetsu_left > h2 > time').attr('datetime')
    # 日付 "2023-02-24"の文字列型⇒datetime型⇒date型へ変換
    d = datetime.datetime.strptime(ts, '%Y-%m-%d').date()
    # 始値
    o_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(1) > td').text()
    o = float(o_str.split(' ')[0].replace(',', ''))
    # 高値
    h_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(2) > td').text()
    h = float(h_str.split(' ')[0].replace(',', ''))
    # 安値
    l_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(3) > td').text()  
    l = float(l_str.split(' ')[0].replace(',', ''))
    # 終値
    c_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(4) > td').text()
    c = float(c_str.split(' ')[0].replace(',', ''))
    # 出来高
    v_str = q.find('#kobetsu_left > table:nth-child(4) > tbody > tr:nth-child(1) > td').text()
    v = int(v_str.split()[0].replace(',', ''))

    print(code, d, o, h, l, c, v)
    return code, d, o, h, l, c, v

  except (ValueError, IndexError):
    e = traceback.format_exc()
    print(e)
    return None

def brands_generator():
    conn = sqlite3.connect('stocks.db')
    cur = conn.cursor()
    cur.execute('select code from brands')
    return cur.fetchall()   # 銘柄コードが入ったタプルのリストを出力（[(1111,)(1112,)(1113,)……]）

def daily_get_kabutan(db_file_name):
    for code in brands_generator():
        print(code)
        prices = get_price_kabutan(code[0])
        print(prices)
        if prices:
            conn = sqlite3.connect(db_file_name)
            with conn:
                try:
                    sql1 = 'INSERT INTO raw_prices(code, date, open, high, low, close, volume) VALUES(?,?,?,?,?,?,?)'
                    sql2 = 'INSERT INTO prices(code, date, open, high, low, close, volume) VALUES(?,?,?,?,?,?,?)'
                    conn.execute(sql1, prices)
                    conn.execute(sql2, prices)
                except sqlite3.Error as e:
                    print('sqlite3.Error occurred:', e.args[0])
            conn.commit()
            time.sleep(1)

if __name__ == "__main__":
  args = sys.argv
  #daily_get_kabutan(args[1])
  get_price_kabutan(args[1])
