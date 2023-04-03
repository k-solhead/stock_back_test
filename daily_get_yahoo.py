# -*- coding: utf-8 -*-
"""daily_get_yahoo.py
    全銘柄の最新の日足データをyahoo_finance_api2から取得しデータベースに登録

    brands_generator()関数でSQLite3ファイル「stocks.db」のテーブル「brands」に
    登録済みの銘柄情報kから順次取り出したcodeをもとにyahoo_finance_api2で日足データ
    を取り出し、daily_get_yahoo()関数の引数db_file_nameで指定するSQLite3ファイル
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
import sys
import datetime
from yahoo_finance_api2 import share
from yahoo_finance_api2.exceptions import YahooFinanceError
import sqlite3

def get_price_yahoo(code):
    my_share = share.Share(code + '.T')
    symbol_data = None

    try:
        # 最新の日足データを1日分取得
        symbol_data = my_share.get_historical(
            share.PERIOD_TYPE_DAY, 0,
            share.FREQUENCY_TYPE_DAY, 1)
    except YahooFinanceError as e:
        print(e.message)
        #sys.exit(1)
        pass
    
    if (symbol_data == None):
        pass
    else:
        # タイムスタンプ
        ts = symbol_data['timestamp'][0]
        # タイムスタンプからdatetime型に変換
        dt = datetime.datetime.fromtimestamp(ts / 1000)
        # datetime型からdate型に変換
        d = dt.date()
        o = symbol_data['open'][0]
        h = symbol_data['high'][0]
        l = symbol_data['low'][0]
        c = symbol_data['close'][0]
        v = symbol_data['volume'][0]

        return code, d, o, h, l, c, v

def brands_generator():
    conn = sqlite3.connect('stocks.db')
    with conn:
        for code in conn.execute('select code from brands'):
            print(code)
            prices = get_price_yahoo(code[0])
            print(prices)
            if prices:
                yield prices

def daily_get_yahoo(db_file_name):
    conn = sqlite3.connect(db_file_name)
    with conn:
        try:
            sql = 'INSERT INTO raw_prices(code, date, open, high, low, close, volume) VALUES(?,?,?,?,?,?,?)'
            conn.executemany(sql, brands_generator())
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
    conn.commit()

if __name__ == "__main__":
    args = sys.argv
    daily_get_yahoo(args[1])

