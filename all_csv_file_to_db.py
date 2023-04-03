# -*- coding: utf-8 -*-
"""all_csv_file_to_db.py
    「Yahoo!ファイナンスVIP倶楽部」からダウンロードした過去の日足csvファイルをデータベースに登録する

    1 ダウンロードディレクトリからcsvファイルのパスを順次取り出すジェネレーター関数が回る
    2 取り出したパスからcsvファイルを読み込み、一行ずつ日足データを取り出すジェネレーター関数が回る
    3 一行ずつデータベースに登録する
    
    Args:
        db_file_name (str): カレントディレクトリのSQLite3ファイル名
        csv_file_dir (str): ダウンロードしたcsvファイルのあるディレクトリのパス（絶対パス）
 
    ＜設定＞
    ・DBテーブル名「raw_prices」
    ・DBテーブル名「prices」(raw_pricesと同じ列)
    
    ＜参考＞
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
import csv
import glob
import datetime
import os
import sqlite3
import sys

# 「Yahoo!ファイナンスVIP倶楽部」からダウンロードした特定銘柄の日足csvファイルをデータにする
def generate_price_from_csv_file(csv_file_name, code):
    with open(csv_file_name) as f:
        print(csv_file_name)
        reader = csv.reader(f)
        next(reader)    # 先頭行を飛ばす
        for row in reader:
            d = datetime.datetime.strptime(row[0], '%Y/%m/%d').date()
            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            c = float(row[4])
            v = int(row[5])
            yield code, d, o, h, l, c, v

# 指定したディレクトリ（csv_dir）にあるcsvファイルのパスを順次取り出し、
# パスから取り出したファイル名(1111.T.csv)と銘柄コード(1111)を切り出しながら
# 渡された関数（generate_func）の引数に次々と渡す⇒csvから日足データを出力する
def generate_from_csv_dir(csv_dir, generate_func):
    for path in glob.glob(os.path.join(csv_dir, "*.T.csv")):
        file_name = os.path.basename(path)
        code = file_name.split('.')[0]
        for d in generate_func(path, code):
            yield d


# 特定のディレクトリ（csv_file_dir）にあるcsvファイル 「"*.T.csv"」を順次読み込み、
# 銘柄ごとに日足データをデータベースに登録する
def all_csv_file_to_db(db_file_name, csv_file_dir):
    price_generator = generate_from_csv_dir(csv_file_dir, generate_price_from_csv_file)
    print(price_generator)
    conn = sqlite3.connect(db_file_name)
    with conn:
        try:
            sql = """
                INSERT INTO raw_prices(code, date, open, high, low, close, volume)
                VALUES(?,?,?,?,?,?,?)
                """
            conn.executemany(sql, price_generator)
        except sqlite3.Error as e:
            print('sqlite3.Error occurred:', e.args[0])
    conn.commit()
    
    # データベース「raw_prices」に登録された各銘柄の過去日足データを「prices」にコピーする
    cur = conn.cursor()
    cur.execute("INSERT INTO prices SELECT * FROM raw_prices")
    conn.commit()
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    args = sys.argv
    all_csv_file_to_db(args[1], args[2])