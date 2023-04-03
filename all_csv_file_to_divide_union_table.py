# -*- coding: utf-8 -*-
"""all_csv_file_to_divide_union_table.py
    「Yahoo!ファイナンスVIP倶楽部」からダウンロードしたcsvファイルから株式分割・併合情報をデータベースに登録する

    1 ダウンロードディレクトリからcsvファイルのパスを順次取り出すジェネレーター関数が回る
    2 取り出したパスからcsvファイルを読み込み、一行ずつ調整前後の終値から株式分割・併合を読み出すジェネレーター関数が回る
    3 株式分割・併合があれば一行ずつデータベースに登録する
    
    Args:
        db_file_name (str): カレントディレクトリのSQLite3ファイル名
        csv_file_dir (str): ダウンロードしたcsvファイルのあるディレクトリのパス
 
    ＜設定＞
    ・DBテーブル名「divide_union_data」
    
    ＜参考＞
        CREATE TABLE "divide_union_data" (
	        "code"	TEXT,
	        "date_of_right_allotment"	TEXT,
	        "before"	REAL,
	        "after"	REAL,
	        PRIMARY KEY("code","date_of_right_allotment")
        )
"""
import csv
import glob
import datetime
import os
import sqlite3
import sys

# 「Yahoo!ファイナンスVIP倶楽部」からダウンロードしたcsvファイルから
# 調整前と調整後の終値を取り出し前日の両値からの変動の率から株式分割・併合
# があった場合、その比率を算出する
def generator_divide_union_from_csv_file(csv_file_name, code):
    with open(csv_file_name) as f:
        reader = csv.reader(f)
        next(reader)    # 先頭行を飛ばす
        
        def parse_recode(row):
            d = datetime.datetime.strptime(row[0], '%Y/%m/%d').date()
            r = float(row[4])   # 調整前終値
            a = float(row[6])   # 調整後終値
            return d, r, a
        
        _, r_n, a_n = parse_recode(next(reader))
        for row in reader:
            d, r, a = parse_recode(row)
            rate = (a_n * r) / (a * r_n)
            if abs(rate - 1) > 0.005:
                if rate < 1:
                    before = round(1 / rate, 2)
                    after = 1
                else:
                    before = 1
                    after = round(rate, 2)
                yield code, d, before, after
            r_n = r
            a_n = a

# 指定したディレクトリ（csv_dir）にあるcsvファイルのパスを順次取り出し、
# パスから取り出したファイル名(1111.T.csv)と銘柄コード(1111)を切り出しながら
# 渡された関数（generate_func）の引数に次々と渡す⇒csvの調整前後終値から株式分割・併合情報を読み出す
def generate_from_csv_dir(csv_dir, generate_func):
    for path in glob.glob(os.path.join(csv_dir, "*.T.csv")):
        file_name = os.path.basename(path)
        code = file_name.split('.')[0]
        for d in generate_func(path, code):
            yield d

# 特定のディレクトリ（csv_file_dir）にあるcsvファイル 「"*.T.csv"」を順次読み込み、
# 2日間の調整前と調整後の終値から株式分割・併合の有無を判断し、その情報をデータベースに登録する
def all_csv_file_to_divide_union_table(db_file_name, csv_file_dir):
    divide_union_generator = generate_from_csv_dir(csv_file_dir, generator_divide_union_from_csv_file)
    conn = sqlite3.connect(db_file_name)
    with conn:
        sql = """
        INSERT INTO
        divide_union_data (code, date_of_right_allotment, before, after)
        VALUES(?,?,?,?)
        """
        conn.executemany(sql, divide_union_generator)

if __name__ == "__main__":
    args = sys.argv
    all_csv_file_to_divide_union_table(args[1], args[2])
