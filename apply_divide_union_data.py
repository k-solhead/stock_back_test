# -*- coding: utf-8 -*-
"""apply_divide_union_data(db_file_name, date_of_right_allotment)
    株価分割・併合が行われた銘柄の株価を反映する。
    divide_union_dataテーブルのうち本日までに株価分割・併合が行われた未適用のものを探し、
    pricesテーブルの株価（raw_pricesテーブルはそのまま）に反映するととともに適用済みの
    applied_divide_union_dataテーブルに登録し、重複しないようにする。
"""
import datetime
import sqlite3
import sys

def apply_divide_union_data(db_file_name, date_of_right_allotment):
    conn = sqlite3.connect(db_file_name)

    # date_of_right_allotment 以前の分割・併合データで未適用のものを取得する
    sql = """
    SELECT 
        d.code, d.date_of_right_allotment, d.before, d.after
    FROM 
        divide_union_data AS d
    WHERE
        d.date_of_right_allotment < ?
        AND NOT EXISTS (
            SELECT 
                * 
            FROM 
                applied_divide_union_data AS a
            WHERE 
                d.code = a.code
                AND d.date_of_right_allotment = a.date_of_right_allotment
            ) 
    ORDER BY 
        d.date_of_right_allotment
    """
    cur = conn.execute(sql, (date_of_right_allotment,))
    divide_union_data = cur.fetchall()
    
    with conn:
        conn.execute('BEGIN TRANSACTION')
        for code, date_of_right_allotment, before, after in divide_union_data:
            
            rate = before / after
            inv_rate = 1 / rate
            
            conn.execute(
              'UPDATE prices SET '
              ' open = open * :rate, '
              ' high = high * :rate, '
              ' low = low  * :rate, '
              ' close = close * :rate, '
              ' volume = volume * :inv_rate '
              'WHERE code = :code '
              '  AND date <= :date_of_right_allotment',
              {'code' : code,
               'date_of_right_allotment' : date_of_right_allotment,
               'rate' : rate,
               'inv_rate' : inv_rate})
                          
            conn.execute(
              'INSERT INTO '
              'applied_divide_union_data(code, date_of_right_allotment) '
              'VALUES(?,?)',
              (code, date_of_right_allotment))
            print(code, date_of_right_allotment, before, after)
if __name__ == "__main__":
    args = sys.argv
    apply_divide_union_data(args[1], (datetime.date.today()).strftime("%Y-%m-%d"))
