import stock.insert_new_brands_to_db as newbrands
import stock.insert_delete_brands_to_db as delbrands
import stock.get_brand as getbrand
import daily_get_yahoo2 as getprice
import stock.insert_divide_to_db as getdivide
import apply_divide_union_data as applydivide
import datetime
import sqlite3
import sys

def daily_stock(db_file_name):
    newbrands.insert_new_brands_to_db(db_file_name)     #　新規上場銘柄情報を取得
    delbrands.insert_delete_brands_to_db(db_file_name)  #　上場廃止銘柄情報を取得

    d_today = (datetime.date.today()).strftime("%Y-%m-%d")     # 本日の日付を取得
    conn = sqlite3.connect(db_file_name)
    sql1 = "SELECT code FROM new_brands WHERE date <= ?"
    cur = conn.execute(sql1, (d_today,))    #　本日かそれ以前に上場した銘柄を取り出す
    n_code = cur.fetchall()     #　あればタプルのリストとして取り出し
    print(n_code)
    if n_code:
        for code in n_code:
            getbrand.insert_brand(db_file_name, code[0])    # 銘柄情報を取得し、brandsに登録する
    else:
        pass
    conn.commit()
    
    sql2 = "SELECT code FROM delete_brands WHERE date <= ?"
    cur = conn.execute(sql2, (d_today,))    #　本日かそれ以前に廃止になった銘柄を取り出す
    d_code = cur.fetchall()             #　あればタプルのリストとして
    print(d_code)
    if d_code:
        for code in d_code:
            try:
                cur.execute("INSERT INTO lost_brands SELECT * FROM brands WHERE code = ?", code)
            except sqlite3.Error as e:
                print('sqlite3.Error occurred:', e.args[0])
                pass
        for code in d_code:
            try:
                cur.execute("DELETE FROM brands WHERE code = ?", code)
            except sqlite3.Error as e:
                print('sqlite3.Error occurred:', e.args[0])
                pass
    else:
        pass
    conn.commit()

    getprice.daily_get_yahoo(db_file_name)                      # 本日の日足株価を取得しraw_pricesとpricesに登録する
    getdivide.insert_divide_to_db(db_file_name)                 # 最新の株式分割・併合上場を取得しdivide_union_dataに登録する
    applydivide.apply_divide_union_data(db_file_name, d_today)  # 株式分割・併合された銘柄の株価をpricesテーブルに反映させる
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    args = sys.argv
    daily_stock(args[1])

    