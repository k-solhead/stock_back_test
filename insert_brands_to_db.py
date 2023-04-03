# -*- coding: utf-8 -*-
"""insert_brands_to_db(db_file_name, code_range)
  銘柄情報をサイト「株探」から一括取得してデータベース登録する。

  Args:
    db_file_name: ファイル名（sqlite3データベース）
      Table名:brands
        code (str)  
        name (str)
        short_name (str)
        market (str)
        unit (str)
        sector (int)
        
    code_range: 銘柄の証券番号(1301-9997)

"""
from pyquery import PyQuery
import re
import time
import sqlite3
import sys

def get_brand(code):
  # サイト「株探」 code=[証券コード（4桁）]
  url = 'https://kabutan.jp/stock/?code={}'.format(code)

  q = PyQuery(url)
  # タグ<div class="company_block">があるかないかで指定されたcodeの銘柄の存在を判定
  if len(q.find('div.company_block')) == 0:
    return None

  try:
    # 銘柄名（正式名称）
    name = q.find('div.company_block > h3').text()
    # [証券コード] + [銘柄名（略称）]　
    code_short_name =  q.find('#stockinfo_i1 > div.si_i1_1 > h2').text()
    # 銘柄名（略称）
    short_name = re.sub('[0-9]{4}', '', code_short_name)
    # 上場市場名
    market = q.find('span.market').text()
    unit_str = q.find('#kobetsu_left > table:nth-child(4) > tbody > tr:nth-child(6) > td').text()
    # 単位株数
    unit = int(unit_str.split()[0].replace(',', ''))
    # セクタ
    sector = q.find('#stockinfo_i2 > div > a').text()
  except (ValueError, IndexError) as e:
    print('brandsget.Error occurred:', e.args[0])
    return None
  print(code, name, short_name, market, sector, unit)
  return code, name, short_name, market, sector, unit

def brands_generator(a, b):
  for code in range(a, b):
    print(code)
    brand = get_brand(code)
    if brand:
      yield brand
    time.sleep(1)

def insert_brands_to_db(db_file_name, a, b):
  conn = sqlite3.connect(db_file_name)
  with conn:
    sql = 'INSERT INTO brands(code, name, short_name, market, sector, unit) VALUES(?,?,?,?,?,?)'
    conn.executemany(sql, brands_generator(a, b))

if __name__ == "__main__":
    args = sys.argv
    insert_brands_to_db(args[1], int(args[2]), int(args[3]))
