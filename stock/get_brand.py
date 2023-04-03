# -*- coding: utf-8 -*-
"""get_brand(code)
  銘柄情報をサイト「株探」よりスクレイピングで取得してデータベース登録する。

  Args:
    code (str): 銘柄の証券コード

  Returns:
      code (str)        銘柄の証券コード
      name (str)        銘柄名（社名）
      short_name (str)  銘柄名（略称）
      market (str)      上場市場名
      unit (str)        単位株数
      sector (integer)  セクタ（業種）

"""
from pyquery import PyQuery
import re
import sys
import sqlite3

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
  except (ValueError, IndexError):
    return None

  print( code, name, short_name, market, sector, unit )
  return code, name, short_name, market, sector, unit

def insert_brand(db_file_name, code):
  brand_data = get_brand(code)
  conn = sqlite3.connect(db_file_name)
  with conn:
    try:
      sql = 'INSERT INTO brands(code, name, short_name, market, sector, unit) VALUES(?,?,?,?,?,?)'
      conn.execute(sql, brand_data)
    except sqlite3.Error as e:
      print('sqlite3.Error occurred:', e.args[0])
      pass

if __name__ == "__main__":
  args = sys.argv

  insert_brand(args[1], args[2])
