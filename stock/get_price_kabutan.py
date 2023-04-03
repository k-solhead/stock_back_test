# -*- coding: utf-8 -*-
"""
  get_price_kabutan(code)
    株価を取得する（株探）
    証券番号(code)で指定された銘柄の最新の日足データを１日分取得する
    Args:
        code (str): 銘柄の証券コード

    Returns:
        code (str)  証券コード
        d (str)     日付（年/月/日）0詰めなし
        o (float)   始値
        h (float)   高値
        l (float)   安値
        c (float)   終値
        v (integer) 出来高
    
    株探（https://kabutan.jp/）からスクレイピングで最新の日足データを取得
"""
from pyquery import PyQuery
import sys

def get_price_kabutan(code):
  # サイト「株探」 code=[証券コード（4桁）]
  url = 'https://kabutan.jp/stock/?code={}'.format(code)

  q = PyQuery(url)

  if len(q.find('div.company_block')) == 0:
    return None

  try:
    # 日付 "2023-02-24"　→　"2023/2/24"
    ts = q.find('#kobetsu_left > h2 > time').attr('datetime')
    dt = ts.split("-")
    year = dt[0]
    # 月と日の頭に付く0詰めを外す
    month = dt[1].lstrip('0')
    day = dt[2].lstrip('0')
    d = year + '/' + month + '/' + day
    # 始値
    o_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(1) > td').text()
    o = float(o_str.split('  ')[0].replace(',', ''))
    # 高値
    h_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(2) > td').text()
    h = float(h_str.split('  ')[0].replace(',', ''))
    # 安値
    l_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(3) > td').text()  
    l = float(l_str.split('  ')[0].replace(',', ''))
    # 終値
    c_str = q.find('#kobetsu_left > table:nth-child(3) > tbody > tr:nth-child(4) > td').text()
    c = float(c_str.split('  ')[0].replace(',', ''))
    # 出来高
    v_str = q.find('#kobetsu_left > table:nth-child(4) > tbody > tr:nth-child(1) > td').text()
    v = int(v_str.split()[0].replace(',', ''))

    print(code, d, o, h, l, c, v)
    return code, d, o, h, l, c, v

  except (ValueError, IndexError):
    return None

  

if __name__ == "__main__":
  args = sys.argv

  get_price_kabutan(int(args[1]))
