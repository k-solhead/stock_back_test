# -*- coding: utf-8 -*-
"""download_stock_csv()
    「Yahoo!ファイナンスVIP倶楽部」から銘柄ごとに過去の日足データをCSVファイルとしてダウンロード（Chrome版）
    
    ＜概要＞
    seleniumを使い、「Yahoo!ファイナンスVIP倶楽部」をスクレイピングしながらダウンロードボタンを押し、
    全銘柄の過去の日足データを銘柄ごとにCSVファイルとしてダウンロードする。
    「Yahoo!ファイナンスVIP倶楽部」の有料会員のアカウントが必要。
    
    ＜使い方＞
    ・Chromeブラウザが起動し、ヤフートップ画面に自動アクセスする
    ・Yahoo!ファイナンスVIP倶楽部（有料）に手動でログインする
    ・コマンドラインプロンプトの'After login, press enter: 'にennterを打ち込む
    
    ＜設定＞
    ・Optionsでダウンロード先を指定。CDIR=カレントディレクトリ/data/CSV
    ・SQLite3のデータベース（カレントディレクトリにある「brands.db」）
      テーブル「brands」の「code (TEXT) ,name (TEXT),short_name (TEXT),market (TEXT),sector (TEXT),unit (INTEGER)」
      からcodeを順番に取り出し、urlを生成。

"""
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options # オプションを使うために必要

import sqlite3
import os
import time

def download_stock_csv():

    option = Options()
    # seleniumでのダウンロード先のフォルダの設定(※必ずフルパスとすること)
    CDIR = os.getcwd()
    download_path = os.path.join(CDIR, 'data', 'CSV')
    option.add_experimental_option("prefs", {"download.default_directory": download_path})
    driver = webdriver.Chrome(options=option)
    driver.get('https://www.yahoo.co.jp/')

    # ここで手動ログインを行う。ログインしたらenter
    input('After login, press enter: ')

    conn = sqlite3.connect('stocks.db')
    with conn:

        for code in conn.execute('select code from brands'):
            url = 'https://stocks.finance.yahoo.co.jp/stocks/history/?code={0}.T'.format(str(code[0]))
            driver.get(url)

            try:
                driver.find_element_by_link_text('時系列データをダウンロード（CSV）').click()
                time.sleep(1)
            
            except NoSuchElementException:
                pass

if __name__ == '__main__':
    
    download_stock_csv()