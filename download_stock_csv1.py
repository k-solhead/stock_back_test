# -*- coding: utf-8 -*-
"""download_stock_csv1()
    「Yahoo!ファイナンスVIP倶楽部」から銘柄ごとに過去の日足データをCSVファイルとしてダウンロード（FireFox版）
    
    ＜概要＞
    seleniumを使い、「Yahoo!ファイナンスVIP倶楽部」をスクレイピングしながらダウンロードボタンを押し、
    全銘柄の過去の日足データを銘柄ごとにCSVファイルとしてダウンロードする。
    「Yahoo!ファイナンスVIP倶楽部」の有料会員のアカウントが必要。
    
    ＜使い方＞
    ・FireFoxブラウザが起動し、ヤフートップ画面に自動アクセスする
    ・Yahoo!ファイナンスVIP倶楽部（有料）に手動でログインする
    ・コマンドラインプロンプトの'After login, press enter: 'にennterを打ち込む
    ・
    
    ＜設定＞
    ・firefox_profileで引数save_dirでダウンロード先を指定。コマンドプロンプトでカレントディレクトリを指定。
    ・SQLite3のデータベース（カレントディレクトリにある「brands.db」）
      テーブル「brands」の「code (TEXT) ,name (TEXT),short_name (TEXT),market (TEXT),sector (TEXT),unit (INTEGER)」
      からcodeを順番に取り出し、urlを生成。

"""
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import sqlite3

def download_stock_csv1(save_dir):

    # CSVファイルを自動で save_dir に保存するための設定
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList",
                           2)
    profile.set_preference("browser.download.manager.showWhenStarting",
                           False)
    profile.set_preference("browser.download.dir", save_dir)
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "text/csv")

    driver = webdriver.Firefox(firefox_profile=profile)
    driver.get('https://www.yahoo.co.jp/')

    # ここで手動でログインを行う。ログインしたら enter
    input('After login, press enter: ')

    conn = sqlite3.connect('brands.db')
    with conn:

        for code in conn.execute('select code from brands'):
            url = 'https://stocks.finance.yahoo.co.jp/stocks/history/?code={0}.T'.format(str(code[0]))
            driver.get(url)

            try:
                driver.find_element_by_link_text('時系列データをダウンロード（CSV）').click()

            except NoSuchElementException:
                pass

if __name__ == '__main__':
    import os
    download_stock_csv1(os.getcwd())