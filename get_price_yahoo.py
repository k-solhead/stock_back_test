# -*- coding: utf-8 -*-
"""
    get_price_yahoo(code)
        株価を取得する（yahoo_finance_api2）
        証券番号(code)で指定された銘柄の最新の日足データを１日分取得するよう設定
        
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
    
    <<yahoo_finance_api2>>
        https://pypi.org/project/yahoo-finance-api2/
    
        
        FREQUENCY_TYPEは次のような設定が可能です

            分足 share.FREQUENCY_TYPE_MINUTE
            時間足 share.FREQUENCY_TYPE_HOUR
            日足 share.FREQUENCY_TYPE_DAY
            週足 share.FREQUENCY_TYPE_WEEK
            月足 share.FREQUENCY_TYPE_MONTH
        
        PERIOD_TYPEは次のような設定が可能です
    
            日で指定 share.PERIOD_TYPE_DAY
            週で指定 share.PERIOD_TYPE_WEEK
            月で指定 share.PERIOD_TYPE_MONTH
            年で指定 share.PERIOD_TYPE_YEAR
        
        返り値は以下Key文字列をベースにした辞書型で、各値は日時ごとの順番にリストに格納されている
        {
            'timestamp': [...],
            'open': [...],
            'high': [...],
            'low': [...],
            'close': [...],
            'adj_close': [...],
            'volume': [...]
        }
"""
import datetime
from yahoo_finance_api2 import share
from yahoo_finance_api2.exceptions import YahooFinanceError
import sys

def get_price_yahoo(code):
    my_share = share.Share(code + '.T')
    symbol_data = None

    try:
        # 最新の日足データを1日分取得
        symbol_data = my_share.get_historical(
            share.PERIOD_TYPE_DAY, 1,
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
        # datetime型から文字列型に変換
        year = dt.strftime('%Y')
        # 月と日の頭に付く0詰めを外す
        month = dt.strftime('%m').lstrip('0')
        day = dt.strftime('%d').lstrip('0')
        d = year + '/' + month + '/' + day
        o = symbol_data['open'][0]
        h = symbol_data['high'][0]
        l = symbol_data['low'][0]
        c = symbol_data['close'][0]
        v = symbol_data['volume'][0]

        print(code, d, o, h, l, c, v)
        return code, d, o, h, l, c, v

if __name__ == "__main__":
    args = sys.argv
    get_price_yahoo(args[1])