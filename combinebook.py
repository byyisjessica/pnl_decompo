# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 15:15:14 2026

@author: huxin2
"""

import dolphindb as ddb
import pandas as pd
import numpy as np
from datetime import datetime

def from_dolphinDB(query):
    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456")
    df = s.run(query)
    s.close()
    return df

today = datetime.now().strftime('%Y.%m.%d')

def combinebook_precise(timecut='15:01:00'):
    # query = f""" select * from loadTable("dfs://eq_history", "trade") where partition(date, {today}) and securityid == "159400" and timestamp < {today} {timecut} """
    query = f""" select * from trade where securityid == "159400" and timestamp < {today} {timecut} """
    trade = from_dolphinDB(query)
    lasttime_trade = trade['timestamp'].max()

    cancel = trade[trade['trade_bs_flag'] == 'C'].copy()
    cancel['trade_no'] = cancel['trade_buy_no'] + cancel['trade_sell_no']
    cancel = cancel.rename(columns={'trade_volume': 'cancel_volume'})
    cancel = cancel.set_index('trade_no')['cancel_volume']
    
    trade = trade[trade['trade_bs_flag'] != 'C'].copy()
    trade1 = trade[['trade_buy_no', 'trade_volume']].rename(columns={'trade_buy_no': 'trade_no'})
    trade2 = trade[['trade_sell_no', 'trade_volume']].rename(columns={'trade_sell_no': 'trade_no'})
    trade = pd.concat([trade1, trade2])
    trade = trade['trade_volume'].groupby(trade['trade_no']).sum()
    
    # query = f""" select * from loadTable("dfs://eq_history", "order") where partition(date, {today}) and securityid == "159400" and timestamp < {today} {timecut} """
    query = f""" select * from order where securityid == "159400" and timestamp < {today} {timecut} """
    order = from_dolphinDB(query)
    lasttime_order = order['timestamp'].max()
    
    order = pd.merge(order, cancel, left_on='order_index', right_index=True, how='left')
    order = pd.merge(order, trade, left_on='order_index', right_index=True, how='left')
    
    order['remain_volume'] = order['order_volume'] - order['cancel_volume'].fillna(0) - order['trade_volume'].fillna(0)
    
    order_buy = order[order['order_side'] == 'B']
    order_buy = order_buy['remain_volume'].groupby(order_buy['order_price']).sum()
    order_buy = order_buy.sort_index(ascending=False)
    order_buy = order_buy[order_buy > 0]
    
    order_sell = order[order['order_side'] == 'S']
    order_sell = order_sell['remain_volume'].groupby(order_sell['order_price']).sum()
    order_sell = order_sell.sort_index(ascending=True)
    order_sell = order_sell[order_sell > 0]
    
    lasttime = max(lasttime_trade, lasttime_order)
    
    return order_buy, order_sell, lasttime

def combinebook_ambiguous(tick, timecut):
    order_buy_slice = pd.Series(tick.loc[0, ['bidvolume1', 'bidvolume2', 'bidvolume3', 'bidvolume4', 'bidvolume5']].astype('float').values.flatten(),
                                index=tick.loc[0, ['bidprice1', 'bidprice2', 'bidprice3', 'bidprice4', 'bidprice5']].astype('float').values.flatten())
    order_buy_slice.name = 'volume_slice'
    order_sell_slice = pd.Series(tick.loc[0, ['askvolume1', 'askvolume2', 'askvolume3', 'askvolume4', 'askvolume5']].astype('float').values.flatten(),
                                index=tick.loc[0, ['askprice1', 'askprice2', 'askprice3', 'askprice4', 'askprice5']].astype('float').values.flatten())
    order_sell_slice.name = 'volume_slice'
    
    query = f""" select * from trade where securityid == "159400" and timestamp > {today} {timecut} """
    trade = from_dolphinDB(query)
    lasttime_trade = trade['timestamp'].max()
    
    cancel = trade[trade['trade_bs_flag'] == 'C'].copy()
    cancel['trade_no'] = cancel['trade_buy_no'] + cancel['trade_sell_no']
    cancel = cancel.rename(columns={'trade_volume': 'cancel_volume'})
    cancel = cancel.set_index('trade_no')['cancel_volume']
    
    trade = trade[trade['trade_bs_flag'] != 'C'].copy()
    trade1 = trade[['trade_buy_no', 'trade_volume']].rename(columns={'trade_buy_no': 'trade_no'})
    trade2 = trade[['trade_sell_no', 'trade_volume']].rename(columns={'trade_sell_no': 'trade_no'})
    trade = pd.concat([trade1, trade2])
    trade = trade['trade_volume'].groupby(trade['trade_no']).sum()
    
    query = f""" select * from order where securityid == "159400" """
    order = from_dolphinDB(query)
    lasttime_order = order['timestamp'].max()
    
    order = pd.merge(order, cancel, left_on='order_index', right_index=True, how='left')
    order = pd.merge(order, trade, left_on='order_index', right_index=True, how='left')
    order['decrease_volume'] = order['cancel_volume'].fillna(0) + order['trade_volume'].fillna(0)
    order = order[order['decrease_volume'] > 0].copy()
    kill_order_buy = order[order['order_side'] == 'B'].copy()
    kill_order_buy = kill_order_buy['decrease_volume'].groupby(kill_order_buy['order_price']).sum()
    kill_order_buy.name = 'kill_order'
    kill_order_buy = kill_order_buy[kill_order_buy.index >= order_buy_slice.index[-1]]
    kill_order_sell = order[order['order_side'] == 'S'].copy()
    kill_order_sell = kill_order_sell['decrease_volume'].groupby(kill_order_sell['order_price']).sum()
    kill_order_sell.name = 'kill_order'
    kill_order_sell = kill_order_sell[kill_order_sell.index <= order_sell_slice.index[-1]]
    
    query = f""" select * from order where securityid == "159400" and timestamp > {today} {safetime.strftime('%H:%M:%S')} """
    order = from_dolphinDB(query)
    order_buy = order[order['order_side'] == 'B'].copy()
    order_buy = order_buy[order_buy['order_price'] >= order_buy_slice.index[-1]].copy()
    new_order_buy = order_buy['order_volume'].groupby(order_buy['order_price']).sum()
    new_order_buy.name = 'new_order'
    order_sell = order[order['order_side'] == 'S'].copy()
    order_sell = order_sell[order_sell['order_price'] <= order_sell_slice.index[-1]].copy()
    new_order_sell = order_sell['order_volume'].groupby(order_sell['order_price']).sum()
    new_order_sell.name = 'new_order'
    
    order_buy_slice = pd.merge(order_buy_slice, new_order_buy, left_index=True, right_index=True, how='outer')
    order_buy_slice = pd.merge(order_buy_slice, kill_order_buy, left_index=True, right_index=True, how='outer')
    order_buy_slice['remain_volume'] = order_buy_slice['volume_slice'].fillna(0) + order_buy_slice['new_order'].fillna(0) - order_buy_slice['kill_order'].fillna(0)
    
    order_sell_slice = pd.merge(order_sell_slice, new_order_sell, left_index=True, right_index=True, how='outer')
    order_sell_slice = pd.merge(order_sell_slice, kill_order_sell, left_index=True, right_index=True, how='outer')
    order_sell_slice['remain_volume'] = order_sell_slice['volume_slice'].fillna(0) + order_sell_slice['new_order'].fillna(0) - order_sell_slice['kill_order'].fillna(0)
    
    order_buy = order_buy_slice[order_buy_slice['remain_volume'] > 0]['remain_volume'].sort_index(ascending=False)
    order_sell = order_sell_slice[order_sell_slice['remain_volume'] > 0]['remain_volume'].sort_index(ascending=True)
    
    lasttime = max(lasttime_trade, lasttime_order)
    
    return order_buy, order_sell, lasttime
   
#%%

query = f""" select last(safetime) from loadTable("dfs://eq_history", "safetime") where partition(date, {today}) and stkcode == "159400" """
safetime = from_dolphinDB(query).loc[0, 'last_safetime']

query = f""" select * from loadTable("dfs://eq_history", "etftick") where partition(date, {today}) and securityid == "159400" and timestamp == {today} {safetime.strftime('%H:%M:%S')} """
tick = from_dolphinDB(query)

order_buy, order_sell, lasttime = combinebook_precise(timecut=safetime.strftime('%H:%M:%S'))

check1 = tick.loc[0, ['askprice1', 'askprice2', 'askprice3', 'askprice4', 'askprice5']].astype('float').values.flatten() == order_sell.index[:5]
check2 = tick.loc[0, ['bidprice1', 'bidprice2', 'bidprice3', 'bidprice4', 'bidprice5']].astype('float').values.flatten() == order_buy.index[:5]
check3 = tick.loc[0, ['askvolume1', 'askvolume2', 'askvolume3', 'askvolume4', 'askvolume5']].astype('float').values.flatten() == order_sell.values[:5]
check4 = tick.loc[0, ['bidvolume1', 'bidvolume2', 'bidvolume3', 'bidvolume4', 'bidvolume5']].astype('float').values.flatten() == order_buy.values[:5]
if check1.all() == False or check2.all() == False or check3.all() == False or check4.all() == False:
    order_buy, order_sell, lasttime = combinebook_ambiguous(tick, safetime.strftime('%H:%M:%S'))
    sendmessage = {
        'stkcode': '159400',
        'marketid': 'sz',
        'time': lasttime,
        'mode': 'ambiguous',
        }
else:
    order_buy, order_sell, lasttime = combinebook_precise()
    sendmessage = {
        'stkcode': '159400',
        'marketid': 'sz',
        'time': lasttime,
        'mode': 'precise',
        }

order_sell_count = min(len(order_sell), 10)
order_buy_count = min(len(order_buy), 10)
askprice_dict = {f'askprice{i+1}': order_sell.index[i] if i < order_sell_count else np.nan for i in range(10)}
askvolume_dict = {f'askvolume{i+1}': int(order_sell.values[i]) if i < order_sell_count else np.nan for i in range(10)}
bidprice_dict = {f'bidprice{i+1}': order_buy.index[i] if i < order_buy_count else np.nan for i in range(10)}
bidvolume_dict = {f'bidvolume{i+1}': int(order_buy.values[i]) if i < order_buy_count else np.nan for i in range(10)}
sendmessage.update(askprice_dict)
sendmessage.update(askvolume_dict)
sendmessage.update(bidprice_dict)
sendmessage.update(bidvolume_dict)

sendmessage_df = pd.DataFrame(sendmessage, index=[0])
sendmessage_df['date'] = pd.to_datetime(today)
sendmessage_df['time_insertdb'] = datetime.now()
session = ddb.session()
session.connect("localhost", 8848, "admin", "123456")
appender = ddb.TableAppender(dbPath="dfs://eq_history", tableName="combinebook", ddbSession=session)
num = appender.append(sendmessage_df)
session.close()