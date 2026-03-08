# -*- coding: utf-8 -*-
import pandas as pd
import dolphindb as ddb
import multiprocessing as mp
from datetime import datetime
import os, eqapi, signal, json

class MyApplication(eqapi.RqApplication):
    
    def __init__(self, settings, queue):
        super(MyApplication, self).__init__(settings)
        self.__queue = queue
        self.HQ_dict = json.load(open('data/EQnames.json', 'r'))
    
    def onQuote(self, quotes):
        new_quotes = [json.loads(q) for q in quotes]
        df = pd.DataFrame(new_quotes)
        temp = list(df.columns)
        df.columns = [self.HQ_dict[k] for k in temp]
        
        time_min = df.at[0, 'time'] / 100000
        if (914 < time_min and time_min < 1131) or (1259 < time_min and time_min < 1501):
            try:
                # 尝试非阻塞放入，队列满时丢弃旧数据
                if self.__queue.full():
                    try:
                        _ = self.__queue.get_nowait()  # 丢弃最旧数据
                    except:
                        pass
                self.__queue.put_nowait(df)  # 放入新数据
            except:
                pass
    
    def onLog(self, msg):
        print("MyApplication on Log:", msg)
        pass
    
    def onConnect(self, msg):
        print("MyApplication connect success")
        print(msg)
    
    def onError(self, msg):
        print("MyApplication on Error")
        print(msg)

    def onDisconnect(self, msg):
        print("MyApplication on Disconnect")
        print(msg)


def Function2(queue):
    t_day = datetime.now().strftime("%Y-%m-%d")
    
    colName = ['securityid', 'market', 'date', 'time',
               'quote_type', 'trade_order_channel',
               'trade_index', 'trade_price', 'trade_volume', 'trade_buy_no', 'trade_sell_no', 'trade_bs_flag',
               'biz_index', 'timestamp']
    
    s = ddb.session()
    conn = s.connect("localhost", 8848, "admin", "123456")
    if not conn:
        print('Database connection failed')
        return
    appender = ddb.TableAppender(tableName='trade', ddbSession=s)
    
    def parse_ticktime(time_val):
        try:
            return datetime.strptime(t_day + str(time_val).zfill(9), '%Y-%m-%d%H%M%S%f')
        except:
            return None
    
    while True:
        if not queue.empty():
            try:
                data = queue.get_nowait()
                # print(data.T)
                
                # securityid              510050
                # market                     259
                # date                  20260302
                # time                 134728920
                # quote_type                   3
                # trade_order_channel          4
                # trade_index           21632691
                # trade_price              31270
                # trade_volume             78100
                # trade_buy_no          13243691
                # trade_sell_no         13243602
                # trade_bs_flag                B
                # biz_index             21632691
                
                # 0   securityid           1 non-null      object
                # 1   market               1 non-null      int64
                # 2   date                 1 non-null      int64
                # 3   time                 1 non-null      int64
                # 4   quote_type           1 non-null      int64
                # 5   trade_order_channel  1 non-null      int64
                # 6   trade_index          1 non-null      int64
                # 7   trade_price          1 non-null      int64
                # 8   trade_volume         1 non-null      int64
                # 9   trade_buy_no         1 non-null      int64
                # 10  trade_sell_no        1 non-null      int64
                # 11  trade_bs_flag        1 non-null      object
                # 12  biz_index            1 non-null      int64
                
                data['trade_price'] /= 1e4
                data['timestamp'] = data['time'].apply(parse_ticktime)
                data['date'] = pd.to_datetime(t_day)
                if data.loc[0, 'securityid'][0] in ['0', '1', '3']:
                    data['biz_index'] = 0
                    
                valid_data = data[colName].dropna(subset=['timestamp'])
                if not valid_data.empty:
                    appender.append(valid_data)
                    
            except Exception as e:
                print(f"Error processing data: {e}")
                continue

def Function1(queue, query_line):
    setting = eqapi.EqSetting()
    setting.ip, setting.port, setting.user, setting.passwd = "10.66.178.4", "12011", "derivatives_zy_sc", "derivatives_zy_sc"
    setting1 = eqapi.EqSetting()
    setting1.ip, setting1.port, setting1.user, setting1.passwd = "10.66.178.3", "12011", "derivatives_zy_sc", "derivatives_zy_sc"
    test = MyApplication(settings=[setting, setting1], queue=queue)
    test.start()
    
    if test.state() == eqapi.EqState.EQ_STATE_CONNECT:
        test.sub(query_line, 0)
    
    os.system('pause')
    os.kill(os.getpid(), signal.SIGTERM)


if __name__ == '__main__':
    print('trade行情')
   
    query_line = "szl2:trade:159400"
    
    # 创建带最大大小的队列
    queue = mp.Queue(maxsize=10)
    function1 = mp.Process(target=Function1, args=(queue, query_line), daemon=True)
    function2 = mp.Process(target=Function2, args=(queue,), daemon=True)
    process = [function1, function2]
    
    for job in process:
        job.start()
    for job in process:
        job.join()
    for job in process:
        if job.is_alive():
            print('进程仍在运行', job)
            job.terminate()
    