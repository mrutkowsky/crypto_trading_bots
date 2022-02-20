from binance.client import Client
import pandas as pd
import sqlalchemy
from binance import BinanceSocketManager

api_key = 'x'
api_secret = 'y'

client = Client(api_key, api_secret)
client.get_account()

bsm = BinanceSocketManager(client)
socket = bsm.trade_socket('BTCUSDT')


def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'p']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


engine = sqlalchemy.create_engine('sqlite:///BTCUSDTstream.db')

while True:
    await socket.__aenter__()
    msg = await socket.recv()
    frame = createframe(msg)
    frame.to_sql('BTCUSDT', engine, if_exists='apped', index=False)
    
    print(msg)
