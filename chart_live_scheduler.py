# import
import numpy as np
import pandas as pd
import ccxt
from datetime import datetime
import urllib.parse
import operator
import cufflinks as cf
from IPython.display import display,HTML,clear_output
import schedule
import threading
import time


# We set the all charts as public
cf.set_config_file(sharing='public', theme='pearl', offline=False)
cf.go_offline()


def create_auth(credBIN):
    auth_users = {}
    for i in range(0, len(credBIN)):
        user_name = credBIN.iloc[i]['SUBACCOUNT']
        uri_nickname = urllib.parse.quote(user_name)
        apiKey = credBIN.iloc[i]['ACCOUNT_API_KEY']
        secret = credBIN.iloc[i]['ACCOUNT_API_SECRET']
        
        try:
            auth_users[user_name] = ccxt.ftx({
                'apiKey':apiKey,
                'secret':secret,
                'headers':{
                    'FTX-SUBACCOUNT':uri_nickname
                }
            })
        except Exception as e:
            print(user_name, '\n' + str(e))
        
    return auth_users


def get_total_USD_balance(auth_users, view=False):
    balances = {}
    allowed_coins = ['USD', 'USDT', 'USDC', 'USDP', 'TUSD', 'BUSD']
    
    for user_name in auth_users:
        total_amount = 0
        try:
            balances_user = auth_users[user_name].fetchBalance()
            balance_eur = 0

            for i in range(len(balances_user['info']['result'])):
                if balances_user['info']['result'][i]['coin'] == 'EUR':
                    balance_eur = balances_user['info']['result'][i]['usdValue']

            for balance in balances_user['total']:
                if balance in allowed_coins:
                    total_amount += balances_user['total'][balance]

            balances[user_name] = total_amount + float(balance_eur)

            if view:
                print(f"{user_name}: {balances_user[user_name]} $")
        
        except Exception as e:
            print(user_name + '\n'+str(e))
    
    return balances


def get_pl(auth_users, starting_balances):
    charts = {}
    balances_updated = get_total_USD_balance(auth_users, False)
    
    try:
        for user_name in auth_users:
            starting_amount = starting_balances.loc[user_name][0]
            pnl = 0
            if starting_amount != 0:
                pnl = round((balances_updated[user_name]-starting_amount)/starting_amount*100,2)
            if pnl > -100:
                charts[user_name + ' ' + auth_users[user_name].apiKey[:3]] = pnl 
    except Exception as e:
        print('ERROR pnl ' + user_name + ' ' + auth_users[user_name].apiKey[:3])
        charts[user_name + ' ' + auth_users[user_name].apiKey[:3]] = 0
    
    return charts


def update_charts(auth_users, starting_balances):
    auth_users_italia = create_auth(pd.read_csv('CREDENTIALS/2022 ITALIA.csv'))
    charts_italia = get_pl(auth_users_italia, starting_balances)
    df_italia = pd.DataFrame.from_dict(charts_italia, orient='index', columns=['PnL'])
    df_italia = df_italia.sort_values(by=['PnL'], ascending=True)
    df_italia = df_italia.iloc[-20:]
    
    italia = pd.read_csv('CREDENTIALS/2022 ITALIA.csv')
    tradingon = italia.where(italia.COMMUNITY == 'Tradingon').dropna()
    auth_tradingon =create_auth(tradingon)
    charts_tradingon = get_pl(auth_tradingon, starting_balances)
    df_tradingon = pd.DataFrame.from_dict(charts_tradingon, orient='index', columns=['PnL'])
    df_tradingon = df_tradingon.sort_values(by=['PnL'], ascending=True)
    df_tradingon = df_tradingon.iloc[-20:]
    
    afterside = italia.where(italia.COMMUNITY == 'Afterside').dropna()
    auth_afterside =create_auth(afterside)
    charts_afterside = get_pl(auth_afterside, starting_balances)
    df_afterside = pd.DataFrame.from_dict(charts_afterside, orient='index', columns=['PnL'])
    df_afterside = df_afterside.sort_values(by=['PnL'], ascending=True)
    #df_afterside = df_afterside.iloc[-20:]
    
    tcg = italia.where(italia.COMMUNITY == 'The Crypto Gateway').dropna()
    auth_tcg =create_auth(tcg)
    charts_tcg = get_pl(auth_tcg, starting_balances)
    df_tcg = pd.DataFrame.from_dict(charts_tcg, orient='index', columns=['PnL'])
    df_tcg = df_tcg.sort_values(by=['PnL'], ascending=True)
    df_tcg = df_tcg.iloc[-15:]
    
    auth_users_spagna = create_auth(pd.read_csv('CREDENTIALS/2022 SPAGNA.csv'))
    charts_spagna = get_pl(auth_users_spagna, starting_balances)
    df_spagna = pd.DataFrame.from_dict(charts_spagna, orient='index', columns=['PnL'])
    df_spagna = df_spagna.sort_values(by=['PnL'], ascending=True)
    # df_spagna = df_spagna.iloc[-15:]
    
    clear_output(wait=True)
    display(HTML('<table style="border-collapse: collapse; width: 100%;" border="1"><tbody><tr><td style="width: 33%; border-style: none;"><h1 style="color: #5e9ca0; text-align: center;"><img src="https://tradingon.it/wp-content/uploads/2021/03/tradingon-logo-1.svg" alt="logo" width="213" height="55" /></h1></td><td style="width: 33%; text-align: center;"><h2><span style="color: #ff9900;"><span style="color: #333300;">TRADING</span> COMPETITION</span></h2><h2><span style="color: #ff9900;"><span style="color: #333300;">october 2022</span></td><td style="width: 33%; border-style: none;"><h1 style="color: #5e9ca0; text-align: center;"><img src="https://upload.wikimedia.org/wikipedia/commons/6/68/FTX_logo.svg" alt="logo" width="213" height="55" /></tr></tbody></table>'))
    print('last update:', datetime.now().strftime("%H:%M:%S"))
    
    display(HTML('<h3>ITALY</h3>'))
    df_italia.iplot(kind='barh', bargap=.2)
    
    display(HTML('<h3>TRADING ON</h3>'))
    df_tradingon.iplot(kind='barh', bargap=.2)
    
    display(HTML('<h3>THE CRYPTO GATEWAY</h3>'))
    df_tcg.iplot(kind='barh', bargap=.2)
    
    display(HTML('<h3>AFTERSIDE</h3>'))
    df_afterside.iplot(kind='barh', bargap=.2)
    
    display(HTML('<h3>SPAIN</h3>'))
    df_spagna.iplot(kind='barh', bargap=.2)
    
    
# init

auth_users = create_auth(pd.read_csv('CREDENTIALS/2022.csv'))
starting_balances = pd.read_csv('start_users.csv', index_col=0)

update_charts(auth_users, starting_balances)

# SCHEDULER
run = True
schedule.every(10).minutes.do(update_charts, auth_users=auth_users, starting_balances=starting_balances)


def scheduled():
    while run:
        schedule.run_pending()
        time.sleep(1)


threads = [threading.Thread(target=scheduled)]
[thread.start() for thread in threads]
