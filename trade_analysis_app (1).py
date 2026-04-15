"""
trade_analysis_app.py
Streamlit version of Trade Analysis notebook.

HOW TO RUN (use the launcher to set upload limit):
    python run_app.py

Or manually:
    1. python -c "import pathlib; d=pathlib.Path.home()/'.streamlit'; d.mkdir(exist_ok=True); (d/'config.toml').write_text('[server]\nmaxUploadSize = 2048\nmaxMessageSize = 2048\n')"
    2. streamlit run trade_analysis_app.py

Notes:
- finplot requires a Qt display and cannot run in Streamlit (headless).
  Cell 4 (interactive finplot chart) is omitted from this app.
- All Cell 3 matplotlib charts ARE displayed inline.
- Excel output is produced and offered as a download.
- Tick CSV files and Raw Report xlsx can be uploaded directly or inside a .zip.
"""

import io
import os
import tempfile
import zipfile
import warnings
import datetime
import enum
import json
import logging
import random
import re
import string

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
from matplotlib.ticker import FormatStrFormatter
from scipy import signal
from tabulate import tabulate

pd.options.mode.chained_assignment = None
np.seterr(divide='ignore', invalid='ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

import requests
from websocket import create_connection

logger = logging.getLogger(__name__)


# =============================================================================
# SECTION 1 — TvDatafeed class & helpers  (unchanged from original)
# =============================================================================

class Interval(enum.Enum):
    in_1_minute  = "1";  in_3_minute  = "3";  in_5_minute  = "5"
    in_15_minute = "15"; in_30_minute = "30"; in_45_minute = "45"
    in_1_hour    = "1H"; in_2_hour    = "2H"; in_3_hour    = "3H"
    in_4_hour    = "4H"; in_daily     = "1D"; in_weekly    = "1W"
    in_monthly   = "1M"


class TvDatafeed:
    __sign_in_url    = 'https://www.tradingview.com/accounts/signin/'
    __search_url     = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers     = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout     = 5

    def __init__(self, username=None, password=None):
        self.ws_debug = False
        self.token = self.__auth(username, password)
        if self.token is None:
            self.token = "unauthorized_user_token"
        self.ws = None
        self.session       = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):
        if username is None or password is None: return None
        try:
            r = requests.post(self.__sign_in_url,
                              data={"username": username, "password": password, "remember": "on"},
                              headers=self.__signin_headers)
            return r.json()['user']['auth_token']
        except Exception: return None

    def __create_connection(self):
        self.ws = create_connection("wss://data.tradingview.com/socket.io/websocket",
                                    headers=self.__ws_headers, timeout=self.__ws_timeout)

    @staticmethod
    def __generate_session():
        return "qs_" + "".join(random.choice(string.ascii_lowercase) for _ in range(12))

    @staticmethod
    def __generate_chart_session():
        return "cs_" + "".join(random.choice(string.ascii_lowercase) for _ in range(12))

    @staticmethod
    def __prepend_header(st): return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, p): return self.__prepend_header(self.__construct_message(func, p))

    def __send_message(self, func, args): self.ws.send(self.__create_message(func, args))

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            out = re.search('"s":\[(.+?)\}\]', raw_data).group(1)
            data = []
            for xi in out.split(',{"'):
                xi  = re.split("\[|:|,|\]", xi)
                ts  = datetime.datetime.fromtimestamp(float(xi[4]))
                row = [ts]; vd = True
                for i in range(5, 10):
                    if not vd and i == 9: row.append(0.0); continue
                    try: row.append(float(xi[i]))
                    except ValueError: vd = False; row.append(0.0)
                data.append(row)
            df = pd.DataFrame(data, columns=["datetime","open","high","low","close","volume"]).set_index("datetime")
            df.insert(0, "symbol", value=symbol)
            return df
        except AttributeError: return None

    @staticmethod
    def __format_symbol(symbol, exchange, contract=None):
        if ":" in symbol: pass
        elif contract is None: symbol = f"{exchange}:{symbol}"
        elif isinstance(contract, int): symbol = f"{exchange}:{symbol}{contract}!"
        else: raise ValueError("not a valid contract")
        return symbol

    def get_hist(self, symbol, exchange="NSE", interval=None, n_bars=10, fut_contract=None, extended_session=False):
        if interval is None: interval = Interval.in_daily
        symbol = self.__format_symbol(symbol, exchange, fut_contract)
        interval = interval.value
        self.__create_connection()
        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message("quote_set_fields", [self.session,"ch","chp","current_session","description",
            "local_description","language","exchange","fractional","is_tradable","lp","lp_time","minmov",
            "minmove2","original_name","pricescale","pro_name","short_name","type","update_mode","volume",
            "currency_code","rchp","rtc"])
        self.__send_message("quote_add_symbols", [self.session, symbol, {"flags": ["force_permission"]}])
        self.__send_message("quote_fast_symbols", [self.session, symbol])
        self.__send_message("resolve_symbol", [self.chart_session, "symbol_1",
            '={"symbol":"' + symbol + '","adjustment":"splits","session":' +
            ('"regular"' if not extended_session else '"extended"') + "}"])
        self.__send_message("create_series", [self.chart_session,"s1","s1","symbol_1",interval,n_bars])
        self.__send_message("switch_timezone", [self.chart_session, "exchange"])
        raw_data = ""
        while True:
            try:
                result = self.ws.recv(); raw_data += result + "\n"
            except Exception as e: logger.error(e); break
            if "series_completed" in result: break
        return self.__create_df(raw_data, symbol)


def timezone_adjustments(date1):
    from datetime import datetime as dt1, timedelta
    if dt1(2021,11,7,2,0,0) <= date1 <= dt1(2022,3,14,2,0,0):  return date1 - timedelta(hours=6)
    if dt1(2022,11,6,2,0,0) <= date1 <= dt1(2023,3,13,2,0,0):  return date1 - timedelta(hours=6)
    if dt1(2023,11,5,2,0,0) <= date1 <= dt1(2024,3,10,2,0,0):  return date1 - timedelta(hours=6)
    return date1 - timedelta(hours=5)


def cal_vwap(df):
    p = df.Price.values; q = df.Volume.values
    return df.assign(VWAP=(p * q).cumsum() / q.cumsum())


# =============================================================================
# ZIP HELPER — input layer only, no changes to processing logic
# =============================================================================

def _extract_from_zip(uploaded_zip, expected_extensions=('.csv', '.xlsx')):
    buf = io.BytesIO(uploaded_zip.read())
    uploaded_zip.seek(0)
    with zipfile.ZipFile(buf) as zf:
        for name in zf.namelist():
            if name.startswith('__MACOSX') or name.startswith('.'):
                continue
            if any(name.lower().endswith(ext) for ext in expected_extensions):
                inner = io.BytesIO(zf.read(name))
                inner.name = os.path.basename(name)
                return inner, os.path.basename(name)
    return None, None


def _resolve_upload(uploaded_file, expected_extensions=('.csv', '.xlsx')):
    if uploaded_file is None:
        return None
    if uploaded_file.name.lower().endswith('.zip'):
        inner, name = _extract_from_zip(uploaded_file, expected_extensions)
        if inner is None:
            st.warning(f'ZIP "{uploaded_file.name}" did not contain a recognised file '
                       f'({", ".join(expected_extensions)}). Please check the archive.')
        return inner
    return uploaded_file


# =============================================================================
# SECTION 2 — Core analysis function  (Cell 3 logic, unchanged)
# =============================================================================

def run_tick_analysis(
    dp_tick_file, mt5_tick_file, uk_tick_file, ic_tick_file,
    raw_report_file,
    mt_type, acc_currency, filter_id_list, filter_symbol,
    custom_orders, n_orders_to_check,
    plot_option, tick_provider_DP, tick_provider_MT5, tick_provider_UK, tick_provider_IC,
    use_adjusted_lag,
    tmp_dir,
):
    from datetime import datetime as datetime1, timedelta

    logs       = []
    figures    = {}
    excel_bufs = {}

    def log(msg):
        logs.append(msg)
        print(msg)

    dp_tick  = pd.read_csv(dp_tick_file,  names=['Date','Bid','Ask','Source'], header=None, low_memory=False)
    mt5_tick = pd.read_csv(mt5_tick_file, sep=';', skiprows=[0], encoding='UTF-16LE')
    try:
        uk_tick = pd.read_csv(uk_tick_file, sep=';', skiprows=[0], encoding='UTF-16LE')
    except:
        uk_tick = pd.read_csv(uk_tick_file, names=['Date','Bid','Ask','Source'], header=None, low_memory=False)
    ic_tick = pd.read_csv(ic_tick_file, sep=';', skiprows=[0], encoding='UTF-16LE')

    dp_tick  = dp_tick.drop('Source', axis=1)
    mt5_tick = mt5_tick.drop(['Last','Volume'], axis=1)
    try:    uk_tick = uk_tick.drop(['Last','Volume'], axis=1)
    except: uk_tick = uk_tick.drop('Source', axis=1)
    ic_tick = ic_tick.drop(['Last','Volume'], axis=1)

    for df in [dp_tick, mt5_tick, uk_tick, ic_tick]:
        df['Date'] = pd.to_datetime(df['Date'])

    dp_tick_min_date = dp_tick['Date'].min().strftime('%Y-%m-%d %H:%M:%S')
    dp_tick_max_date = dp_tick['Date'].max().strftime('%Y-%m-%d %H:%M:%S')

    seconds = pd.DataFrame()
    seconds['Date'] = pd.date_range(dp_tick_min_date, end=dp_tick_max_date, freq='s')
    seconds['place_holder'] = '0'
    seconds['Date'] = seconds['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    seconds = seconds.set_index('Date')

    for df in [dp_tick, mt5_tick, uk_tick, ic_tick]:
        df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    if mt_type == 'MT4':   dp_tick_precise = dp_tick.copy()
    elif mt_type == 'MT5': dp_tick_precise = mt5_tick.copy()
    else: raise ValueError('mt_type accepts "MT4" or "MT5" only!')

    dp_tick  = dp_tick.groupby('Date')[['Bid','Ask']].mean()
    mt5_tick = mt5_tick.groupby('Date')[['Bid','Ask']].mean()
    uk_tick  = uk_tick.groupby('Date')[['Bid','Ask']].mean()
    ic_tick  = ic_tick.groupby('Date')[['Bid','Ask']].mean()
    dp_tick  = dp_tick.rename({'Bid':'DP Bid',  'Ask':'DP Ask'},  axis=1)
    mt5_tick = mt5_tick.rename({'Bid':'MT5 Bid','Ask':'MT5 Ask'}, axis=1)
    uk_tick  = uk_tick.rename({'Bid':'UK Bid',  'Ask':'UK Ask'},  axis=1)
    ic_tick  = ic_tick.rename({'Bid':'IC Bid',  'Ask':'IC Ask'},  axis=1)

    combined_tick = pd.merge(dp_tick, mt5_tick, left_index=True, right_index=True, how='outer')
    combined_tick = pd.merge(combined_tick, uk_tick,  left_index=True, right_index=True, how='outer')
    combined_tick = pd.merge(combined_tick, ic_tick,  left_index=True, right_index=True, how='outer')
    combined_tick = pd.merge(combined_tick, seconds,  left_index=True, right_index=True, how='outer')
    combined_tick = combined_tick.drop('place_holder', axis=1).sort_index()
    combined_tick = combined_tick.reset_index()
    mask = (combined_tick['Date'] >= dp_tick_min_date) & (combined_tick['Date'] <= dp_tick_max_date)
    combined_tick = combined_tick.loc[mask].set_index('Date')

    log(datetime1.now().strftime("%Y-%m-%d %H:%M:%S") + ' Finished loading and processing tick data')

    dp_tick1           = dp_tick.reset_index()
    tick_cut_off_start = dp_tick1['Date'][0]
    tick_cut_off_end   = dp_tick1['Date'][-1:].values[0]

    if not isinstance(filter_id_list, list):
        raise ValueError('Please make sure filter_id_list is a list.')

    for filter_id in filter_id_list:
        log('\n' + '='*80 + '\nMT ID: ' + str(filter_id) + '\n' + '='*80)
        figs_for_id = []

        excel_buf = io.BytesIO()
        writer    = pd.ExcelWriter(excel_buf, engine='xlsxwriter')
        empty_df  = pd.DataFrame()
        empty_df.to_excel(writer, sheet_name='Summary', index=False)

        try:
            combined_tick.to_excel(writer, sheet_name='Tick Data', index=True)
        except:
            log('Warning: Tick data too large for Excel sheet.')

        mt_trades = pd.read_excel(raw_report_file, engine='openpyxl')
        mt_trades = mt_trades[mt_trades['login'] == filter_id]

        if acc_currency == 'USC':
            for col in ['volume','commission','swap','profit']:
                mt_trades[col] = round(mt_trades[col] / 100, 2)

        mt_trades.to_excel(writer, sheet_name='Trade History', index=False)
        mt_trades['symbol'] = mt_trades['symbol'].str.replace('.s','',regex=False).str.replace('.c','',regex=False)
        mt_trades['open_time']  = pd.to_datetime(mt_trades['open_time'])
        mt_trades['close_time'] = pd.to_datetime(mt_trades['close_time'])
        mt_trades['Duration']   = (mt_trades['close_time'] - mt_trades['open_time']) / pd.Timedelta(minutes=1)

        mt_trades_temp = mt_trades.drop(['ticket','login','open_price','close_price','SL','TP','comment','reason'],axis=1).copy()
        log('\nSummary of Descriptive Statistics:\n' + tabulate(round(mt_trades_temp.describe().T, 4), headers='keys', tablefmt='psql', floatfmt=',.2f'))

        mt_trades['Net P&L'] = mt_trades['commission'] + mt_trades['swap'] + mt_trades['profit']
        columns = {'ticket':'Trade','volume':'Vol','commission':'Comm','swap':'Swap',
                   'profit':'Gross P&L','Net P&L':'Net P&L','Duration':'Avg Dur'}
        mt_trades_gb = mt_trades.groupby(['symbol','reason'], as_index=False).agg(
            {'ticket':'count','volume':'sum','commission':'sum','swap':'sum',
             'profit':'sum','Net P&L':'sum','Duration':'mean'}).rename(columns=columns)
        total_row    = mt_trades_gb.sum().rename('Total')
        mt_trades_gb = pd.concat([mt_trades_gb, total_row.to_frame().T], ignore_index=True)
        mt_trades_gb.loc[mt_trades_gb.index[-1],'Avg Dur'] = mt_trades['Duration'].mean()
        c = 0
        for name, group in mt_trades.groupby(['symbol','reason']):
            mt_trades_gb.at[c,'Win Rate'] = round(len(group[group['profit']>0])/len(group['profit'])*100,2); c+=1
        mt_trades_gb.loc[mt_trades_gb.index[-1],'Win Rate'] = round(len(mt_trades[mt_trades['profit']>0])/len(mt_trades['profit'])*100,2)
        mt_trades_gb.loc[mt_trades_gb.index[-1],'symbol'] = 'Total'
        mt_trades_gb.loc[mt_trades_gb.index[-1],'reason'] = ''
        log('\nTrading Volume and P&L by Symbol and Reason:\n' + tabulate(mt_trades_gb, headers='keys', tablefmt='psql', floatfmt=',.2f', showindex=False))

        mt_trades['YYYY-MM'] = mt_trades['close_time'].apply(lambda x: x.strftime('%Y-%m'))
        monthly_pnl = mt_trades.groupby('YYYY-MM', as_index=False).agg({'volume':'sum','Net P&L':'sum'})
        c = 0
        for name, group in mt_trades.groupby('YYYY-MM'):
            monthly_pnl.at[c,'Win Rate'] = round(len(group[group['profit']>0])/len(group['profit'])*100,2); c+=1
        log('\nTrading P&L by Month:\n' + tabulate(monthly_pnl, headers='keys', tablefmt='psql', floatfmt=',.2f', showindex=False))
        log('\nNote 1: Trade range from ' + str(mt_trades['open_time'].min()) + ' to ' + str(mt_trades['close_time'].max()) + '.')

        mt_trades = mt_trades[mt_trades['open_time']  > tick_cut_off_start]
        mt_trades = mt_trades[mt_trades['close_time'] < tick_cut_off_end]

        open_trade   = mt_trades[['login','ticket','open_time','type','volume','symbol','open_price','reason','comment']].copy()
        open_trade['entry'] = 'In'
        closed_trade = mt_trades[['login','ticket','close_time','type','volume','symbol','close_price','reason','commission','swap','profit','Duration','comment']].copy()
        closed_trade['entry'] = 'Out'
        closed_trade = closed_trade[~(closed_trade['close_time']=='NaT')]
        closed_trade = closed_trade.rename({'type':'type1'},axis=1)
        closed_trade['type'] = np.where(closed_trade['type1']=='buy','sell','buy')
        closed_trade = closed_trade.drop('type1',axis=1)
        open_trade   = open_trade.rename({'open_time':'Executed Time','open_price':'Price','symbol':'Symbol'},axis=1)
        closed_trade = closed_trade.rename({'close_time':'Executed Time','close_price':'Price','symbol':'Symbol'},axis=1)
        trade = pd.concat([open_trade, closed_trade], ignore_index=False).sort_values('Executed Time')
        trade.to_excel(writer, sheet_name='Processed Trades', index=False)

        combined_tick1 = combined_tick.copy().reset_index()
        combined_tick1['Date'] = pd.to_datetime(combined_tick1['Date'])
        mask  = ((trade['Executed Time'] >= combined_tick1['Date'].values[0]) &
                 (trade['Executed Time'] <= combined_tick1['Date'].values[-1]))
        trade = trade.loc[mask]
        trade = trade.rename({'volume':'Volume','swap':'Swap','type':'Type','reason':'Reason'},axis=1)
        trade = trade[trade['Symbol']==filter_symbol]

        trade_precise = trade.copy()
        trade_precise['Executed Time'] = trade_precise['Executed Time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        trade_precise = trade_precise.rename({'Executed Time':'Date'},axis=1)
        trade_precise['Volume'] = trade_precise['Volume'].where(trade_precise['Type'].ne('sell'),-trade_precise['Volume'])
        trade_precise = trade_precise.sort_values('Date',kind='mergesort')
        trade_precise['Open_Positions']  = trade_precise['Volume'].round(2).cumsum().round(2)
        trade_precise['Duplicate Check'] = trade_precise['ticket'].astype(str) + '_' + trade_precise['entry']

        _dp = dp_tick_precise.set_index('Date')
        _dp['Sequence'] = _dp.groupby(level='Date').cumcount()
        for n in [1,5,10]:
            _dp[f'Bid d({n})'] = _dp['Bid'].diff(periods=n)
            _dp[f'Ask d({n})'] = _dp['Ask'].diff(periods=n)
        _dp['Bid Avg(d(5))']  = _dp['Bid d(5)'].rolling(5).mean()
        _dp['Bid Avg(d(10))'] = _dp['Bid d(10)'].rolling(10).mean()
        _dp['Ask Avg(d(5))']  = _dp['Ask d(5)'].rolling(5).mean()
        _dp['Ask Avg(d(10))'] = _dp['Ask d(10)'].rolling(10).mean()
        _dp = _dp.reset_index()

        tick_and_trade = pd.merge(_dp, trade_precise, on='Date', how='outer')
        tick_and_trade = tick_and_trade.set_index('Date').sort_values('Sequence').sort_index().reset_index()
        duplicated     = tick_and_trade.duplicated('Duplicate Check',keep='first')
        columns_to_nan = [c for c in trade_precise.columns if c != 'Date']
        tick_and_trade.loc[duplicated, columns_to_nan] = np.nan

        scope_idx = tick_and_trade.loc[tick_and_trade.ticket.notna()].index
        processed_tick_and_trade = pd.concat(
            [tick_and_trade.iloc[max(0,scope_idx[i]-30):scope_idx[i]+30] for i in range(len(scope_idx))]
        )
        processed_tick_and_trade = processed_tick_and_trade[~processed_tick_and_trade.index.duplicated(keep='first')]

        for n in [1,5,10]:
            processed_tick_and_trade[f'Shifted Bid d({n})'] = processed_tick_and_trade[f'Bid d({n})'].shift(-n).bfill()
            processed_tick_and_trade[f'Shifted Ask d({n})'] = processed_tick_and_trade[f'Ask d({n})'].shift(-n).bfill()
        for td in ['1','5','10']:
            processed_tick_and_trade['Check d('+td+')'] = np.where(
                (processed_tick_and_trade['Type']=='buy')  & (processed_tick_and_trade['Shifted Ask d('+td+')']>0),'Bad',
                np.where((processed_tick_and_trade['Type']=='sell') & (processed_tick_and_trade['Shifted Bid d('+td+')']<0),'Bad','Good'))
        processed_tick_and_trade['Flag'] = np.where(
            processed_tick_and_trade[['Check d(1)','Check d(5)','Check d(10)']].eq('Bad').any(axis=1),'Bad','Good')
        processed_tick_and_trade['Avg_d(Ask)'] = processed_tick_and_trade[['Shifted Ask d(1)','Shifted Ask d(5)','Shifted Ask d(10)']].mean(axis=1)
        processed_tick_and_trade['Avg_d(Bid)'] = processed_tick_and_trade[['Shifted Bid d(1)','Shifted Bid d(5)','Shifted Bid d(10)']].mean(axis=1)
        processed_tick_and_trade['Max_d(Ask)'] = np.where(
            (processed_tick_and_trade['Type']=='buy') & (processed_tick_and_trade['Flag']=='Bad'),
            processed_tick_and_trade[['Shifted Ask d(1)','Shifted Ask d(5)','Shifted Ask d(10)']].max(axis=1),
            processed_tick_and_trade[['Shifted Ask d(1)','Shifted Ask d(5)','Shifted Ask d(10)']].min(axis=1))
        processed_tick_and_trade['Max_d(Bid)'] = np.where(
            (processed_tick_and_trade['Type']=='sell') & (processed_tick_and_trade['Flag']=='Bad'),
            processed_tick_and_trade[['Shifted Bid d(1)','Shifted Bid d(5)','Shifted Bid d(10)']].min(axis=1),
            processed_tick_and_trade[['Shifted Bid d(1)','Shifted Bid d(5)','Shifted Bid d(10)']].max(axis=1))
        for col in ['Check d(1)','Check d(5)','Check d(10)','Flag']:
            processed_tick_and_trade.loc[processed_tick_and_trade['Type'].isnull(), col] = ''

        processed_tick_and_trade.to_excel(writer, sheet_name='DP Tick and Trades', index=False)
        processed_tick_and_trade[processed_tick_and_trade['ticket'].notna()].to_excel(writer, sheet_name='Front Running Data', index=False)

        trade['Net P&L'] = trade['commission'] + trade['Swap'] + trade['profit']
        trade['Profitability Ratio'] = trade['Net P&L'] / trade['Duration'] / trade['Volume']
        trade  = trade.replace([np.inf,-np.inf],0).sort_values('Profitability Ratio',ascending=False)
        trade1 = trade[trade['Profitability Ratio'].notna()].copy()
        trade2 = trade.sort_values('Net P&L',ascending=False); trade2 = trade2[trade2['Profitability Ratio'].notna()]
        trade3 = trade[trade['Duration'].notna()].sort_values('Duration')

        ratio_top_orders         = trade1.ticket.tolist()[:n_orders_to_check]
        ratio_bottom_orders      = trade1.ticket.tolist()[-n_orders_to_check:][::-1]
        best_orders              = trade2.ticket.tolist()[:n_orders_to_check]
        worst_orders             = trade2.ticket.tolist()[-n_orders_to_check:][::-1]
        shortest_duration_orders = trade3.ticket.tolist()[:n_orders_to_check]

        sheet_map = {1:'Custom Orders',2:'Highest Ratio',3:'Lowest Ratio',
                     4:'Highest Net P&L',5:'Lowest Net P&L',6:'Shortest Duration'}
        for sheet in sheet_map.values(): empty_df.to_excel(writer, sheet_name=sheet)

        list_to_plot = [custom_orders,[ratio_top_orders],ratio_bottom_orders,
                        best_orders,worst_orders,shortest_duration_orders]
        labels = {1:'Custom Trades',2:'Highest Net P&L / Duration / Volume Ratio',
                  3:'Lowest Net P&L / Duration / Volume Ratio',4:'Highest Net P&L',
                  5:'Lowest Net P&L',6:'Shortest Duration'}

        count     = 0
        saved_img = False
        row_number = 1

        for list_orders in list_to_plot:
            try: list_orders = [item for sublist in list_orders for item in sublist]
            except: pass
            count += 1
            log('\n--- ' + labels[count] + ' ---')
            log('Ticket ID: ' + str(list_orders))

            for rank, order_id in enumerate(list_orders):
                trade_to_plot = trade[trade['ticket']==order_id].sort_values('entry')
                if len(trade_to_plot) < 2:
                    log('Warning: Not enough trades for order ' + str(order_id)); continue
                list_of_time = trade_to_plot['Executed Time'].tolist()
                try: open_time = list_of_time[0]; close_time = list_of_time[-1]
                except: log('Ticket ID ' + str(order_id) + ' does not exist.'); continue

                duration    = round(trade_to_plot['Duration'].tolist()[-1], 2)
                trade_in    = trade_to_plot['Type'].tolist()[0]
                trade_out   = trade_to_plot['Type'].tolist()[-1]
                open_color  = 'green' if trade_in  == 'buy' else 'red'
                close_color = 'red'   if trade_in  == 'buy' else 'green'
                trade_vol   = trade_to_plot['Volume'].tolist()[-1]

                if (datetime1.strptime('00:00:00',"%H:%M:%S").time() <=
                        datetime1.strptime(str(list_of_time[0]),"%Y-%m-%d %H:%M:%S").time() <=
                        datetime1.strptime('00:15:00',"%H:%M:%S").time()):
                    open_time1  = list_of_time[0] - timedelta(minutes=15)
                    close_time1 = list_of_time[-1] + timedelta(minutes=3)
                    marker_plot = '.' if duration < 60 else None
                else:
                    if duration > 15:
                        open_time1  = list_of_time[0] - timedelta(seconds=duration*60/15)
                        close_time1 = list_of_time[-1] + timedelta(seconds=duration*60/15)
                        marker_plot = None
                    else:
                        open_time1  = list_of_time[0] - timedelta(seconds=30)
                        close_time1 = list_of_time[-1] + timedelta(seconds=30)
                        marker_plot = '.'

                mask      = (combined_tick1['Date']>=open_time1) & (combined_tick1['Date']<=close_time1)
                temp_tick = combined_tick1.loc[mask]
                if len(temp_tick) < 5: log('Note: Tick data does not cover order ' + str(order_id) + '.'); continue

                if duration > 120:
                    temp_tick = temp_tick.copy()
                    temp_tick['Lag'] = 0; temp_tick['Lag_adjusted'] = 0
                else:
                    temp_tick = temp_tick.set_index('Date')
                    temp_tick_diff = temp_tick.diff().fillna(0)
                    if mt_type == 'MT4':
                        temp_tick_diff['DP_diff']     = temp_tick_diff[['DP Bid','DP Ask']].mean(axis=1)
                        temp_tick_diff['Others_diff'] = temp_tick_diff[['MT5 Bid','MT5 Ask','UK Bid','UK Ask','IC Bid','IC Ask']].mean(axis=1)
                    else:
                        temp_tick_diff['DP_diff']     = temp_tick_diff[['MT5 Bid','MT5 Ask']].mean(axis=1)
                        temp_tick_diff['Others_diff'] = temp_tick_diff[['DP Bid','DP Ask','UK Bid','UK Ask','IC Bid','IC Ask']].mean(axis=1)
                    temp_tick_diff_new = pd.DataFrame()
                    temp_tick_diff = temp_tick_diff.reset_index()
                    temp_tick_diff['Date'] = pd.to_datetime(temp_tick_diff['Date'])
                    search_time = '20s'; temp_no = 0
                    for name, group in temp_tick_diff.groupby(pd.Grouper(key='Date',freq=search_time)):
                        corr = signal.correlate(group['DP_diff'],group['Others_diff'])
                        lags = signal.correlation_lags(group['DP_diff'].size,group['Others_diff'].size,mode="full")
                        lag  = lags[np.argmax(corr)]
                        group = group.copy()
                        group['Lag'] = lag
                        group['Lag_adjusted'] = lag if lag==temp_no else 0
                        temp_no = lag
                        temp_tick_diff_new = pd.concat([temp_tick_diff_new,group],ignore_index=True)
                    temp_tick_diff_new.loc[temp_tick_diff_new.Lag==-int(search_time[:-1])+1,'Lag'] = 0
                    temp_tick_diff_new.loc[temp_tick_diff_new.Lag_adjusted==-int(search_time[:-1])+1,'Lag_adjusted'] = 0
                    temp_tick_diff_new['Date'] = pd.to_datetime(temp_tick_diff_new['Date'],format='%Y-%m-%d %H:%M:%S')
                    temp_tick_diff_new = temp_tick_diff_new.set_index('Date')[['Lag','Lag_adjusted']]
                    temp_tick = pd.merge(temp_tick,temp_tick_diff_new,left_index=True,right_index=True,how='outer')
                    temp_tick = temp_tick.reset_index()
                    tt_min = temp_tick['Date'].min().strftime('%Y-%m-%d %H:%M:%S').replace(':','.')
                    tt_max = temp_tick['Date'].max().strftime('%Y-%m-%d %H:%M:%S').replace(':','.')
                    temp_tick.to_csv(os.path.join(tmp_dir, str(order_id)+' '+filter_symbol+' '+tt_min+'_'+tt_max+'.csv'),index=False)

                price_in     = trade_to_plot['Price'].tolist()[0]
                price_out    = trade_to_plot['Price'].tolist()[-1]
                commission   = round(trade_to_plot['commission'].tolist()[-1],2)
                swap         = round(trade_to_plot['Swap'].tolist()[-1],2)
                gross_profit = round(trade_to_plot['profit'].tolist()[-1],2)
                net_profit   = round(trade_to_plot['Net P&L'].tolist()[-1],2)
                reason       = str(trade_to_plot['Reason'].tolist()[-1])
                comment      = str(trade_to_plot['comment'].tolist()[-1])
                if comment=='nan': comment='N/A'
                pro_ratio_v = trade2[trade2['ticket']==order_id]['Profitability Ratio'].values
                pro_ratio   = round(pro_ratio_v[0],2) if len(pro_ratio_v)>0 else 'N/A'

                lead_lag_text = (
                    '\nLead/Lag: None (Duration > 120 min)' if duration > 120 else
                    '\nLead/Lag: Cross Correlation; Interval: ' + search_time.lower()
                )

                fig, (ax1, ax2) = plt.subplots(2,1,height_ratios=[5,1],figsize=(20,10))
                fig.subplots_adjust(hspace=0.00)
                fig.suptitle(
                    'MT ID: '+str(filter_id)+'; Ticket: '+str(order_id)+'; Duration: '+str(duration)+
                    ' min; Reason: '+reason+'; Comment: '+comment+'; P.Ratio: '+str(pro_ratio)+
                    '\nIn: '+str(trade_in)+' '+filter_symbol+' '+str(trade_vol)+' lot @ '+str(price_in)+' at '+str(open_time)+
                    '\nOut: '+str(trade_out)+' '+filter_symbol+' '+str(trade_vol)+' lot @ '+str(price_out)+' at '+str(close_time)+
                    '\nGross: '+str(gross_profit)+' Comm: '+str(commission)+' Swap: '+str(swap)+' Net: '+str(net_profit)+' USD'+
                    lead_lag_text
                )
                ax1.ticklabel_format(useOffset=False)

                providers = [tick_provider_DP,tick_provider_MT5,tick_provider_UK,tick_provider_IC]
                if plot_option in ['Bid','Ask']:
                    cols_colors = {
                        'Bid':[('DP Bid','tab:blue'),('MT5 Bid','tab:orange'),('UK Bid','tab:green'),('IC Bid','tab:red')],
                        'Ask':[('DP Ask','tab:blue'),('MT5 Ask','tab:orange'),('UK Ask','tab:green'),('IC Ask','tab:red')],
                    }
                    for (col,color),show in zip(cols_colors[plot_option],providers):
                        if show: ax1.plot(temp_tick['Date'],temp_tick[col],marker=marker_plot,label=col,color=color,alpha=0.7,zorder=10 if 'DP' in col else 1)
                elif plot_option == 'BidAsk':
                    for (bc,ac,color),show in zip(
                        [('DP Bid','DP Ask','tab:blue'),('MT5 Bid','MT5 Ask','tab:orange'),('UK Bid','UK Ask','tab:green'),('IC Bid','IC Ask','tab:red')],providers):
                        if show:
                            ax1.plot(temp_tick['Date'],temp_tick[bc],marker=marker_plot,label=bc,color=color,alpha=0.7)
                            ax1.plot(temp_tick['Date'],temp_tick[ac],marker=marker_plot,linestyle='dashed',label=ac,color=color,alpha=0.7)
                else:
                    plt.close(); raise ValueError("plot_option must be 'Bid','Ask', or 'BidAsk'")

                ax1.axvline(x=open_time, color=open_color, linestyle='-',linewidth=2,alpha=0.7)
                ax1.axhline(y=price_in,  color=open_color, linestyle='-',linewidth=2,alpha=0.7)
                ax1.axvline(x=close_time,color=close_color,linestyle='-',linewidth=2,alpha=0.7)
                ax1.axhline(y=price_out, color=close_color,linestyle='-',linewidth=2,alpha=0.7)
                ax1.grid()

                ax2.ticklabel_format(useOffset=False)
                pos_tmp = temp_tick.copy(); pos_tmp[pos_tmp['Lag']<0] = np.nan
                neg_tmp = temp_tick.copy(); neg_tmp[neg_tmp['Lag']>0] = np.nan
                lag_col = 'Lag_adjusted' if use_adjusted_lag else 'Lag'
                ax2.fill_between(pos_tmp['Date'],pos_tmp[lag_col],label='Lag', lw=0.5,color='tab:red',  alpha=1)
                ax2.fill_between(neg_tmp['Date'],neg_tmp[lag_col],label='Lead',lw=0.5,color='tab:green',alpha=1)
                ax2.axhline(0,color='black',lw=1)
                ax2.yaxis.set_major_formatter(FormatStrFormatter('%.0f'))
                yabs_max = abs(max(ax2.get_ylim(),key=abs))
                ax2.axis(ymin=-yabs_max,ymax=yabs_max)
                ax2.set_ylabel('Second',rotation=90); ax2.grid()

                bid_col_dp = 'DP Bid'  if mt_type=='MT4' else 'MT5 Bid'
                ask_col_dp = 'DP Ask'  if mt_type=='MT4' else 'MT5 Ask'
                prefix     = 'DP MT4'  if mt_type=='MT4' else 'DP MT5'
                price_feed_open  = prefix + ' Price Feed (Open): Normal'
                price_feed_close = prefix + ' Price Feed (Close): Normal'

                for is_open,t_type,exe_time,price,feed_key in [
                    (True, trade_in, open_time, price_in,'Open'),
                    (False,trade_out,close_time,price_out,'Close')]:
                    tick_col = ask_col_dp if t_type=='buy' else bid_col_dp
                    try:
                        tp = temp_tick.loc[temp_tick['Date']==exe_time,tick_col].values[0]
                        if pd.isna(tp): raise ValueError
                    except:
                        tmp_dp = temp_tick[['Date',bid_col_dp,ask_col_dp]].dropna()
                        last   = tmp_dp[tmp_dp['Date']<exe_time].tail(1)
                        tp     = last[tick_col].values[0]
                        diff   = (last['Date'].values[0]-exe_time)/pd.Timedelta(seconds=1)
                        msg    = prefix+f' Price Feed ({feed_key}): Last tick was '+str(datetime1.strptime(str(last['Date'].values[0])[:-4],'%Y-%m-%dT%H:%M:%S.%f'))+', which is '+str(diff)+' second(s) of difference.'
                        if is_open: price_feed_open  = msg
                        else:       price_feed_close = msg
                    color_shade = 'green' if ((t_type=='buy' and price>tp) or (t_type=='sell' and price<tp)) else 'red'
                    where_mask  = (temp_tick['Date']<=exe_time) if is_open else (temp_tick['Date']>=exe_time)
                    ax1.fill_between(temp_tick['Date'],price,tp,where=where_mask,facecolor=color_shade,alpha=0.3)

                wb_color = 'green' if (price_feed_open==prefix+' Price Feed (Open): Normal' and price_feed_close==prefix+' Price Feed (Close): Normal') else 'red'
                plt.figtext(0.5,0.03,price_feed_open+'\n'+price_feed_close,ha='center',fontsize=11,
                            bbox={'facecolor':wb_color,'alpha':0.3,'pad':5})

                for _ax in [ax1,ax2]:
                    _ax.grid(visible=True,which='minor',color='grey',linestyle='--',alpha=0.25)
                    _ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
                    _ax.margins(x=0)
                if duration <= 4:
                    ax1.xaxis.set_minor_locator(mdates.SecondLocator())
                    ax2.xaxis.set_minor_locator(mdates.SecondLocator())
                ax1.legend(); ax2.legend()
                ax1.tick_params(axis='x',which='major',bottom=False,top=False,labelbottom=False)

                sheet_name  = sheet_map[count]
                figure_name = sheet_map[count]+' ('+str(rank+1)+') - '+filter_symbol+' '+str(order_id)
                fig_path    = os.path.join(tmp_dir, figure_name + '.png')
                plt.savefig(fig_path, dpi=150, bbox_inches='tight')
                figs_for_id.append((figure_name, fig))

                worksheet = writer.sheets[sheet_name]
                if not saved_img: row_number = 1
                else:             row_number += 38
                worksheet.write(row_number-1,0,'Ticket ID:')
                worksheet.write(row_number-1,1,str(order_id))
                row_number += 1
                worksheet.insert_image('A'+str(row_number), fig_path, {'x_scale':0.8,'y_scale':0.8})
                saved_img = True
                plt.close(fig)
            saved_img = False

        writer.close()
        excel_buf.seek(0)
        excel_bufs[filter_id] = excel_buf
        figures[filter_id]    = figs_for_id
        log('\nSuccessfully generated Excel for MT ID ' + str(filter_id))

    return excel_bufs, figures, '\n'.join(logs)


# =============================================================================
# SECTION 3 — Streamlit UI
# =============================================================================

st.set_page_config(page_title='Trade Analysis', layout='wide')

# -----------------------------------------------------------------------------
# STICKY FLOATING NAV BAR — UI only, no logic changes
# -----------------------------------------------------------------------------
NAV_HTML = """
<style>
#trade-nav {
    position: fixed;
    top: 70px;
    right: 20px;
    z-index: 99999;
    background: rgba(20, 22, 34, 0.95);
    border: 1px solid rgba(255,255,255,0.13);
    border-radius: 12px;
    padding: 10px 8px;
    box-shadow: 0 6px 24px rgba(0,0,0,0.55);
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 158px;
    font-family: "Source Sans Pro", sans-serif;
}
#trade-nav .nav-label {
    font-size: 10px;
    color: rgba(255,255,255,0.35);
    text-transform: uppercase;
    letter-spacing: 0.09em;
    padding: 2px 8px 5px 8px;
    user-select: none;
}
#trade-nav a {
    display: flex;
    align-items: center;
    gap: 7px;
    color: rgba(255,255,255,0.80);
    text-decoration: none !important;
    font-size: 12.5px;
    padding: 6px 10px;
    border-radius: 7px;
    transition: background 0.15s, color 0.15s;
    cursor: pointer;
    white-space: nowrap;
}
#trade-nav a:hover {
    background: rgba(255,255,255,0.10);
    color: #ffffff;
}
#trade-nav a.nav-active {
    background: rgba(99,179,237,0.20);
    color: #90cdf4;
    font-weight: 600;
}
#trade-nav hr.nav-divider {
    border: none;
    border-top: 1px solid rgba(255,255,255,0.09);
    margin: 3px 4px;
}
</style>

<div id="trade-nav">
  <div class="nav-label">Navigation</div>
  <a href="#" onclick="navJump('nav-top');return false;">&#8679; Top</a>
  <hr class="nav-divider"/>
  <a href="#" onclick="navJump('nav-upload');return false;">&#128193; Upload / Inputs</a>
  <a href="#" onclick="navJump('nav-charts');return false;">&#128202; Charts</a>
  <a href="#" onclick="navJump('nav-log');return false;">&#128203; Analysis Log</a>
  <a href="#" onclick="navJump('nav-download');return false;">&#11015; Download</a>
</div>

<script>
function navJump(id) {
    var el = document.getElementById(id);
    if (!el) { try { el = window.parent.document.getElementById(id); } catch(e) {} }
    if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
}
(function() {
    var ids  = ['nav-top','nav-upload','nav-charts','nav-log','nav-download'];
    var navLinks = document.querySelectorAll('#trade-nav a');
    function setActive() {
        var current = ids[0];
        ids.forEach(function(id) {
            var el = document.getElementById(id);
            if (!el) { try { el = window.parent.document.getElementById(id); } catch(e) {} }
            if (el && el.getBoundingClientRect().top <= 140) { current = id; }
        });
        navLinks.forEach(function(a, i) {
            a.classList.toggle('nav-active', ids[i] === current);
        });
    }
    window.addEventListener('scroll', setActive, { passive: true });
    try { window.parent.addEventListener('scroll', setActive, { passive: true }); } catch(e) {}
    setInterval(setActive, 400);
    setActive();
})();
</script>
"""

def _anchor(anchor_id):
    st.markdown(
        f'<div id="{anchor_id}" style="position:relative;top:-80px;'
        f'visibility:hidden;pointer-events:none;"></div>',
        unsafe_allow_html=True
    )

st.markdown(NAV_HTML, unsafe_allow_html=True)

# ── Top anchor ────────────────────────────────────────────────────────────────
_anchor('nav-top')
st.title('Trade Analysis')

# ── sidebar inputs ────────────────────────────────────────────────────────────
with st.sidebar:
    st.header('Configuration')
    mt_type      = st.selectbox('MT Type',    ['MT4','MT5'])
    acc_currency = st.selectbox('Account Currency', ['USD','USC'])
    filter_id_input  = st.text_input('MT ID(s) — comma separated', '10024717')
    filter_symbol    = st.text_input('Symbol', 'XAUUSD').strip().upper()
    custom_orders_in = st.text_input('Custom Order IDs (comma separated, optional)', '')
    n_orders_to_check = st.number_input('Orders to check per category', min_value=1, value=20, step=1)
    plot_option  = st.selectbox('Plot Option', ['Bid','Ask','BidAsk'])
    tick_provider_DP  = st.checkbox('Show DP tick',  value=True)
    tick_provider_MT5 = st.checkbox('Show MT5 tick', value=True)
    tick_provider_UK  = st.checkbox('Show UK tick',  value=True)
    tick_provider_IC  = st.checkbox('Show IC tick',  value=True)
    use_adjusted_lag  = st.checkbox('Use adjusted lag', value=True)

filter_id_list = [int(x.strip()) for x in filter_id_input.split(',') if x.strip().isdigit()]
custom_orders  = [int(x.strip()) for x in custom_orders_in.split(',') if x.strip().isdigit()]

# ── Upload / Inputs anchor ────────────────────────────────────────────────────
_anchor('nav-upload')
st.header('Upload Files')
col1, col2 = st.columns(2)
with col1:
    dp_file  = st.file_uploader('DP tick file (CSV or ZIP)',  type=['csv','zip'], key='dp')
    mt5_file = st.file_uploader('MT5 tick file (CSV or ZIP)', type=['csv','zip'], key='mt5')
    uk_file  = st.file_uploader('UK tick file (CSV or ZIP)',  type=['csv','zip'], key='uk')
with col2:
    ic_file  = st.file_uploader('IC tick file (CSV or ZIP)',  type=['csv','zip'], key='ic')
    raw_file = st.file_uploader('Raw Report (xlsx or ZIP)',   type=['xlsx','zip'], key='raw')

all_uploaded = all([dp_file, mt5_file, uk_file, ic_file, raw_file])

# ── run button ────────────────────────────────────────────────────────────────
if st.button('Run Analysis', disabled=not all_uploaded):
    if not filter_id_list:
        st.error('Please enter at least one valid numeric MT ID.')
        st.stop()

    # Resolve ZIP uploads inside the button block only
    dp_file_resolved  = _resolve_upload(dp_file,  expected_extensions=('.csv',))
    mt5_file_resolved = _resolve_upload(mt5_file, expected_extensions=('.csv',))
    uk_file_resolved  = _resolve_upload(uk_file,  expected_extensions=('.csv',))
    ic_file_resolved  = _resolve_upload(ic_file,  expected_extensions=('.csv',))
    raw_file_resolved = _resolve_upload(raw_file, expected_extensions=('.xlsx',))

    if not all([dp_file_resolved, mt5_file_resolved,
                uk_file_resolved, ic_file_resolved, raw_file_resolved]):
        st.error('One or more ZIP files did not contain a recognised file. '
                 'Please check your uploads.')
        st.stop()

    with st.spinner('Running analysis... this may take a few minutes.'):
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                excel_bufs, figures, log_text = run_tick_analysis(
                    dp_tick_file=dp_file_resolved,
                    mt5_tick_file=mt5_file_resolved,
                    uk_tick_file=uk_file_resolved,
                    ic_tick_file=ic_file_resolved,
                    raw_report_file=raw_file_resolved,
                    mt_type=mt_type,
                    acc_currency=acc_currency,
                    filter_id_list=filter_id_list,
                    filter_symbol=filter_symbol,
                    custom_orders=custom_orders,
                    n_orders_to_check=int(n_orders_to_check),
                    plot_option=plot_option,
                    tick_provider_DP=tick_provider_DP,
                    tick_provider_MT5=tick_provider_MT5,
                    tick_provider_UK=tick_provider_UK,
                    tick_provider_IC=tick_provider_IC,
                    use_adjusted_lag=use_adjusted_lag,
                    tmp_dir=tmp_dir,
                )

                st.success('Analysis complete!')

                # ── Charts anchor ─────────────────────────────────────────────
                _anchor('nav-charts')
                st.header('Price Feed Charts')
                for fid, fig_list in figures.items():
                    st.subheader('MT ID: ' + str(fid))
                    for name, fig in fig_list:
                        st.write('**' + name + '**')
                        st.pyplot(fig)
                        plt.close(fig)

                # ── Analysis Log anchor ───────────────────────────────────────
                _anchor('nav-log')
                with st.expander('Analysis Log'):
                    st.text(log_text)

                # ── Download anchor ───────────────────────────────────────────
                _anchor('nav-download')
                st.header('Download Results')
                for fid, buf in excel_bufs.items():
                    fname = 'DP_' + str(fid) + '_Orders Analysis_' + filter_symbol + '_Raw.xlsx'
                    st.download_button(
                        label='Download Excel — MT ID ' + str(fid),
                        data=buf,
                        file_name=fname,
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        key='dl_' + str(fid),
                    )

            except Exception as e:
                st.error('Error during analysis: ' + str(e))
                st.exception(e)

elif not all_uploaded:
    st.info('Please upload all 5 files and configure settings in the sidebar, then click Run Analysis.')
