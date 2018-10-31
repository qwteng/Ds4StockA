import logging
import sys
import datetime
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts


def logger_init(logfile):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S',
        filename=logfile,
        filemode='w')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


logger_init('app.log')


# table names where the stock data is stored
class Tables:
    STOCK_BASIC = 'stock_basic'  # stock basic info
    TOP10_HOLDER = 'top10_holder'  # stock top10 holder info
    TOP10_FLOATHOLDER = 'top10_floatholder'  # stock top10 floatholder info

    def __init__(self):
        pass


# get tushare api
def get_tsapi():
    ts.set_token("b84661efadd955c6eb9cea5a6a1b7ffe3e5a4831c2de7b7c2e8d2c01")
    return ts.pro_api()


# get stock basic info to dataframe
def get_stock_basic(ts_api):
    df_stock_basic = ts_api.query(
        'stock_basic',
        excjamge_id='',
        list_status='L',
        fields='ts_code, symbol, name, area, industry, list_date, market,is_hs'
    )
    return df_stock_basic


# get top 10 holders info to dataframe
def get_top10_holders(ts_api, ts_code, start_date, end_date):
    df_top10_holder = ts_api.query(
        'top10_holders',
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date)
    return df_top10_holder


def get_top10_floatholders(ts_api, ts_code, start_date, end_date):
    df_top10_floatholder = ts_api.query(
        'top10_floatholders',
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date)
    return df_top10_floatholder


def get_sqlite_conn(db_name):
    engine = create_engine('sqlite:///./' + db_name, echo=False)
    return engine


def collect_stock_basic(ts_api, db_name, table_name):
    logging.info("begin to get stock basic info ...")
    df_stock_basic = get_stock_basic(ts_api)
    con = get_sqlite_conn(db_name)
    df_stock_basic.to_sql(
        table_name, con=con, if_exists='replace', index=False)
    logging.info("get stock basic info over")


# get table data to dataframe
def get_table_data(db_name, table_name):
    con = get_sqlite_conn(db_name)
    df = None
    try:
        df = pd.read_sql_table(table_name, con)
    except Exception:
        pass
    return df


def collect_top10_holder(ts_api, db_name, table_stock_basic,
                         table_top10_holder, start_date, end_date):
    logging.info("begin to get stock top10 holder info ...")
    df_stock_basic_db = get_table_data(db_name, table_stock_basic)
    df_top10_holder_db = get_table_data(db_name, table_top10_holder)
    ts_codes = None
    if df_stock_basic_db is None:  # no stock to collect
        return

    if df_top10_holder_db is None:
        ts_codes = set(df_stock_basic_db.ts_code)
    else:
        ts_codes_stock_basic = set(df_stock_basic_db.ts_code)
        ts_codes_top10_holder = set(df_top10_holder_db.ts_code)
        ts_codes = ts_codes_stock_basic - ts_codes_top10_holder
    if (ts_codes is None) or (len(ts_codes) < 1):
        return

    logging.info("There are " + str(len(ts_codes)) +
                 " stocks to get top10 holder")
    con = get_sqlite_conn(db_name)
    for ts_code in ts_codes:
        df_top10_holder = get_top10_holders(ts_api, ts_code, start_date,
                                            end_date)
        df_top10_holder.to_sql(
            table_top10_holder, con=con, if_exists='append', index=False)
        logging.info(ts_code + " get top10 holder success")
    logging.info("get stock top10 holder info over")


def collect_top10_floatholder(ts_api, db_name, table_stock_basic,
                              table_top10_floatholder, start_date, end_date):
    logging.info("begin to get stock top10 floatholder info ...")
    df_stock_basic_db = get_table_data(db_name, table_stock_basic)
    df_top10_floatholder_db = get_table_data(db_name, table_top10_floatholder)
    ts_codes = None
    if df_stock_basic_db is None:  # no stock to collect
        return

    if df_top10_floatholder_db is None:
        ts_codes = set(df_stock_basic_db.ts_code)
    else:
        ts_codes_stock_basic = set(df_stock_basic_db.ts_code)
        ts_codes_top10_floatholder = set(df_top10_floatholder_db.ts_code)
        ts_codes = ts_codes_stock_basic - ts_codes_top10_floatholder
    if (ts_codes is None) or (len(ts_codes) < 1):
        return

    logging.info("There are " + str(len(ts_codes)) +
                 " stocks to get top10 floatholder")
    con = get_sqlite_conn(db_name)
    for ts_code in ts_codes:
        df_top10_floatholder = get_top10_floatholders(ts_api, ts_code,
                                                      start_date, end_date)
        df_top10_floatholder.to_sql(
            table_top10_floatholder, con=con, if_exists='append', index=False)
        logging.info(ts_code + " get top10 floatholder success")
    logging.info("get stock top10 floatholder info over")


# collect stock basic info, top10 holder info, top10 floatholder info to db
def collect_stock_info(ts_api, db_name, start_date, end_date):
    collect_stock_basic(ts_api, db_name, Tables.STOCK_BASIC)
    collect_top10_holder(ts_api, db_name, Tables.STOCK_BASIC,
                         Tables.TOP10_HOLDER, start_date, end_date)
    collect_top10_floatholder(ts_api, db_name, Tables.STOCK_BASIC,
                              Tables.TOP10_FLOATHOLDER, start_date, end_date)


def get_quater_range(quater):
    quater_map = {
        'Q1': ('0101', '0330'),
        'Q2': ('0401', '0630'),
        'Q3': ('0701', '0930'),
        'Q4': ('1001', '1231')
    }
    return quater_map.get(quater)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        logging.info('para error')
        exit(-1)
    year = sys.argv[1]
    quater = sys.argv[2]
    day_range = get_quater_range(quater)
    if day_range is None:
        logging.info('wrong quater para')
        exit(-1)

    time_str = datetime.datetime.now().strftime('%Y%m%d')
    db_name = 'stock_' + time_str + '.sqlite3'
    ts_api = get_tsapi()
    start_date = year + day_range[0]
    end_date = year + day_range[1]

    logging.info('start collect stock info in day range [' + start_date + ',' +
                 end_date + '] ...')
    collect_stock_info(ts_api, db_name, start_date, end_date)
    logging.info('stock info collect over.')