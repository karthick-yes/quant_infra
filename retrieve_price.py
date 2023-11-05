import MySQLdb as mdb
import yfinance as yf
import datetime


def connect_to_db():
    db_host = 'localhost'
    db_user = 'me'
    db_pass = 'Tothemoon12utYu'
    db_name = 'price_data'
    con = mdb.connect(host =db_host, user = db_user, password = db_pass, db = db_name)
    return con


def obtain_list_db_tickers(con):

    with con:
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]


def get_historical_data_yfinance(ticker : str , start_date = (2000,1,1), end_date = datetime.date.today()):

    prices = []

    try:
        yf_data = yf.download(ticker,start_date, end_date, interval="1d")
        prices = list(yf_data.to_records())
    except Exception as e:
        print(f"Could not download the price data due to the following error:{e}")

    return prices



def insert_daily_data(data_vendor_id, symbol_id, daily_price_data, now = datetime.datetime.utcnow()):

    daily_price_data = [ (data_vendor_id, symbol_id, d[0], now, now, d[1], d[2], d[3], d[4], d[5], d[6]) for d in daily_price_data]

    c_str = """data_vendor_id, symbol_id, price_date, created_date,last_updated_date, open_price, high_price, low_price,close_price, volume, adj_close_price"""

    insert_string = ("%s * 11")[:-2]

    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (c_str, insert_string)

    con = connect_to_db()
    with con:
        cur = con.cursor()
        cur.executemany(final_str, daily_price_data)


if __name__ == "__main__":
    con = connect_to_db()
    tickers = obtain_list_db_tickers(con)
    
    for i, ticker in enumerate(tickers):
        print( f" Adding data for {ticker[1]}: {i+1} out of the {len(tickers)} available.")

        yf_data = get_historical_data_yfinance(ticker[1])
        insert_daily_data('1', ticker[0], yf_data)
    print("Successfully worked")
    



