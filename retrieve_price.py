import MySQLdb as mdb
import yfinance as yf
import datetime
from sqlalchemy import create_engine, text

#connect to the sql database
def connect_to_db():
    db_host = 'localhost'
    db_user = 'me'
    db_pass = 'Tothemoon12utYu'
    db_name = 'price_data'
    engine = create_engine(f"mysql+mysqldb://{db_user}:{db_pass}@{db_host}/{db_name}")
    return engine

#obtain the list of tickers
def obtain_list_db_tickers(engine):

    with engine.connect() as con:
        data = con.execute(text("SELECT id, ticker FROM symbol"))
        return [(d[0], d[1]) for d in data]

#download the historical data for each tickers
def get_historical_data_yfinance(ticker:str , start_date =  "2022-01-01", end_date = "2023-01-01"):
    ticker = ticker + ".NS"
   

    prices = []

    try:
        yf_data = yf.Ticker(ticker)
        prices = yf_data.history(start=start_date, end=end_date)
        prices = list(prices.to_records())
    except Exception as e:
        print(f"Could not download the price data due to the following error:{e}")

    return prices

#insert daily data into
def insert_daily_data(data_vendor_id, symbol_id, daily_price_data, now = datetime.datetime.utcnow()):

    daily_price_data = [ {"data_vendor_id": data_vendor_id, 
                          "symbol_id": symbol_id, 
                          "price_date": d[0], 
                          "created_date": now, 
                          "last_updated_date": now, 
                          "open_price": d[1], 
                          "high_price": d[2], 
                          "low_price": d[3], 
                          "close_price": d[4], 
                          "volume": d[5], 
                          "adj_close_price": d[6]} for d in daily_price_data]

    final_str = "INSERT INTO daily_price (data_vendor_id, symbol_id, price_date, created_date,last_updated_date, open_price, high_price, low_price,close_price, volume, adj_close_price) VALUES (:data_vendor_id, :symbol_id, :price_date, :created_date,:last_updated_date, :open_price, :high_price, :low_price, :close_price, :volume, :adj_close_price)"

    engine = connect_to_db()
    with engine.connect() as con:
        for data in daily_price_data:
            con.execute(text(final_str), data)
        con.commit()


if __name__ == "__main__":
    engine = connect_to_db()
    tickers = obtain_list_db_tickers(engine)
    
    for i, ticker in enumerate(tickers):
        try:
            print(f" Adding data for {ticker[1]}: {i+1} out of the {len(tickers)} available.")
            yf_data = get_historical_data_yfinance(ticker[1])
            insert_daily_data('1', ticker[0], yf_data)
        except Exception as e:
            print(f"Failed to fetch data for {ticker[1]}. Error: {e}")

    



