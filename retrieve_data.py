from retrieve_price import connect_to_db
import pandas as pd
import MySQLdb as mdb




def get_price_data(ticker :str):
    con = connect_to_db()


    sql = f"""SELECT dp.price_date, dp.adj_close_price
    FROM symbol AS sym
    INNER JOIN daily_price AS dp
    ON dp.symbol_id = sym.id
    WHERE sym.ticker = '{ticker}'
    ORDER BY dp.price_date ASC;"""


    goog = pd.read_sql_query(sql, con=con, index_col="price_date")

    return goog.tail()

get_price_data("HDFCBANK")