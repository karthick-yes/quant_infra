
import csv
import datetime
import MySQLdb as mdb

now = datetime.datetime.utcnow()

#get symbols from the csv file
def get_symbols():
    symbols = []
    try:
        with open('ind.csv', newline='') as csvfile:
            c = csv.reader(csvfile)
            next(csvfile)
            for row in c:
                symbols.append((row[2],#symbol
                       'stock',
                       row[0], #name
                       row[1],
                       'INR',now, now)) #sector
    except FileNotFoundError:
        print("File not found:")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return symbols




def get_list_of_db_tickers(symbols):
    """
    Function which would upload the values inside the symbols list to the DB
    """

    #db_credentials
    db_host = 'localhost'
    db_user = 'me'
    db_pass = 'Tothemoon12utYu'
    db_name = 'price_data'
    con = mdb.connect(host =db_host, user = db_user, password = db_pass, db = db_name)

    c_str = """ticker, instrument, name, sector, currency, created_date, last_updated_date"""
    
    insert_str = ("%s, " * 7)[:-2]
    final_str = "INSERT INTO SYMBOL (%s) VALUES (%s)" %  (c_str, insert_str)

    with con:
        cur  = con.cursor()
        cur.executemany(final_str, symbols)


if __name__ == "__main__":
    symbols = get_symbols()
    get_list_of_db_tickers(symbols)
    print("%s symbols were loaded successfully." % len(symbols))

