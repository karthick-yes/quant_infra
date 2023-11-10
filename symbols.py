
import csv
import datetime
import MySQLdb as mdb
from sqlalchemy import create_engine, text

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
    db_host = 'localhost'
    db_user = 'me'
    db_pass = 'Tothemoon12utYu'
    db_name = 'price_data'
    engine = create_engine(f"mysql+mysqldb://{db_user}:{db_pass}@{db_host}/{db_name}")
    
   
    final_str = "INSERT INTO SYMBOL (ticker, instrument, name, sector, currency, created_date, last_updated date) VALUES (:ticker, :instrument, :name, :sector, :currency, :created_date, :last_updated_date)" 

    with engine.connect() as conn:
        conn.execute(text(final_str), symbols)
        conn.commit()


if __name__ == "__main__":
    symbols = get_symbols()
    get_list_of_db_tickers(symbols)
    print(f"{len(symbols)} symbols were inserted to the database successfully.")

