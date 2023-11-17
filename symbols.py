
import csv
import datetime
import MySQLdb as mdb
from sqlalchemy import create_engine, text, delete, MetaData, Table

now = datetime.datetime.utcnow()

def connect_to_db():
    db_host = 'localhost'
    db_user = 'me'
    db_pass = 'Tothemoon12utYu'
    db_name = 'price_data'
    engine = create_engine(f"mysql+mysqldb://{db_user}:{db_pass}@{db_host}/{db_name}")
    return engine


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
    db_host = 'localhost'
    db_user = 'me'
    db_pass = 'Tothemoon12utYu'
    db_name = 'price_data'
    engine = create_engine(f"mysql+mysqldb://{db_user}:{db_pass}@{db_host}/{db_name}")

    final_str = "INSERT INTO SYMBOL (ticker, instrument, name, sector, currency, created_date, last_updated_date) VALUES (:ticker, :instrument, :name, :sector, :currency, :created_date, :last_updated_date)"

    with engine.connect() as conn:
        for symbol in symbols:
            conn.execute(text(final_str), 
                         {"ticker": symbol[0], 
                          "instrument": symbol[1], 
                          "name": symbol[2], 
                          "sector": symbol[3], 
                          "currency": symbol[4], 
                          "created_date": symbol[5], 
                          "last_updated_date": symbol[6]})
        conn.commit()

def clear_tables(engine, table_name:str):
    metadata_obj = MetaData()
    table = Table(table_name, metadata_obj,autoload_with=engine)
    with engine.connect() as conn:
        delete_stmnt = delete(table)
        conn.execute(delete_stmnt)
        conn.commit()


if __name__ == "__main__":
    symbols = get_symbols()
    engine = connect_to_db()
    clear_tables(engine=engine,table_name='symbol')
    get_list_of_db_tickers(symbols)
    print(f"{len(symbols)} symbols were inserted to the database successfully.")

