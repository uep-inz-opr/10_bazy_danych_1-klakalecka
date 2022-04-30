import csv
import requests
import sqlite3

url = 'https://raw.githubusercontent.com/khashishin/repozytorium_z_plikiem_polaczenia/main/polaczenia_duze.csv'
r = requests.get(url, allow_redirects=True)
open('polaczenia_duze.csv', 'wb').write(r.content)

sqlite_con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

cur = sqlite_con.cursor()

cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''')

sqlite_con.commit()

with open('polaczenia_duze.csv','r') as fin:
    reader = csv.reader(fin, delimiter = ";")
    next(reader, None)
    rows = [x for x in reader]
    cur.executemany("INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", rows)
    sqlite_con.commit()

class ReportGenerator:
  def __init__(self, connection):
    self.connection = connection
    self.report_text = None

  def generate_report(self):
      cursor = self.connection.cursor()
      sql_query = "Select sum(duration) from polaczenia"
      cursor.execute(sql_query)
      result = cursor.fetchone()[0]
      self.report_text = f"{result}"

  def get_suma_czasow_trwania(self):
    return self.report_text


if __name__ == '__main__':
    nazwa_pliku = input()
    mp = ReportGenerator(input)
    rg = ReportGenerator(sqlite_con)
    rg.generate_report()
    print(rg.get_suma_czasow_trwania())
