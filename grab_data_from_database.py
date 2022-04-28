import psycopg2
import psycopg2.extras
import pandas as pd
import datetime
import statistics


class postgres_db:
    def __init__(self,hostname='',database='',username='',password='',port_id=0):
        self.hostname=hostname
        self.database=database
        self.username=username
        self.password=password
        self.port_id=port_id
        self.conn=psycopg2.connect(
            host=self.hostname,
            dbname=self.database,
            user=self.username,
            password=self.password,
            port=self.port_id
        )
        self.cur=self.conn.cursor()
        

    def execution(self,script=''''''):
        self.cur.execute(script)
        self.conn.commit()

    def grab_data(self,script=''''''):
        self.cur.execute(script)
        return self.cur.fetchall()

    
def main():
    
    database=postgres_db(hostname='pgm-3ns7dw6lqemk36rgpo.pg.rds.aliyuncs.com',database='postgres',username='loratech',password='loraTECH123',port_id=5432)
    
    script='''SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'data_price'; '''
    column_name_list=database.grab_data(script) # in a tuple format
    print(column_name_list)
    
    script='''SELECT DISTINCT ticker FROM data_price; '''
    ticker_list=database.grab_data(script) 
    ticker_list=[ticker[0] for ticker in ticker_list]
    
    script='''SELECT DISTINCT trading_day FROM data_price'''
    month_list=database.grab_data(script)
    month_list=[date[0].strftime('%Y-%m') for date in month_list] # for each tuple we convert them into yyyymm
    month_list=list(set(month_list))
    month_list=sorted(month_list, key=lambda x: datetime.datetime.strptime(x, '%Y-%m')) # a month list with sorted values
    print(month_list)
    medians=[]
    tickers=[]
    months=[]
    for ticker in ticker_list:
        for month in month_list:
            month=datetime.datetime.strptime(month, "%Y-%m")
            m=str(month.month)
            year=str(month.year)
            script = 'SELECT close ' + 'FROM data_price WHERE '+"ticker = '"+ ticker+"' AND date_part('month',trading_day) = " +m + " AND date_part('year',trading_day) = "+year
            data=database.grab_data(script)
            close = [close[0] for close in data]
            if len(close) ==0:
                medians.append(0)
            else:
                medians.append(statistics.median(close))
            tickers.append(ticker)
            months.append(year+'-'+month)
            if len(close) ==0:
                print(ticker+' in month '+m+' and year '+year+ ' has median '+ str(0))
            else:
                print(ticker+' in month '+m+' and year '+year+ ' has median '+ str(statistics.median(close)))
    
    med_close=pd.DataFrame({
        'Ticker':tickers,
        'Month':months,   
        'Median':medians
        })
    
    med_close.to_csv('med_close.csv')
    
if __name__ == "__main__":
    main()