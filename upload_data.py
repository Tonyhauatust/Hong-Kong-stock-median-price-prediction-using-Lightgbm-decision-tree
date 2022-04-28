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
        

    def execution(self,script='''''',command=()):
        self.cur.execute(script,command)
        self.conn.commit()

    def grab_data(self,script=''''''):
        self.cur.execute(script)
        return self.cur.fetchall()
    

        

    
def main():
    
    database=postgres_db(hostname='pgm-3ns7dw6lqemk36rgpo.pg.rds.aliyuncs.com',database='postgres',username='loratech',password='loraTECH123',port_id=5432)
    
    # script='''CREATE TABLE med_close_tony_hau(ticker VARCHAR(30), month timestamp, median_price VARCHAR(30));'''
    # column_name_list=database.execution(script) # in a tuple format
    # data=pd.read_csv('med_close.csv')
    # print(data)

    # for _,row in data.iterrows():
    #     ticker=row['Ticker']
    #     month=row['Month']
    #     price=row['Median']
    #     print(ticker,month,price)
    #     script = '''INSERT INTO med_close_tony_hau (ticker,month,price) VALUES (%s,%s,%s)'''
    #     database.execution(script,(ticker,month,price))
    #     print('Successfully upload data!'+str(ticker)+str(month)+str(price))
    script = '''SELECT * FROM med_close_tony_hau'''
    data=database.grab_data(script)
    print(data)
        
if __name__ == "__main__":
    main()