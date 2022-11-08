from abc import ABC, abstractmethod
import datetime

import psycopg2
from psycopg2 import sql
 
class BatchExtractor(ABC):

    @abstractmethod
    def get_log_data(self, location):
        # retrieve newer data than self.timestamp
        # return data
        pass
    
    @abstractmethod
    def get_registry_data(self, location):
        # retrieve newer data based on some way to recognize newer data for registry type of data
        # return data 
        pass


class BatchUploader(ABC):
    @abstractmethod
    def save_data(self,data,location):
        # store data, passed as parameter, in the provided location
        pass

class PostgreBatchUploader(BatchUploader):
    def __init__(self, filename):
      # read connection parameters from file
      with open (filename, 'r') as myfile:
        # read file line by life and removing whitespace
        cred = [l.strip() for l in myfile]
        # connect to db using credentials
        self.conn = psycopg2.connect(
          host = cred[0],
          port = cred[1],
          user = cred[3],
          password = cred[4],
          dbname = cred[2]
        )
        self.conn.autocommit = True
        
    def save_data(self,data,location="transaction_backup",): 
        # initialize cursor
        cur = self.conn.cursor()
        # get number of rows of the table
        number_of_columns = len(data[0])
        # execute query to save data
        for entry in data:
          query_string = sql.SQL("Insert into {} values ({})").format(
            sql.Identifier(location),
            sql.SQL(', ').join(sql.Placeholder()*number_of_columns)
          ).as_string(cur)
          cur.execute(cur.mogrify(query_string,entry))
        # close cursor
        cur.close()


 
class PostgreBatchExtractor(BatchExtractor):
    def __init__(self, config):
      # read connection parameters from file
      with open (config, 'r') as myfile:
        # read file line by life and removing whitespace
        cred = [l.strip() for l in myfile]
        # connect to db using credentials
        self.conn = psycopg2.connect(
          host = cred[0],
          port = cred[1],
          user = cred[3],
          password = cred[4],
          dbname = cred[2]
        )
        # initialize timestamp to min value to use to get only fresh 
        self.timestamp = datetime.datetime.min

    def get_log_data(self, location="transaction"):
        # initialize cursor
        cur = self.conn.cursor()
        # execute query to get fresh data
        cur.execute(sql.SQL("select * from {} where datetime > %s").format(sql.Identifier(location)),(self.timestamp,))
        #cur.execute("select * from %s where datetime > %s",(location, self.timestamp,))
        # read data from cursor
        data = cur.fetchall()
        # close cursor
        cur.close()
        # update timestamp to current time
        self.timestamp = datetime.datetime.now()
        # return latest data
        return data

    def get_registry_data(self, location="account"):
        # for the sake of code simplicity, since this is just an example, we just take all registry data 
        # instead of applying a technique to only get new data 
        # initialize cursor
        cur = self.conn.cursor()
        # execute query to get fresh data
        cur.execute(sql.SQL("select * from {}").format(sql.Identifier(location)))
        #cur.execute("select * from %s",(location,))
        # read data from cursor
        data = cur.fetchall()
        # close cursor
        cur.close()
        # return latest data
        return data



extractor = PostgreBatchExtractor('postgre_credentials.txt')
data = extractor.get_registry_data("account")
print(data)
uploader = PostgreBatchUploader('postgre_credentials_backup.txt')
uploader.save_data(data,location="account_backup")


extractor = PostgreBatchExtractor('postgre_credentials.txt')
data = extractor.get_log_data("transaction")
print(data)
uploader = PostgreBatchUploader('postgre_credentials_backup.txt')
uploader.save_data(data,location="transaction_backup")