# adapter/snowflake.py
import snowflake.connector
from .base import BaseDataWareouseAdapter

class SnowflakeAdapter(BaseDataWareouseAdapter):
    def __init__(self, config):
        self.config = config
        self.conn = snowflake.connector.connect(
            user=self.config['user'],
            password=self.config['password'],
            account=self.config['account'],
            warehouse=self.config['warehouse'],
            database=self.config['database'],
            schema=self.config['schema']
        )
        self.cur = self.conn.cursor()

    def execute(self, sql):
        try:
            self.cur.execute(sql)
            result = self.cur.fetchone()
            return result, self.cur.description
        except Exception as e:
            print(f"An error occurred: {e}")
            return None, []

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
