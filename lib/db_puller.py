import pandas as pd
import lib.helpers as h


class DBPuller(object):
    """
    Pulls price, arrival, and location data from postgres RDS instance
    
    Args:
        commodity (str): Commodity to pull
        start (str): Start date of pull
        end (str): End date of pull
    """
    def __init__(self, commodity, start, end=None):
        self.commodity = commodity
        self.start = start
        self.end = end
        if not self.end:
            self.end = str(pd.to_datetime('today').date())
        
    
    def get_data(self):
        engine = h.db_connect()
        conn = engine.connect()
        query = "select * from {} where commodity = '{}' and date BETWEEN '{}' and '{}'"
        self.prices = pd.read_sql(query.format('prices', self.commodity, self.start, self.end), con=conn)
        self.arrivals = pd.read_sql(query.format('arrivals', self.commodity, self.start, self.end), con=conn)
        self.lm = pd.read_sql("select * from location_map", con=conn)
        conn.close()