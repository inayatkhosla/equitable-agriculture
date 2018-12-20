"""
Defines and creates database tables
"""

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
import helpers as h


# Connect to Postgres Database
engine = h.db_connect()


# Define Schema
Base = declarative_base()

class Prices(Base):
    __tablename__ = 'prices'
    commodity = Column(String, nullable=False, primary_key=True)
    date = Column(Date, nullable=False, primary_key=True)
    state = Column(String, nullable=False, primary_key=True)
    district = Column(String, nullable=False, primary_key=True)
    market = Column(String, nullable=False, primary_key=True)
    grade = Column(String, nullable=False, primary_key=True)
    variety = Column(String, nullable=False, primary_key=True)
    max_price = Column(Float)
    min_price = Column(Float)
    modal_price = Column(Float)
    
    

class Arrivals(Base):
    __tablename__='arrivals'
    commodity = Column(String, nullable=False, primary_key=True)
    date = Column(Date, nullable=False, primary_key=True)
    state = Column(String, nullable=False, primary_key=True)
    district = Column(String, nullable=False, primary_key=True)
    market = Column(String, nullable=False, primary_key=True)
    quantity = Column(Float, nullable=False)
    
    
class LocationMap(Base):
    __tablename__ = 'location_map'
    state = Column(String, nullable=False, primary_key=True)
    district = Column(String, nullable=False, primary_key=True)
    market = Column(String, nullable=False, primary_key=True)
    

# Create tables 
Base.metadata.create_all(engine)     
#Prices.__table__.create(bind=engine, checkfirst=True)
#Arrivals.__table__.create(bind=engine, checkfirst=True)
#LocationMap.__table__.create(bind=engine, checkfirst=True)