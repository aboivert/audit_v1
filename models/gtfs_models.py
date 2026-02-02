"""
Modèles SQLAlchemy pour les tables GTFS temporaires
"""
from sqlalchemy import Column, Integer, String, Float, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()

class GTFSAgency(Base):
    __tablename__ = 'gtfs_agency'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(50), nullable=False, index=True)  # UUID comme string
    agency_id = Column(String(255))
    agency_name = Column(String(255))
    agency_url = Column(String(500))
    agency_timezone = Column(String(100))
    agency_lang = Column(String(10))
    agency_phone = Column(String(50))
    agency_fare_url = Column(String(500))
    agency_email = Column(String(255))

class GTFSRoutes(Base):
    __tablename__ = 'gtfs_routes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    route_id = Column(String(255))
    agency_id = Column(String(255))
    route_short_name = Column(String(255))
    route_long_name = Column(String(500))
    route_desc = Column(Text)
    route_type = Column(Integer)
    route_url = Column(String(500))
    route_color = Column(String(10))
    route_text_color = Column(String(10))
    route_sort_order = Column(Integer)

class GTFSTrips(Base):
    __tablename__ = 'gtfs_trips'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    route_id = Column(String(255))
    service_id = Column(String(255))
    trip_id = Column(String(255))
    trip_headsign = Column(String(255))
    trip_short_name = Column(String(255))
    direction_id = Column(Integer)
    block_id = Column(String(255))
    shape_id = Column(String(255))
    wheelchair_accessible = Column(Integer)
    bikes_allowed = Column(Integer)

class GTFSStops(Base):
    __tablename__ = 'gtfs_stops'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    stop_id = Column(String(255))
    stop_code = Column(String(255))
    stop_name = Column(String(255))
    stop_desc = Column(Text)
    stop_lat = Column(Float)
    stop_lon = Column(Float)
    zone_id = Column(String(255))
    stop_url = Column(String(500))
    location_type = Column(Integer)
    parent_station = Column(String(255))
    stop_timezone = Column(String(100))
    wheelchair_boarding = Column(Integer)
    level_id = Column(String(255))
    platform_code = Column(String(255))

class GTFSStopTimes(Base):
    __tablename__ = 'gtfs_stop_times'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    trip_id = Column(String(255))
    arrival_time = Column(String(20))
    departure_time = Column(String(20))
    stop_id = Column(String(255))
    stop_sequence = Column(Integer)
    stop_headsign = Column(String(255))
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    continuous_pickup = Column(Integer)
    continuous_drop_off = Column(Integer)
    shape_dist_traveled = Column(Float)
    timepoint = Column(Integer)

class GTFSCalendar(Base):
    __tablename__ = 'gtfs_calendar'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    service_id = Column(String(255))
    monday = Column(Integer)
    tuesday = Column(Integer)
    wednesday = Column(Integer)
    thursday = Column(Integer)
    friday = Column(Integer)
    saturday = Column(Integer)
    sunday = Column(Integer)
    start_date = Column(String(10))
    end_date = Column(String(10))

class GTFSCalendarDates(Base):
    __tablename__ = 'gtfs_calendar_dates'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    service_id = Column(String(255))
    date = Column(String(10))
    exception_type = Column(Integer)

class GTFSFareAttributes(Base):
    __tablename__ = 'gtfs_fare_attributes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    fare_id = Column(String(255))
    price = Column(Float)
    currency_type = Column(String(10))
    payment_method = Column(Integer)
    transfers = Column(Integer)
    agency_id = Column(String(255))
    transfer_duration = Column(Integer)

class GTFSFareRules(Base):
    __tablename__ = 'gtfs_fare_rules'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    fare_id = Column(String(255))
    route_id = Column(String(255))
    origin_id = Column(String(255))
    destination_id = Column(String(255))
    contains_id = Column(String(255))

class GTFSShapes(Base):
    __tablename__ = 'gtfs_shapes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    shape_id = Column(String(255))
    shape_pt_lat = Column(Float)
    shape_pt_lon = Column(Float)
    shape_pt_sequence = Column(Integer)
    shape_dist_traveled = Column(Float)

class GTFSFrequencies(Base):
    __tablename__ = 'gtfs_frequencies'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    trip_id = Column(String(255))
    start_time = Column(String(20))
    end_time = Column(String(20))
    headway_secs = Column(Integer)
    exact_times = Column(Integer)

class GTFSTransfers(Base):
    __tablename__ = 'gtfs_transfers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    from_stop_id = Column(String(255))
    to_stop_id = Column(String(255))
    transfer_type = Column(Integer)
    min_transfer_time = Column(Integer)

class GTFSFeedInfo(Base):
    __tablename__ = 'gtfs_feed_info'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, nullable=False, index=True)
    feed_publisher_name = Column(String(255))
    feed_publisher_url = Column(String(500))
    feed_lang = Column(String(10))
    default_lang = Column(String(10))
    feed_start_date = Column(String(10))
    feed_end_date = Column(String(10))
    feed_version = Column(String(255))
    feed_contact_email = Column(String(255))
    feed_contact_url = Column(String(500))

def create_gtfs_tables(engine):
    """Créer toutes les tables GTFS"""
    Base.metadata.create_all(engine)
    print("Tables GTFS créées avec succès!")

if __name__ == "__main__":
    import sys
    import os
    
    # Ajouter le répertoire parent au path pour importer config
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from config import Config
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    create_gtfs_tables(engine)