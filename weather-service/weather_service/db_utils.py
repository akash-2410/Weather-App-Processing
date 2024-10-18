"""
This module provides functions for interacting with the weather data stored in the database.

Functions:
1. `cleanup_old_realtime_weather()`: Removes records older than 24 hours from the `realtime_weather` table.
2. `insert_realtime_weather(dt, main_condition, temp, feels_like, pressure, humidity, rain, clouds, city)`: Inserts a new real-time weather record into the `realtime_weather` table.
3. `insert_daily_weather(date, avg_temp, max_temp, min_temp, dom_condition)`: Inserts a new daily weather record into the `daily_weather` table.
4. `insert_alert_event(dt, city, trigger, reason)`: Inserts a new alert event into the `alert_events` table.
5. `aggregate_daily_weather()`: Aggregates real-time weather data to daily summaries and inserts them into the `daily_weather` table.
6. `get_alerts()`: Retrieves all alert events from the `alert_events` table.
7. `get_historical_data()`: Retrieves all historical weather data from the `daily_weather` table and returns it as a JSON string.
8. `get_realtime_data()`: Retrieves all real-time weather data from the `realtime_weather` table and returns it as a JSON string.
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
import datetime
import json

from weather_service.db_models import engine, RealtimeWeather, DailyWeather, AlertEvent


Session = sessionmaker(bind=engine)
session = Session()

def cleanup_old_realtime_weather():
    cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=24)
    session.query(RealtimeWeather).filter(RealtimeWeather.dt < cutoff_time).delete(synchronize_session=False)
    session.commit()

async def insert_realtime_weather(dt, main_condition, temp, feels_like, pressure, humidity, rain, clouds, city):
 
    cleanup_old_realtime_weather()
    
    new_data = RealtimeWeather(
        dt=dt,
        main_condition=main_condition,
        temp=temp,
        feels_like=feels_like,
        pressure=pressure,
        humidity=humidity,
        rain=rain,
        clouds=clouds,
        city=city
    )
    session.add(new_data)
    session.commit()

def insert_daily_weather(date, avg_temp, max_temp, min_temp, dom_condition):

    new_data = DailyWeather(
        date=date,
        avg_temp=avg_temp,
        max_temp=max_temp,
        min_temp=min_temp,
        dom_condition=dom_condition
    )
    session.add(new_data)
    session.commit()

def insert_alert_event(dt, city, trigger, reason):

    new_event = AlertEvent(
        dt=dt,
        city=city,
        reason=reason,
        trigger=trigger,
    )
    session.add(new_event)
    session.commit()

def aggregate_daily_weather():
    
    today = datetime.date.today()
    start_time = datetime.datetime.combine(today, datetime.time.min)
    end_time = datetime.datetime.combine(today, datetime.time.max)
    
    result = session.query(
        RealtimeWeather.city,
        func.date(RealtimeWeather.dt).label('date'),
        func.avg(RealtimeWeather.temp).label('avg_temp'),
        func.max(RealtimeWeather.temp).label('max_temp'),
        func.min(RealtimeWeather.temp).label('min_temp'),
        func.max(RealtimeWeather.main_condition).label('dom_condition')
    ).filter(
        RealtimeWeather.dt >= start_time,
        RealtimeWeather.dt <= end_time
    ).group_by(
        RealtimeWeather.city,
        func.date(RealtimeWeather.dt)
    ).all()

    for row in result:
        daily_weather = DailyWeather(
            date=row.date,
            city=row.city,
            avg_temp=row.avg_temp,
            max_temp=row.max_temp,
            min_temp=row.min_temp,
            dom_condition=row.dom_condition
        )
        session.merge(daily_weather)
    
    session.commit()

def get_alerts():
    alerts = session.query(AlertEvent).all()
    return alerts

def get_historical_data():
   
    historical_data = session.query(DailyWeather).all()
    return json.dumps([data.__dict__ for data in historical_data])

def get_realtime_data():

    realtime_data = session.query(RealtimeWeather).all()
    data_list = [row.__dict__ for row in realtime_data]
    
    for item in data_list:
        item.pop('_sa_instance_state', None)

    json_data = json.dumps(data_list, default=str)
    
    return json_data
