# Import Dependencies
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, and_
import datetime as dt
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/20160914<br/>"
        f"/api/v1.0/20160914/20170110"
    )

@app.route("/api/v1.0/precipitation")
def precipitation_last12months():

    # Create session for link from Python to Db
    session = Session(engine)
    
    """Return precipitation data for the last 12 months from most recent date in dataset"""
    # Get most recent date in dataset
    recent_date = session.query(func.max(Measurement.date)).scalar()

    # Calculate the date one year from the recent date in data set.
    last_twelve_months = dt.datetime.strptime(recent_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Perform a query to retrieve the date and precipitation scores
    results = session.query(Measurement.date, func.sum(Measurement.prcp).label('prcp')).\
                   filter(Measurement.date >= last_twelve_months).group_by(Measurement.date).order_by(Measurement.date).all()

    # Close session
    session.close()

    # Create a dictionary from the row data and append to a list of precipitation
    precip_last12months = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["Date"] = date
        prcp_dict["Precipitation"] = round(prcp, 2)
        precip_last12months.append(prcp_dict)        

    return jsonify(precip_last12months)


@app.route("/api/v1.0/stations")
def station_list():

    # Create session for link from Python to Db
    session = Session(engine)

    """Return list of all stations - unique values"""
    results = session.query(func.distinct(Measurement.station).label('station')).all()

    # Close session
    session.close()

     # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)


@app.route("/api/v1.0/tobs")
def temp_observation():

    # Create session for link from Python to Db
    session = Session(engine)

    """Return list of temperature observations for most active station for last one year"""
    # Step1 - get station with highest observations (most active)
    results = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).\
              order_by(func.count(Measurement.station).desc()).limit(1).all()

    most_active_station = results[0].station

    # Step2 - Prepare start and end dates (end=latest date for station and start=-365 days)
    end_date = session.query(func.max(Measurement.date)).filter(Measurement.station == most_active_station).scalar()
    start_date = dt.datetime.strptime(end_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Step3 - query date and temperature for the station between start and end-dates
    qry_results = session.query(Measurement.date, Measurement.tobs).\
                   filter(Measurement.date >= start_date, Measurement.station == most_active_station).order_by(Measurement.date).all()

    # Close session
    session.close()

    station_tobs = []
    for date, tobs in qry_results:
        temp_dict = {}
        temp_dict["Date"] = date
        temp_dict["Temperature"] = round(tobs, 2)
        station_tobs.append(temp_dict)        

    return jsonify(station_tobs)


@app.route("/api/v1.0/<start_date>")
def temp_stats_with_start_date(start_date):

    # Validate start_date to be in YYYYMMDD format. Proceed with query if valid, else return error
    try:
        dt.datetime.strptime(start_date, '%Y%m%d')

        # Create session for link from Python to Db
        session = Session(engine)

        """Return listing oa min, max and Average temperature in dates GT or EQ start-date"""
        results = session.query(func.min(Measurement.tobs).label('min_temp'), func.max(Measurement.tobs).label('max_temp'),\
            func.avg(Measurement.tobs).label('avg_temp')).filter(Measurement.date >= start_date).all()

        # Close session
        session.close()

        # Return jsonify results from list comprehension
        result_list = []
        result_list = [{"Minimum temperature": result[0], "Maximum temperature": result[1], "Average temperature": result[2]} for result in results]
        return jsonify(result_list)

    except:
        return jsonify({"error": "Incorrect date format or invalid date, should be in YYYYMMDD (range from 20100101 thru 20170823"})


@app.route("/api/v1.0/<start_date>/<end_date>")
def temp_stats_with_start_end_dates(start_date, end_date):

    # Validate start_date and end_date to be in YYYYMMDD format. Proceed with query if valid, else return error
    date_error = 'N'
    try:
       st_dt = dt.datetime.strptime(start_date, '%Y%m%d')
       en_dt = dt.datetime.strptime(end_date, '%Y%m%d')            
    except:
        date_error = 'Y'

    if date_error == 'N':
        # Create session for link from Python to Db
        session = Session(engine)

        """Return listing oa min, max and Average temperature in dates GT or EQ start-date"""
        results = session.query(func.min(Measurement.tobs).label('min_temp'), func.max(Measurement.tobs).label('max_temp'),\
            func.avg(Measurement.tobs).label('avg_temp')).filter(Measurement.date >= st_dt).filter(Measurement.date <= en_dt).all()

        # Close session
        session.close()

        # Return jsonify results from list comprehension
        result_list = []
        result_list = [{"Minimum temperature": result[0], "Maximum temperature": result[1], "Average temperature": result[2]} for result in results]
        return jsonify(result_list)

    else:
        return jsonify({"error": "Enter valid start and end date in YYYYMMDD format (range from 20100101 thru 20170823)"})



if __name__ == '__main__':
    app.run(debug=True)
