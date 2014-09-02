package com.unitvectory.shak.weather.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.unitvectory.shak.weather.AppConfig;
import com.unitvectory.shak.weather.db.model.LocationInfo;

/**
 * The weather database.
 * 
 * @author Jared Hatfield
 *
 */
public class WeatherDatabase {

	/**
	 * the log
	 */
	private static Logger log = Logger.getLogger(WeatherDatabase.class);

	/**
	 * the data source
	 */
	private BasicDataSource dataSource;

	/**
	 * Creates a new instance of the WeatherDataabse class.
	 * 
	 * @param config
	 *            the config
	 */
	public WeatherDatabase(AppConfig config) {
		// Connect to the database
		this.dataSource = new BasicDataSource();
		this.dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		this.dataSource.setValidationQuery("SELECT 1");
		this.dataSource.setUsername(config.getDbUser());
		this.dataSource.setPassword(config.getDbPassword());
		this.dataSource.setUrl(config.getDbUrl());
		this.dataSource.setInitialSize(1);
	}

	/**
	 * the insert query
	 */
	private static final String InsertWeatherQuery = "INSERT INTO home_weather "
			+ "(location, time, summary, icon, sunriseTime, sunsetTime, "
			+ "precipIntensity, precipIntensityMax, precipProbability, "
			+ "precipType, temperatureMin, temperatureMinTime, temperatureMax, "
			+ "temperatureMaxTime, apparentTemperatureMin, apparentTemperatureMinTime, "
			+ "apparentTemperatureMax, apparentTemperatureMaxTime, dewPoint, "
			+ "humidity, windSpeed, windBearing, visibility, cloudCover, "
			+ "pressure, ozone, added, updated) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) "
			+ "ON DUPLICATE KEY UPDATE "
			+ "summary = VALUES(summary), icon = VALUES(icon), "
			+ "sunriseTime = VALUES(sunriseTime), "
			+ "sunsetTime = VALUES(sunsetTime), "
			+ "precipIntensity = VALUES(precipIntensity), "
			+ "precipIntensityMax = VALUES(precipIntensityMax), "
			+ "precipProbability = VALUES(precipProbability), "
			+ "precipType = VALUES(precipType), "
			+ "temperatureMin = VALUES(temperatureMin), "
			+ "temperatureMinTime = VALUES(temperatureMinTime), "
			+ "temperatureMax = VALUES(temperatureMax), "
			+ "temperatureMaxTime = VALUES(temperatureMaxTime), "
			+ "apparentTemperatureMin = VALUES(apparentTemperatureMin), "
			+ "apparentTemperatureMinTime = VALUES(apparentTemperatureMinTime), "
			+ "apparentTemperatureMax = VALUES(apparentTemperatureMax), "
			+ "apparentTemperatureMaxTime = VALUES(apparentTemperatureMaxTime), "
			+ "dewPoint = VALUES(dewPoint), humidity = VALUES(humidity), "
			+ "windSpeed = VALUES(windSpeed), windBearing = VALUES(windBearing), "
			+ "visibility = VALUES(visibility), cloudCover = VALUES(cloudCover), "
			+ "pressure = VALUES(pressure), ozone = VALUES(ozone), "
			+ "updated = NOW() ";

	/**
	 * Inserts a weather event
	 * 
	 * @param location
	 *            the location
	 * @param day
	 *            the day
	 * @return true if successful; otherwise false
	 */
	public boolean insert(int location, JSONObject day) {
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = this.dataSource.getConnection();
			stmt = con.prepareStatement(InsertWeatherQuery);
			int field = 1;
			stmt.setInt(field++, location);
			this.setTimestamp(stmt, day, field++, "time");
			this.setString(stmt, day, field++, "summary");
			this.setString(stmt, day, field++, "icon");
			this.setTimestamp(stmt, day, field++, "sunriseTime");
			this.setTimestamp(stmt, day, field++, "sunsetTime");
			this.setDouble(stmt, day, field++, "precipIntensity");
			this.setDouble(stmt, day, field++, "precipIntensityMax");
			this.setDouble(stmt, day, field++, "precipProbability");
			this.setString(stmt, day, field++, "precipType");
			this.setDouble(stmt, day, field++, "temperatureMin");
			this.setTimestamp(stmt, day, field++, "temperatureMinTime");
			this.setDouble(stmt, day, field++, "temperatureMax");
			this.setTimestamp(stmt, day, field++, "temperatureMaxTime");
			this.setDouble(stmt, day, field++, "apparentTemperatureMin");
			this.setTimestamp(stmt, day, field++, "apparentTemperatureMinTime");
			this.setDouble(stmt, day, field++, "apparentTemperatureMax");
			this.setTimestamp(stmt, day, field++, "apparentTemperatureMaxTime");
			this.setDouble(stmt, day, field++, "dewPoint");
			this.setDouble(stmt, day, field++, "humidity");
			this.setDouble(stmt, day, field++, "windSpeed");
			this.setDouble(stmt, day, field++, "windBearing");
			this.setDouble(stmt, day, field++, "visibility");
			this.setDouble(stmt, day, field++, "cloudCover");
			this.setDouble(stmt, day, field++, "pressure");
			this.setDouble(stmt, day, field++, "ozone");
			stmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			log.error("Unable to get person id", e);
			return false;
		} catch (JSONException e) {
			log.error("Failed to parse.", e);
			return false;
		} finally {
			this.closeEverything(con, stmt, null);
		}
	}

	/**
	 * the location query
	 */
	private static final String LocationQuery = "SELECT id, latitude, longitude "
			+ "FROM home_location ORDER BY id";

	/**
	 * Gets all of the locations
	 * 
	 * @return
	 */
	public List<LocationInfo> getLocations() {
		Connection con = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			con = this.dataSource.getConnection();
			stmt = con.prepareStatement(LocationQuery);
			rs = stmt.executeQuery();
			List<LocationInfo> list = new ArrayList<LocationInfo>();
			while (rs.next()) {
				int id = rs.getInt("id");
				double latitude = rs.getDouble("latitude");
				double longitude = rs.getDouble("longitude");
				LocationInfo person = new LocationInfo(id, latitude, longitude);
				list.add(person);
			}

			return list;
		} catch (SQLException e) {
			log.error("Unable to get person id", e);
			return null;
		} finally {
			this.closeEverything(con, stmt, rs);
		}
	}

	/**
	 * Sets the string from the JSON object
	 * 
	 * @param stmt
	 *            the statement
	 * @param json
	 *            the json
	 * @param num
	 *            the field number
	 * @param field
	 *            the field name
	 * @throws SQLException
	 * @throws JSONException
	 */
	private void setString(PreparedStatement stmt, JSONObject json, int num,
			String field) throws SQLException, JSONException {
		if (json.has(field)) {
			stmt.setString(num, json.getString(field));
		} else {
			stmt.setNull(num, Types.VARCHAR);
		}
	}

	/**
	 * Sets the double from the JSON object
	 * 
	 * @param stmt
	 *            the statement
	 * @param json
	 *            the json
	 * @param num
	 *            the field number
	 * @param field
	 *            the field name
	 * @throws SQLException
	 * @throws JSONException
	 */
	private void setDouble(PreparedStatement stmt, JSONObject json, int num,
			String field) throws SQLException, JSONException {
		if (json.has(field)) {
			stmt.setDouble(num, json.getDouble(field));
		} else {
			stmt.setNull(num, Types.DOUBLE);
		}
	}

	/**
	 * Sets the timestamp from the JSON object
	 * 
	 * @param stmt
	 *            the statement
	 * @param json
	 *            the json
	 * @param num
	 *            the field number
	 * @param field
	 *            the field name
	 * @throws SQLException
	 * @throws JSONException
	 */
	private void setTimestamp(PreparedStatement stmt, JSONObject json, int num,
			String field) throws SQLException, JSONException {
		if (json.has(field)) {
			stmt.setTimestamp(num, new Timestamp(json.getLong("time") * 1000));
		} else {
			stmt.setNull(num, Types.TIMESTAMP);
		}
	}

	/**
	 * Closes everything.
	 * 
	 * @param con
	 *            the connection
	 * @param stmt
	 *            the statement
	 * @param rs
	 *            the result set
	 */
	private void closeEverything(Connection con, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
			}
		}

		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
			}
		}

		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
			}
		}
	}
}
