package com.unitvectory.shak.weather.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
			+ "(location, time, summary, added, updated) "
			+ "VALUES (?, ?, ?, NOW(), NOW()) "
			+ "ON DUPLICATE KEY UPDATE "
			+ "summary = VALUES(summary), updated = NOW() ";

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
			stmt.setInt(1, location);
			stmt.setTimestamp(2, new Timestamp(day.getLong("time") * 1000));
			stmt.setString(3, day.getString("summary"));
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
	 * Closes everything.
	 * 
	 * @param con
	 *            the connection
	 * @param stmt
	 *            the statement
	 * @param rs
	 *            the result set
	 */
	protected void closeEverything(Connection con, Statement stmt, ResultSet rs) {
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
