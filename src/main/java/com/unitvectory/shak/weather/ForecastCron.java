package com.unitvectory.shak.weather;

import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.unitvectory.shak.weather.db.WeatherDatabase;
import com.unitvectory.shak.weather.db.model.LocationInfo;
import com.unitvectory.shak.weather.forecastio.ForecastIO;

/**
 * The forecast cron logic.
 * 
 * @author Jared Hatfield
 *
 */
public class ForecastCron {

	/**
	 * the log
	 */
	private static Logger log = Logger.getLogger(ForecastCron.class);

	/**
	 * the forecast.io client
	 */
	private ForecastIO forecastIO;

	/**
	 * the database client
	 */
	private WeatherDatabase database;

	/**
	 * Creates a new instance of the ForecastCron class.
	 * 
	 * @param config
	 *            the config
	 */
	public ForecastCron(AppConfig config) {
		this.forecastIO = new ForecastIO(config.getForecastIo());
		this.database = new WeatherDatabase(config);
	}

	/**
	 * Processes the logic.
	 * 
	 * @throws Exception
	 */
	public void process() throws Exception {
		List<LocationInfo> locations = database.getLocations();
		for (LocationInfo location : locations) {
			log.info("Loading forecast for " + location.getLatitude() + ","
					+ location.getLongitude());
			JSONObject forecast = this.forecastIO.getWeather(
					location.getLatitude(), location.getLongitude());
			JSONObject daily = forecast.getJSONObject("daily");
			JSONArray data = daily.getJSONArray("data");
			for (int i = 0; i < data.length(); i++) {
				JSONObject day = data.getJSONObject(i);
				log.info(day.getString("summary"));
				this.database.insert(location.getId(), day);
			}
		}
	}
}
