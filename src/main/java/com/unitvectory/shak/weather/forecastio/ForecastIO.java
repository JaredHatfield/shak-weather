package com.unitvectory.shak.weather.forecastio;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

/**
 * The forecast.io client.
 * 
 * @author Jared Hatfield
 *
 */
public class ForecastIO {

	/**
	 * the log
	 */
	private static Logger log = Logger.getLogger(ForecastIO.class);

	/**
	 * the base URL
	 */
	private static final String baseUrl = "https://api.forecast.io/forecast/";

	/**
	 * the API key
	 */
	private String apiKey;

	/**
	 * Creates a new instance of the ForecastIO class.
	 * 
	 * @param apiKey
	 *            the api key
	 */
	public ForecastIO(String apiKey) {
		this.apiKey = apiKey;
	}

	/**
	 * Gets the weather for the specified location.
	 * 
	 * @param latitude
	 *            the latitude
	 * @param longitude
	 *            the longitude
	 * @return the weather JSON object
	 */
	public JSONObject getWeather(double latitude, double longitude) {
		String url = baseUrl + this.apiKey + "/" + latitude + "," + longitude;
		try {
			HttpClient client = new DefaultHttpClient();
			HttpGet get = new HttpGet(url);
			HttpResponse response = client.execute(get);
			if (response.getStatusLine().getStatusCode() == 200) {
				String json = EntityUtils.toString(response.getEntity(),
						"UTF-8");
				JSONObject obj = new JSONObject(json);
				return obj;
			}

			return null;
		} catch (Exception e) {
			log.error("Unable to get weather.", e);
			return null;
		}
	}
}
