package com.unitvectory.shak.weather.db.model;

/**
 * The location information.
 * 
 * @author Jared Hatfield
 *
 */
public class LocationInfo {

	/**
	 * the id
	 */
	private int id;

	/**
	 * the latitude
	 */
	private double latitude;

	/**
	 * the longitude
	 */
	private double longitude;

	/**
	 * Creates a new instance of the LocationInfo class.
	 * 
	 * @param id
	 *            the id
	 * @param latitude
	 *            the latitude
	 * @param longitude
	 *            the longitude
	 */
	public LocationInfo(int id, double latitude, double longitude) {
		this.id = id;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @return the latitude
	 */
	public double getLatitude() {
		return latitude;
	}

	/**
	 * @return the longitude
	 */
	public double getLongitude() {
		return longitude;
	}
}
