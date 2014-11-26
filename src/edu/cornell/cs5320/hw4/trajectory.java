package edu.cornell.cs5320.hw4;

import com.mongodb.DBCollection;

public class trajectory {
	public static float latitude,longitude,altitude,date_passed;
	public static int zero;
	public static String date,time;

	public trajectory(){
		
	}

	public trajectory(String line){
		String[] info_set = line.split(",");
		this.latitude = Float.parseFloat(info_set[0]);
		this.longitude = Float.parseFloat(info_set[1]);
		this.zero = Integer.parseInt(info_set[2]);
		this.altitude = Float.parseFloat(info_set[3]);
		this.date_passed = Float.parseFloat(info_set[4]);
		this.date = info_set[5];
		this.time = info_set[6];
	}
}
