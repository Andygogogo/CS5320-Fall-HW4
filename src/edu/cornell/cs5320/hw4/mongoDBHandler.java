package edu.cornell.cs5320.hw4;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

public class mongoDBHandler {
	private static String DB_NAME = "hw4";
	private static String COLLECTION_NAME = "trajectory";
	
	public static void main(String args[]){
//		System.out.println("start!");
//		getCollection();
	}
	
	public static DBCollection getCollection (){
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			DB db = mongoClient.getDB( DB_NAME );
			DBCollection coll = db.getCollection(COLLECTION_NAME);
			return coll;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
		return null;
	}
	
	
	public static void inesertTrajItem(DBCollection coll,String traj_set_id,String traj_name,trajectory t){
		BasicDBObject  traj = new BasicDBObject("traj_set_id",traj_set_id)
							.append("traj_name",traj_name)
							.append("latitude",t.latitude)
							.append("longitude",t.longitude)
							.append("zero",t.zero)
							.append("altitude",t.altitude)
							.append("date_passed",t.date_passed)
							.append("date",t.date)
							.append("time",t.time);
		coll.insert(traj);
	}
	
	
	public static void clearCollection(){
		DBCollection coll = getCollection();
		coll.drop();
	}
}
