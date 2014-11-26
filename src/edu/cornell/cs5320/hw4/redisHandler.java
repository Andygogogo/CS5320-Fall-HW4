package edu.cornell.cs5320.hw4;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.mongodb.DBCollection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;

public class redisHandler {
	public static final String URL = "localhost";
	public static void main(String[] args) {
//		Jedis jedis = new Jedis(URL);
//		System.out.println(jedis.get("counter"));
//		jedis.incr("counter");
//		System.out.println(jedis.get("counter"));
//		jedis.hset("user1:1:2","name","bill");
//		Map<String,String> user =  jedis.hgetAll("user1:1:2");
//		System.out.print(user.get("name"));
//		
//		jedis.flushAll();
//		 Map<String,String> user =  jedis.hgetAll("user:andy");
//		 
//		 System.out.println(user);
//		 
//
//		    Set<String> names=jedis.keys("*n*");
//		    System.out.println(names.size());
//		    Iterator<String> it = names.iterator();
//		    while (it.hasNext()) {
//		        String s = it.next();
//		        System.out.println(s);
//		    }

		 
//		jedis.quit();
	}
	
	public static void clearData(){
		Jedis jedis = new Jedis(URL);
		jedis.flushAll();
		jedis.quit();
	}
	
	public static void inesertTrajItem(Jedis jedis,int lineNumber,String traj_set_id,String traj_name,trajectory t)
	{
		HashMap<String, String> properties = new HashMap<String, String>();

		
		properties.put("traj_set_id", String.valueOf(traj_set_id));
		properties.put("traj_name", String.valueOf(traj_name));
		properties.put("latitude", String.valueOf(t.latitude));
		properties.put("longtitude", String.valueOf(t.longitude));
		properties.put("zero", String.valueOf(t.zero));
		properties.put("altitude", String.valueOf(t.altitude));
		properties.put("date_passed", String.valueOf(t.date_passed));
		properties.put("date", String.valueOf(t.date));
		properties.put("time", String.valueOf(t.time));
		
		String keyName = String.format("%s:%s:%d:%s", traj_set_id,traj_name,lineNumber,t.date);
		jedis.hmset(keyName, properties);
		
	}

	public static Jedis getJedis() {
		Jedis jedis = new Jedis(URL);
		return jedis;
	}
	
}
