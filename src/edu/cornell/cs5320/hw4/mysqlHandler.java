package edu.cornell.cs5320.hw4;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class mysqlHandler {

	private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static String URL = "jdbc:mysql://localhost/homework";
	private static String USER = "homework";
	private static String PASS = "cornellproject";
	
	public static Connection getConnection(){
		try {
			Class.forName(JDBC_DRIVER);
		    Connection conn = DriverManager.getConnection(URL, USER, PASS);
		    return conn;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public static Statement createStatement(Connection conn){
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stmt;
	}
	public static void createDB(){
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();

		    // Do something with the Connection
		    stmt = conn.createStatement();
		    String drop_table_sql = "DROP TABLE IF EXISTS trajectory";
		    String create_table_sql = "CREATE TABLE trajectory(id INT NOT NULL AUTO_INCREMENT, " +
                    "traj_set_id VARCHAR(255) NOT NULL, traj_name VARCHAR(255) NOT NULL, latitude FLOAT, "+
		    		"longitude FLOAT, zero INT, altitude FLOAT, date_passed FLOAT,date DATE, time TIME, PRIMARY KEY (id))";
		    stmt.executeUpdate(drop_table_sql);
		    stmt.executeUpdate(create_table_sql);
		   
		} catch (Exception e) {
		    // handle any errors
		    e.printStackTrace();
		} 
	}
	
	public static void inesertTrajItem(Connection conn,String traj_set_id,String traj_name,trajectory t){
//		String insertTraj = "INSERT INTO trajectory (traj_set_id, traj_name,latitude,longitude,"+
//				"zero,altitude,date_passed,date,time) VALUES (?,?,?,?,?,?,?,?,?)";
	    try {
			// PreparedStatement preparedstmt = conn.prepareStatement(insertTraj);
			// preparedstmt.setString(1, traj_set_id);
			// preparedstmt.setString(2, traj_name);
   //  		preparedstmt.setFloat(3, t.latitude);
   //  		preparedstmt.setFloat(4,t.longtitude);
   //  		preparedstmt.setInt(5, t.zero);
   //  		preparedstmt.setFloat(6,t.altitude);
   //  		preparedstmt.setFloat(7,t.date_passed);
   //  		preparedstmt.setString(8,t.date);
   //  		preparedstmt.setString(9,t.time);
    		
   //  		preparedstmt.executeUpdate();
    		Statement stmt = mysqlHandler.createStatement(conn);
    		String sql = "INSERT INTO trajectory (traj_set_id,traj_name,latitude,longitude,zero,altitude,date_passed,date,time)"+
	    		  		"VALUES ('"+traj_set_id+"','"+traj_name+"','"+t.latitude+"','"+t.longitude+"','"+t.zero+"','"
	    		  			+t.altitude+"','"+t.date_passed+"','"+t.date+"','"+t.time+"')"; 
			stmt.executeUpdate(sql);
			stmt.close();
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static int executeQuery(Connection conn,String sql){
        int count = -1;

        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while(rs.next()){
                count = rs.getInt(1);
            }
        }
        catch(SQLException e){
            e.printStackTrace();
        }

        return count;
	}
}
