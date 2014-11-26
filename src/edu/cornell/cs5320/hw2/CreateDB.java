package edu.cornell.cs5320.hw2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class CreateDB {
	
	private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static String DB_URL = "jdbc:mysql://localhost/cs5320_hw2";
	//TODO read this from config file
	private static String DB_USERNAME = "root";
    private static String DB_PASSWD = "";

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: java CreateDB <file>");
			System.exit(0);
		}
		
		long start = System.currentTimeMillis();
		Connection conn;
		Statement stmt;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWD);
			
			//Create table
			stmt = conn.createStatement();
			String drop_table_sql = "DROP TABLE IF EXISTS road_net";
			String create_table_sql = "CREATE TABLE road_net(from_node INT NOT NULL, to_node INT NOT NULL" 
			                         + " ,INDEX (from_node), INDEX(to_node))";
			
			stmt.executeUpdate(drop_table_sql);
			stmt.executeUpdate(create_table_sql);
			stmt.close();
			System.out.println("Droped table if existed!");
			//Insert data
			String insert_sql = "INSERT INTO road_net (from_node, to_node) VALUES (?,?)";
			PreparedStatement preparedStmt = conn.prepareStatement(insert_sql);
			
			String inputFile = args[0];
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(inputFile));
				String line = "";
				String[] nodes;
				System.out.println("Started to insert data...");
				while ((line = br.readLine()) != null) {
					if(line.startsWith("#")) {
						continue;
					}
					nodes = line.split("\t");
					//nodes = line.split(" ");
					preparedStmt.setInt(1, Integer.parseInt(nodes[0]));
					preparedStmt.setInt(2, Integer.parseInt(nodes[1]));
					
					preparedStmt.executeUpdate();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			
			preparedStmt.close();
			conn.close();
			
			long end = System.currentTimeMillis();
			System.out.println("Finish Created DB in: " + (end-start)/1000d + " secs");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
