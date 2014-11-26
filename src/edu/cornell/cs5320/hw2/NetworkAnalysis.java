package edu.cornell.cs5320.hw2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import edu.cornell.cs5320.hw2.graph.DijkstraSP;
import edu.cornell.cs5320.hw2.graph.DirectedEdge;
import edu.cornell.cs5320.hw2.graph.EdgeDigraph;
import edu.cornell.cs5320.hw2.graph.Graph;

public class NetworkAnalysis {
	private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static String DB_URL = "jdbc:mysql://localhost/cs5320_hw2";
	// TODO read this from config file
	private static String DB_USERNAME = "root";
	private static String DB_PASSWD = "";

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: java NetworkAnalysis <id>");
			System.exit(0);
		}
		String type = args[0];

		Connection conn;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWD);

			if (type.equalsIgnoreCase("NeighbourCount")) { // Question 1
				if (args.length != 2) {
					System.out.println("Usage: java NetworkAnalysis <id>");
					System.exit(0);
				}
				String node_id = args[1];
				int count = networkAnalysis(node_id, conn);
				System.out.println("Q1 NeighbourCount for node_id = "
						+ node_id + ": count=" + count);
			} else if (type.equalsIgnoreCase("ReachabilityCount")) { // Question
																		// 2
				if (args.length != 2) {
					System.out.println("Usage: java NetworkAnalysis <id>");
					System.exit(0);
				}
				String node_id = args[1];
				int count = reachabilityCount(node_id, conn);
				System.out.println("Q2 ReachabilityCount for node_id = "
						+ node_id + ": count=" + count);

			} else if (type.equalsIgnoreCase("DiscoverCliques")) { // Question 3
				if (args.length != 2) {
					System.out.println("Usage: java NetworkAnalysis <id>");
					System.exit(0);
				}
				int clique = Integer.parseInt(args[1]);
				LinkedList<ArrayList<Integer>> result = discoverCliques(clique, conn);
				int r_count = 0;
				for (ArrayList<Integer> c : result) {
					if (c.size() > clique - 1) {
						System.out.print(r_count + ": ");
						for (Integer i : c) {
							System.out.print(i + " ");
						}
						r_count++;
						System.out.println("");		
					}	
			    }
				System.out.println("Q3 DiscoverCliques for " + clique + " is " + result.size());

			} else if (type.equalsIgnoreCase("NetworkDiameter")) { // Question 4

				int diameter = networkDiameter(conn);
				System.out.println("Q4 Graph Diameter is: " + diameter);

			} else {
				System.out.println("Unknown Type!");
			}

			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Question 1 solution
	private static int networkAnalysis(String node_id, Connection conn) {
		long start = System.currentTimeMillis();
		int count = -1;
		// String count_sql = "SELECT count(DISTINCT to_node) FROM road_net WHERE from_node=" + node_id;
		String count_sql = "SELECT COUNT(*) FROM (SELECT DISTINCT to_node from road_net WHERE from_node="
				+ node_id
				+ " UNION SELECT DISTINCT from_node from road_net WHERE to_node="
				+ node_id + ") AS T";
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(count_sql);
			while (rs.next()) {
				count = rs.getInt(1);
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("networkAnalysis execute time is: " + (end - start)
				/ 1000d + " sec");
		return count;
	}

	// Question 2 solution
	private static int reachabilityCount(String node_id, Connection conn) {
		long start = System.currentTimeMillis();
		int count = -1;
		int from_node, to_node;

		String count_sql = "SELECT MAX(node_id) FROM (SELECT MAX(from_node) AS node_id from road_net "
				+ "UNION SELECT MAX(to_node) from road_net) AS T";
		String sql = "SELECT from_node, to_node FROM road_net";
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs_count = stmt.executeQuery(count_sql);
			int v = 0;
			while (rs_count.next()) {
				v = rs_count.getInt(1) + 1;
			}

			System.out.println("Reading data into adjacency list graph structure...");
			Graph graph = new Graph(v);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				from_node = rs.getInt(1);
				to_node = rs.getInt(2);
				graph.addEdge(from_node, to_node);
			}

			System.out.println("V:" + graph.V() + " E:" + graph.E());
			System.out.println("BFS...");
			count = bfs(graph, Integer.parseInt(node_id));

			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		long end = System.currentTimeMillis();
		System.out.println("reachabilityCount execute time is: "
				+ (end - start) / 1000d + " sec");
		return count;
	}

	// Breadth first search
	public static int bfs(Graph G, int node_id) {
		int count = 1;
		boolean[] visited = new boolean[G.V()];
		Queue<Integer> q = new LinkedList<Integer>();
		q.add(node_id);
		visited[node_id] = true;
		while (!q.isEmpty()) {
			int i = q.remove();
			for (Integer j : G.adj(i)) {
				if (!visited[j]) {
					q.add(j);
					visited[j] = true;
					count++;
				}
			}
		}
		return count;
	}

	// Question 3 solution
	private static LinkedList<ArrayList<Integer>> discoverCliques(int k, Connection conn) {
		if (k < 2) {
			System.out.println("Not support 0 or 1 clique");
			System.exit(0);
		}

		long start = System.currentTimeMillis();
		int from_node, to_node;
		LinkedList<ArrayList<Integer>> result_cliques = new LinkedList<ArrayList<Integer>>();

		String count_sql = "SELECT MAX(node_id) FROM (SELECT MAX(from_node) AS node_id from road_net "
				+ "UNION SELECT MAX(to_node) from road_net) AS T";
		String sql = "SELECT from_node, to_node FROM road_net";

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs_count = stmt.executeQuery(count_sql);
			int v = 0;
			while (rs_count.next()) {
				v = rs_count.getInt(1) + 1;
			}

			LinkedList<ArrayList<Integer>> clique_list = new LinkedList<ArrayList<Integer>>();

			Graph graph = new Graph(v);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				from_node = rs.getInt(1);
				to_node = rs.getInt(2);
				graph.addEdge(from_node, to_node);

				ArrayList<Integer> clique = new ArrayList<Integer>();
				clique.add(from_node);
				clique.add(to_node);

				clique_list.add(clique);
			}

			System.out.println("V:" + graph.V() + " E:" + graph.E());

			System.out.println("Start to discover cliques");

			for (int r = 2; r < k; r++) {
				result_cliques.clear();
				for (int t = 0; t < graph.V(); t++) {

					for (ArrayList<Integer> cl : clique_list) {

						int connected_num = 0;
						for (Integer i : cl) {
							for (Integer j : graph.adj(i)) {
								if (j == t) {
									connected_num++;
								}
							}
						}
						
						if (connected_num == r) {
							ArrayList<Integer> ncl = (ArrayList<Integer>)cl.clone();
							ncl.add(t);
							result_cliques.add(ncl);
						}

					}
				}
				
				clique_list = (LinkedList<ArrayList<Integer>>) result_cliques.clone();
			}

			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("reachabilityCount execute time is: "
				+ (end - start) / 1000d + " sec");
		
		return result_cliques;
	}

	// Question 4 solution
	private static int networkDiameter(Connection conn) {
		long start = System.currentTimeMillis();
		int from_node, to_node;
		int diameter = 0;

		String count_sql = "SELECT MAX(node_id) FROM (SELECT MAX(from_node) AS node_id from road_net "
				+ "UNION SELECT MAX(to_node) from road_net) AS T";
		String sql = "SELECT from_node, to_node FROM road_net";

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs_count = stmt.executeQuery(count_sql);
			int v = 0;
			while (rs_count.next()) {
				v = rs_count.getInt(1) + 1;
			}

			EdgeDigraph G = new EdgeDigraph(v + 3);

			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				from_node = rs.getInt(1);
				to_node = rs.getInt(2);
				DirectedEdge de = new DirectedEdge(from_node, to_node, 1);
				G.addEdge(de);
			}

			System.out.println("V:" + G.V() + " E:" + G.E());

			StringBuilder longest_path = null;
			System.out.println("Start Dijkstra for each node ...");
			for (int i = 0; i < G.V(); i++) {
				DijkstraSP sp = new DijkstraSP(G, i);

				// get shortest path
				for (int t = 0; t < G.V(); t++) {
					if (sp.hasPathTo(t)) {
						int dist = (int) sp.distTo(t);
						if (diameter < dist) {
							diameter = dist;
							longest_path = new StringBuilder();
							for (DirectedEdge e : sp.pathTo(t)) {
								longest_path.append(e.from() + "   ");
							}
						}
					}
				}
			}

			System.out.println("Longest path: " + longest_path);

			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("reachabilityCount execute time is: "
				+ (end - start) / 1000d + " sec");

		return diameter;
	}
}
