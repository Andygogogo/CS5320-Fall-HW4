package edu.cornell.cs5320.hw4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;

public class Neo4jDB_Embedded {

	private static final String DB_PATH = "target/neo4j-db-assignment4";

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    
    private static void deleteFileOrDirectory( File file )
    {
        if ( file.exists() )
        {
            if ( file.isDirectory() )
            {
                for ( File child : file.listFiles() )
                {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }
    
    private static enum RelTypes implements RelationshipType
    {
        TO
    }
    
    public static void createDB() {
    	deleteFileOrDirectory( new File( DB_PATH ) );
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook( graphDb );
		ExecutionEngine engine = new ExecutionEngine( graphDb );
		
		try {
			String inputFile = "/Users/lilizhang/Desktop/DB_Assignment4/p2p-Gnutella04.txt";
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line = "";
			String[] nodes;
			
			while ((line = br.readLine()) != null) {
				if(line.startsWith("#")) {
					continue;
				}
				nodes = line.split("\t");
				//nodes = line.split(" ");
				System.out.println(nodes[0] + " " + nodes[1]);
				
				try ( Transaction tx = graphDb.beginTx() )
				{
				    engine.execute( "MERGE (firstNode {id:" + nodes[0] + "}) RETURN firstNode" );
				    engine.execute( "MERGE (secondNode {id:" + nodes[1] + "}) RETURN secondNode" );
				    
				    engine.execute("MATCH (firstNode {id:" + nodes[0] + "}), "
				    		       + "(secondNode {id:" + nodes[1] + "}) "
				    			   + "MERGE (firstNode)-[r:TO]->(secondNode) RETURN r");

				    tx.success();
				}

			}
			
			br.close();
			graphDb.shutdown();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
    }
    
    public static void neighbourCount(GraphDatabaseService graphDb, int nodeId) {
		ExecutionEngine engine = new ExecutionEngine( graphDb );
		
		ExecutionResult result;
		//Neighbor counts
		try ( Transaction ignored = graphDb.beginTx() )
		{
			result = engine.execute("MATCH (node {id:" + nodeId + "})-[r:TO]-(x) "
					                + "RETURN COUNT(DISTINCT x)");
		    
		    String rows = "";
		    for ( Map<String, Object> row : result)
		    {
		        for ( Entry<String, Object> column : row.entrySet() )
		        {
		            rows += column.getValue() + "; ";
		        }
		    }
		    
		    System.out.println("Neighbours Count: " + rows);
		}

    }
    
    public static void reachabilityCount(GraphDatabaseService graphDb, int nodeId) {
		ExecutionEngine engine = new ExecutionEngine( graphDb );
		
		ExecutionResult result;
		//Reachability counts
		try ( Transaction ignored = graphDb.beginTx() )
		{	
			Node node = null;
			result = engine.execute("MATCH (N {id:" + nodeId + "}) RETURN N");
			for( Map<String, Object> row : result) {
				
				for ( Entry<String, Object> column : row.entrySet() )
		        {
		            node = (Node)column.getValue();
		        }

		    }
			
		    int count = 0;
			for(Path nodePath : graphDb.traversalDescription().breadthFirst()
			        .relationships(RelTypes.TO, Direction.OUTGOING)
			        .evaluator(Evaluators.excludeStartPosition())
			        .traverse( node ) ) {
				
				count++;
				
			}
			
			System.out.println("Reachability Count: " + count);
		}
    }
    
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: java Neo4jDB_Embedded createDB|neighbourCount|reachabilityCount "
					           + "<file|nodeid>");
			System.exit(0);
		}
		
		String type = args[0];
		int nodeId = 0;
		if(args.length == 2) {
			nodeId = Integer.parseInt(args[1]);
		}
		
		long start = System.currentTimeMillis();
		
		try {

			if(type.equals("createDB")) {
				createDB();
			} else {
				GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
				registerShutdownHook( graphDb );
				
				long start_query = System.currentTimeMillis();
				if(type.equals("neighbourCount")) {
					neighbourCount(graphDb, nodeId);
				} else if(type.equals("reachabilityCount")) {
					reachabilityCount(graphDb, nodeId);
				} else {
					System.out.println("Unsupported Type!");
				}
				long end_query = System.currentTimeMillis();
				System.out.println("Query finished in: " + (end_query-start_query)/1000d + " secs");
				
				graphDb.shutdown();
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Finish total in: " + (end-start)/1000d + " secs");
	}

}
