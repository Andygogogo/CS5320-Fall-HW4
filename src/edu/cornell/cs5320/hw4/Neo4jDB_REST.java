package edu.cornell.cs5320.hw4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Neo4jDB_REST {
	
	static String SERVER_ROOT_URI = "http://localhost:7474";
	
	public static void connectDB() {
		WebResource resource = Client.create()
		        .resource( SERVER_ROOT_URI );
		
		ClientResponse response = resource.get( ClientResponse.class );

		System.out.println( String.format( "GET on [%s], status code [%d]",
		        SERVER_ROOT_URI, response.getStatus() ) );
		response.close();
	}
	
	
	public static URI createNode(int ID) {
		final String nodeEntryPointUri = SERVER_ROOT_URI + "/db/data/node";
		// http://localhost:7474/db/data/node

		WebResource resource = Client.create()
		        .resource( nodeEntryPointUri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
		        .type( MediaType.APPLICATION_JSON )
		        .entity( "{\"ID\" : \"" + ID + "\"}" )
		        .post( ClientResponse.class );

		final URI location = response.getLocation();
		System.out.println( String.format(
		        "POST to [%s], status code [%d], location header [%s]",
		        nodeEntryPointUri, response.getStatus(), location.toString() ) );
		response.close();

		return location;
	}
	
	private static URI addRelationship( URI startNode, URI endNode,
	        String relationshipType, String jsonAttributes )
	        throws URISyntaxException
	{
	    URI fromUri = new URI( startNode.toString() + "/relationships" );
	    String relationshipJson = generateJsonRelationship( endNode,
	            relationshipType, jsonAttributes );

	    WebResource resource = Client.create()
	            .resource( fromUri );
	    // POST JSON to the relationships URI
	    ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
	            .type( MediaType.APPLICATION_JSON )
	            .entity( relationshipJson )
	            .post( ClientResponse.class );

	    final URI location = response.getLocation();
	    System.out.println( String.format(
	            "POST to [%s], status code [%d], location header [%s]",
	            fromUri, response.getStatus(), location.toString() ) );

	    response.close();
	    return location;
	}
	
	private static String generateJsonRelationship( URI endNode,
            String relationshipType, String... jsonAttributes )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "{ \"to\" : \"" );
        sb.append( endNode.toString() );
        sb.append( "\", " );

        sb.append( "\"type\" : \"" );
        sb.append( relationshipType );
        if ( jsonAttributes == null || jsonAttributes.length < 1 )
        {
            sb.append( "\"" );
        }
        else
        {
            sb.append( "\", \"data\" : " );
            for ( int i = 0; i < jsonAttributes.length; i++ )
            {
                sb.append( jsonAttributes[i] );
                if ( i < jsonAttributes.length - 1 )
                { // Miss off the final comma
                    sb.append( ", " );
                }
            }
        }

        sb.append( " }" );
        return sb.toString();
    }

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		
		try {
			String inputFile = "/Users/lilizhang/Desktop/DB_Assignment4/roadNet-CA.txt";
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line = "";
			String[] nodes;
			while ((line = br.readLine()) != null) {
				if(line.startsWith("#")) {
					continue;
				}
				nodes = line.split("\t");
				System.out.println(nodes[0] + " " + nodes[1]);
			}
			
			br.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Finish Created DB in: " + (end-start)/1000d + " secs");
	}

}
