package cern.ch.impala_tests;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ImpalaClient {

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	private Connection con = null;

	private String hostname;
	
	public ImpalaClient(String host){
		this(host, 21050);
	}
	
	public ImpalaClient(String host, int port){
		this.hostname = host;
		
		String connection_url = "jdbc:hive2://" + host + ':' + port + "/;auth=noSasl";

//		System.out.println("Connecting: " + connection_url);
		
		try {
			Class.forName(JDBC_DRIVER_NAME);

			con = DriverManager.getConnection(connection_url);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public long runQuery(String statement){
		try{
			Statement stmt = con.createStatement();
	
//			System.out.println("Running Query: " + statement);
			long t = System.currentTimeMillis();
			
			ResultSet rs = stmt.executeQuery(statement + "");
	
//			System.out.println("\n== Begin Query Results ==");
			
			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
//				System.out.print(rs.getString(1));
//				System.out.print(" | ");
//				System.out.print(rs.getString(2));
//				System.out.print(" | ");
//				System.out.println(rs.getString(3));
			}
	
//			System.out.println("== End Query Results == " + (System.currentTimeMillis() - t) + " ms\n\n");
			
			return System.currentTimeMillis() - t;
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
		return -1;
	}
	
	public void close(){
		try {
//			System.out.println("Closing connection...");
			con.close();
		} catch (Exception e) {
			// swallow
		}
	}

	public String getHostname() {
		return hostname;
	}
}
