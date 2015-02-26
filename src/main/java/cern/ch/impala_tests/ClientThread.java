package cern.ch.impala_tests;

import java.sql.SQLException;
import java.util.Random;

public class ClientThread extends Thread {

	String[] queries;
	
	String[] clients ;
	
	int num_thread;
	
	Results results;
	
	public ClientThread(int num, String[] clients, String[] queries, Results results) {
		this.num_thread = num;
		this.queries = queries;
		this.clients = clients;
		this.results = results;
	}
	
	@Override
	public void run() {
		super.run();
		
		Random randomGenerator = new Random();
		
		for (int i = 0; i < 10; i++) {
			ImpalaClient client = new ImpalaClient(clients[randomGenerator.nextInt(clients.length)]);
			
			int q_i = randomGenerator.nextInt(queries.length);
			long time = System.currentTimeMillis();
			try {
				client.runQuery(queries[q_i]);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			time = System.currentTimeMillis() - time;
			
			System.out.println("Thread = " + num_thread
					+" Node = "+client.getHostname()
					+" Query = " + q_i
					+" Time = " + time + " ms");
			
			if(time > 0)
				results.add(q_i, time);
			
			client.close();
		}
		
	}
}
