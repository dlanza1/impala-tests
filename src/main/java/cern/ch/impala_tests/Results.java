package cern.ch.impala_tests;

import java.util.LinkedList;

public class Results {

	LinkedList<Long>[] results;
	
	public Results(int num_queries) {
		results = new LinkedList[num_queries];
	}
	
	public void add(int num_query, long time){
		
		if(results[num_query] == null)
			results[num_query] = new LinkedList<Long>();
		
		results[num_query].add(time);
	}
	
	public void show(){
		for (int i = 0; i < results.length; i++) {
			if(results[i] == null)
				System.out.println("Query = " + i + " does not have results.");
			else{
				long sum = 0;

				for (int j = 0; j < results[i].size(); j++) {
					sum += results[i].get(j);
				}
				
				System.out.println("Query = " + i + " Avg = " + (sum / results[i].size()) + " ms");
			}
				
		}
	}
	
}
