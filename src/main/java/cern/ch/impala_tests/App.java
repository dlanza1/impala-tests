package cern.ch.impala_tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Hello world!
 *
 */
public class App {
	

	public static void main(String[] args) throws IOException, InterruptedException {

		String[] queries = {"select min(utc_stamp) as min_stamp, avg(value) as aggregated_value, count(*) as count "
			     + "from lhclog.data_numeric_part_var_id "
			     + "where variable_id = 929658 and variable_id_part_mod_2500 = pmod(2500, 929658)"
				 + "and utc_stamp between cast('2012-08-01 00:00:00' as timestamp) "
				 + "and cast('2012-08-17 23:59:59' as timestamp)"
				 
				 ,"select year(ts), month(ts), day(ts), element_id, min(value_number), avg(value_number), max(value_number), stddev(value_number) "
				 + "from scadar.moon_eventhistory_part "
				 + "where ts between cast('2014-06-01 00:00:00' as timestamp) "
				 + "and cast('2014-06-02 23:59:59' as timestamp) "
				 + "group by year(ts), month(ts), day(ts), element_id "
				 + "order by year(ts), month(ts), day(ts), element_id"
				 
				};
		
		String[] clients = {"itrac902",
							"itrac911",
							"itrac912",
							"itrac913",
							"itrac916",
							"itrac925",
							"itrac926",
							"itrac927"};
		
		for (int i = 0; i < queries.length; i++) {
			System.out.println(i + ": " + queries[i]);
		}
		
		ClientThread[] threads = new ClientThread[4];
		Results results = new Results(queries.length);
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new ClientThread(i, clients, queries, results);
			threads[i].start();
		}
		
		for (int i = 0; i < threads.length; i++) {
			threads[i].join();
		}
		
		results.show();
	}
	
}
