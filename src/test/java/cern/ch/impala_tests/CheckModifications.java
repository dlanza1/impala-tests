package cern.ch.impala_tests;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

public class CheckModifications {

	@Test
	public void normalquery() {
		ImpalaClient c = new ImpalaClient("itrac925");
		
		try {
			int numRecords = numRecords(c.runQuery("select min(utc_stamp) as min_stamp, avg(value) as aggregated_value, count(*) as coun "
									+ "from lhclog.data_numeric_part_var_id_4 "
									+ "where variable_id = 929658 "
									+ "and utc_stamp between cast('2012-08-10 00:00:00' as timestamp) "
									+ "and cast('2012-08-17 23:59:59' as timestamp)"));
			
			assertEquals(numRecords, 1);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		}
		
	}
	
	@Test
	public void subquery() {
		ImpalaClient c = new ImpalaClient("itrac925");
		
		try {
			int numRecords = numRecords(c.runQuery("select l.element_name, v.ts, v.value_number from ("
				+ "select element_id, max(ts) as last_ts "
				+ " from scadar.moon_eventhistory_part "
				+ "where ts between cast('2014-06-01 00:00:00' as timestamp) "
				+ "and cast('2014-06-02 23:59:59' as timestamp)group by element_id"
				+ ") vm, scadar.moon_eventhistory_part v, scadar.moon_elements l "
				+ "where v.element_id = vm.element_id and l.element_id = v.element_id "
				+ "and vm.last_ts = v.ts "
				+ "and ts between cast('2014-06-01 00:00:00' as timestamp) "
				+ "and cast('2014-06-02 23:59:59' as timestamp)"));
			
			assertEquals(numRecords, 90);
		} catch (SQLException e) {
			e.printStackTrace();
			fail();
		}
		
	}

	private int numRecords(ResultSet rs) throws SQLException {
		int count = 0;
		while(rs.next()) count++;
		return count;
	}
}
