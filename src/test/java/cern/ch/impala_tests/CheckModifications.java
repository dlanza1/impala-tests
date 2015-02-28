package cern.ch.impala_tests;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

public class CheckModifications {

	ImpalaClient c_mod;
	ImpalaClient c_ori;

	public CheckModifications() {
		// c_mod = new ImpalaClient("itrac925");
		// c_ori = new ImpalaClient("itrac926");

		// External tests
		// ssh dlanza@lxplus.cern.ch -L 21051:itrac925.cern.ch:21050 -N
		// ssh dlanza@lxplus.cern.ch -L 21050:itrac926.cern.ch:21050 -N
		c_mod = new ImpalaClient("localhost", 21051);
		c_ori = new ImpalaClient("localhost", 21050);
	}
	
	@Test
	public void normalquery() throws SQLException {
		String query = "select variable_id, utc_stamp, value "
				+ "from lhclog.data_numeric_part_var_id_4 "
				+ "where variable_id = 929658 "
				+ "and utc_stamp between cast('2012-08-10 00:00:00' as timestamp) "
				+ "and cast('2012-08-10 01:23:45' as timestamp) "
				+ "order by variable_id, utc_stamp ";

		compareResultSets(c_mod.runQuery(query), c_ori.runQuery(query));
	}

	private void compareDoubles(Double dif_allowed, Double val1, Double val2) {
		BigDecimal value_mod = new BigDecimal(val1);
		BigDecimal value_ori = new BigDecimal(val2);

		if (value_mod.compareTo(value_ori) != 0) {
			BigDecimal dif = value_mod.subtract(value_ori).abs();

			if (dif.floatValue() > dif_allowed) {
				fail("more than " + dif_allowed + " of difference = " + dif);
			} else {
				System.out.println("WARNING: there was a difference in the values of " + dif);
			}
		}
	}

	//@Test
	public void subquery() throws SQLException {
		String query = "select l.element_name, v.ts, v.value_number from ("
				+ "select element_id, max(ts) as last_ts "
				+ " from scadar.moon_eventhistory_part "
				+ "where ts between cast('2014-06-01 00:00:00' as timestamp) "
				+ "and cast('2014-06-02 23:59:59' as timestamp)group by element_id"
				+ ") vm, scadar.moon_eventhistory_part v, scadar.moon_elements l "
				+ "where v.element_id = vm.element_id and l.element_id = v.element_id "
				+ "and vm.last_ts = v.ts "
				+ "and ts between cast('2014-06-01 00:00:00' as timestamp) "
				+ "and cast('2014-06-02 23:59:59' as timestamp) order by l.element_name, v.ts, v.value_number";

		compareResultSets(c_mod.runQuery(query), c_ori.runQuery(query));
	}

	private void compareResultSets(ResultSet rs1, ResultSet rs2) throws SQLException {
		while (rs1.next()) {
			if (!rs2.next())
				fail("with the modifications got less rows");

			if (rs1.getRow() % 1000 == 0)
				System.out.println("Row number: " + rs1.getRow());

			assertEquals(rs2.getObject(1), rs1.getObject(1));
			assertEquals(rs2.getObject(2), rs1.getObject(2));
			assertEquals(rs2.getObject(3), rs1.getObject(3));
		}

		if (rs2.next())
			fail("with the modifications got more rows");

		rs1.close();
		rs2.close();
	}

}
