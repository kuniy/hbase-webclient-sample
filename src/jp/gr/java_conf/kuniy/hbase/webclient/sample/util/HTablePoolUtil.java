package jp.gr.java_conf.kuniy.hbase.webclient.sample.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * HTablePool Utility Class (thread safe)
 */
public class HTablePoolUtil {

	public static final int POOL_SIZE = 5;

	private static HTablePool pool = null;

	/**
	 * Get HTable from HTablePool
	 *
	 * @param conf org.apache.hadoop.conf.Configuration
	 * @param tableName String
	 * @return org.apache.hadoop.hbase.client.HTable
	 * @throws IOException
	 */
	static public HTable getTable(Configuration conf, String tableName) throws IOException {
		if (pool == null) {
			pool = new HTablePool(conf, POOL_SIZE);
		}
	    return (HTable) pool.getTable(tableName);
	}

	/**
	 * Put HTable to HTabePool
	 *
	 * @param table  org.apache.hadoop.hbase.client.HTable
	 * @throws IOException
	 */
	static public void putTable(HTable table) throws IOException {
		 if (table != null) {
			 // pool.putTable(table);   // Deprecated
			 table.close();
		 }
	}
}
