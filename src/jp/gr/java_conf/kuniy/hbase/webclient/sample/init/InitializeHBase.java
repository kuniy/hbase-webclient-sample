package jp.gr.java_conf.kuniy.hbase.webclient.sample.init;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.useraction.UserActionHTable;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTableUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Create HBase tables for "hbase-webclient-sample"
 */
public class InitializeHBase {

	/**
	 * Create HBase tables for "hbase-webclient-sample"
	 *
	 * @param args : args[0] - hbase-site.xml file pass
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: java InitializeHBase <hbase-site.xml path>");
		}
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(args[0]));

		for (Iterator<Map.Entry<String, String>> e = conf.iterator(); e.hasNext(); ) {
			Entry<String, String> map = e.next();
			System.out.println("CONF KEY : " + map.getKey() + ", VALUE : " + map.getValue());
		}

		HTableUtil.createHTable(conf, UserActionHTable.USER_ACTION_TABLE_NAME, UserActionHTable.USER_ACTION_FAMILY_NAME);
		HTableUtil.showHTableAttributes(conf, UserActionHTable.USER_ACTION_TABLE_NAME);
	}

}
