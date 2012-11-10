package jp.gr.java_conf.kuniy.hbase.webclient.sample.init;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTableUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Create HBase tables for "hbase-webclient-sample"
 */
public class InitializeHBase {

	public static final String TABLE_NAME = "USERACTION";
	public static final String FAMILY_NAME = "EXEC";

	/**
	 * Create HBase tables for "hbase-webclient-sample"
	 *
	 * @param args : args[0] - hbase-site.xml file pass
	 */
	public static void main(String[] args) throws Exception {
		// check args
		if (args.length < 1) {
			System.out.println("Usage: java InitializeHBase <hbase-site.xml path>");
		}

		// get configuration
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(args[0]));

		// create table
		HTableUtil.createHTable(conf, TABLE_NAME, FAMILY_NAME);

		// show attributes
		HTableUtil.showHTableAttributes(conf, TABLE_NAME);
	}

}
