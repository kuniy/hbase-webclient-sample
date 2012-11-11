package jp.gr.java_conf.kuniy.hbase.webclient.sample.useraction;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class UserActionHTable {

	public static final String TABLE_NAME = "USERACTION";

	public static final String FAMILY_NAME = "EXEC";

	public static final String COL_ACTION_NAME = "ACTION";

	public static final String COL_TABLE_NAME = "TABLE";

	public static final String COL_PARAM_NAME = "PARAM";

	public void put(Configuration conf, String sessionid,
			String action, String tableName, Map<String, String> parameters) {

	}
}
