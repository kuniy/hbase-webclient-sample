package jp.gr.java_conf.kuniy.hbase.webclient.sample.useraction;

import static jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.PutBase;

import org.apache.hadoop.conf.Configuration;

public class UserActionHTable {

	public static final String USER_ACTION_TABLE_NAME = "USERACTION";

	public static final String USER_ACTION_FAMILY_NAME = "EXEC";

	public static final String USER_ACTION_COL_PARAMS_NAME = "PARAMETERS";

	public static final String USER_ACTION_COL_EXECNANOTIME_NAME = "EXECNANOTIME";

	public void put(Configuration conf, String sessionid, String action, Long execnanotime, Map<String, String> parameters) throws IOException {
		StringBuilder sbKey = new StringBuilder(sessionid);
		sbKey.append(".");
		sbKey.append(action);
		sbKey.append(".");
		sbKey.append(Long.MAX_VALUE - System.currentTimeMillis());
		String key = sbKey.toString();

		StringBuilder sbParams = new StringBuilder();
		String delim = "";
		TreeMap<String, String> treemap = new TreeMap<String, String>(parameters);
		for (Iterator<String> it = treemap.keySet().iterator(); it.hasNext(); ) {
			String paramKey = it.next();
			sbParams.append(delim);
			sbParams.append(paramKey);
			sbParams.append("=");
			sbParams.append(treemap.get(paramKey));
			delim = "&";
		}

		// put parameters
		PutBase putBase = new PutBase();
		Map<String, String> mapParam = new HashMap<String, String>();
		mapParam.put(TABLE_NAME, USER_ACTION_TABLE_NAME);
		mapParam.put(ROWKEY, key);
		mapParam.put(FAMILY_NAME, USER_ACTION_FAMILY_NAME);
		mapParam.put(COLUMN_NAME, USER_ACTION_COL_PARAMS_NAME);
		mapParam.put(VALUE, sbParams.toString());
		putBase.execute(conf, mapParam);

		// put execnanotime
		Map<String, String> mapExec = new HashMap<String, String>();
		mapExec.put(TABLE_NAME, USER_ACTION_TABLE_NAME);
		mapExec.put(ROWKEY, key);
		mapExec.put(FAMILY_NAME, USER_ACTION_FAMILY_NAME);
		mapExec.put(COLUMN_NAME, USER_ACTION_COL_EXECNANOTIME_NAME);
		mapExec.put(VALUE, String.valueOf(execnanotime));
		putBase.execute(conf, mapExec);
	}
}
