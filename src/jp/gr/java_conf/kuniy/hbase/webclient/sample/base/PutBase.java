package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import static jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.*;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTablePoolUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutBase extends HBaseClinetBase {

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		if (! check(conf, parameters)) {
			return CHECK_ERR_MSG;
		}

		HTable table = HTablePoolUtil.getTable(conf, parameters.get(TABLE_NAME));
		Put p = new Put(Bytes.toBytes(parameters.get(ROWKEY)));
		p.add(Bytes.toBytes(parameters.get(FAMILY_NAME)),
				Bytes.toBytes(parameters.get(COLUMN_NAME)),
				Bytes.toBytes(parameters.get(VALUE)));
		table.put(p);

		return true;
	}

	@Override
	public boolean check(Configuration conf, Map<String, String> parameters) {
		if (conf == null
				|| ! parameters.containsKey(TABLE_NAME)
				|| ! parameters.containsKey(ROWKEY)
				|| ! parameters.containsKey(FAMILY_NAME)
				|| ! parameters.containsKey(COLUMN_NAME)
				|| ! parameters.containsKey(VALUE)) {
			return false;
		}
		return true;
	}

}
