package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import static jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.*;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTablePoolUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetBase extends HBaseClinetBase {

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		if (! check(conf, parameters)) {
			return CHECK_ERR_MSG;
		}

		HTable table = HTablePoolUtil.getTable(conf, parameters.get(TABLE_NAME));
		Get g = new Get(Bytes.toBytes(parameters.get(ROWKEY)));
		if (parameters.containsKey(FAMILY_NAME)) {
			g.addFamily(Bytes.toBytes(parameters.get(FAMILY_NAME)));
		}
		if (parameters.containsKey(FAMILY_NAME) && parameters.containsKey(COLUMN_NAME)) {
			g.addColumn(Bytes.toBytes(parameters.get(FAMILY_NAME)), Bytes.toBytes(parameters.get(COLUMN_NAME)));
		}
		Result result = table.get(g);

		String map = "<BR/>";
		String row = Bytes.toString(result.getRow());
		for (KeyValue kv : result.raw()) {
			String fam = Bytes.toString(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength());
			String col = Bytes.toString(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			String val = Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
			map += row + ", " + fam + ":" + col + ", " + val + "<BR/>";
		}
		HTablePoolUtil.putTable(table);
		return map;
	}

	@Override
	public boolean check(Configuration conf, Map<String, String> parameters) {
		if (conf == null
				|| ! parameters.containsKey(TABLE_NAME)
				|| ! parameters.containsKey(ROWKEY)) {
			return false;
		}
		return true;
	}

}
