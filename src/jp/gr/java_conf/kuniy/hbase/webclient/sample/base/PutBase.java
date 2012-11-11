package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTablePoolUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutBase implements HBaseClinetBase {

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		String tableName = parameters.get(HBaseClientKeyList.TABLE_NAME);
		HTable table = HTablePoolUtil.getTable(conf, tableName);

		String familyName = parameters.get(HBaseClientKeyList.FAMILY_NAME);
		String columnName = parameters.get(HBaseClientKeyList.COLUMN_NAME);
		String value = parameters.get(HBaseClientKeyList.VALUE);

		Put p = new Put(Bytes.toBytes(tableName));
		p.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
		table.put(p);

		return true;
	}

}
