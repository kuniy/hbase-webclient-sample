package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import static jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.*;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTablePoolUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanBase extends HBaseClinetBase {

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		conf.setLong("hbase.client.scanner.caching", 100);
		HTable table = HTablePoolUtil.getTable(conf, parameters.get(TABLE_NAME));

		Scan scan = new Scan();
		if (parameters.containsKey(FAMILY_NAME)) {
			scan.addFamily(Bytes.toBytes(parameters.get(FAMILY_NAME)));
		}
		if (parameters.containsKey(FAMILY_NAME) && parameters.containsKey(COLUMN_NAME)) {
			scan.addColumn(Bytes.toBytes(parameters.get(FAMILY_NAME)), Bytes.toBytes(parameters.get(COLUMN_NAME)));
		}
		if (parameters.containsKey(START_ROWKEY)) {
			scan.setStartRow(Bytes.toBytes(parameters.get(START_ROWKEY)));
		}
		if (parameters.containsKey(STOP_ROWKEY)) {
			scan.setStopRow(Bytes.toBytes(parameters.get(STOP_ROWKEY)));
		}
		ResultScanner scanner = table.getScanner(scan);

		//Map<String, String> map = new TreeMap<String, String>();
		String map = "<BR/>";
		for (Result result : scanner) {
			String row = Bytes.toString(result.getRow());
			for (KeyValue kv : result.raw()) {
				String fam = Bytes.toString(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength());
				String col = Bytes.toString(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
				String val = Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
				//map.put(row + ", " + fam + ":" + col, val);
				map += row + ", " + fam + ":" + col + ", " + val + "<BR/>";
			}
		}
		scanner.close();
		HTablePoolUtil.putTable(table);
		return map;
	}

	@Override
	public boolean check(Configuration conf, Map<String, String> parameters) {
		if (conf == null
				|| ! parameters.containsKey(TABLE_NAME)) {
			return false;
		}
		return true;
	}
}
