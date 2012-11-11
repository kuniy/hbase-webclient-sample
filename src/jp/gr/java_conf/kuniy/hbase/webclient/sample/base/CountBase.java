package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTablePoolUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CountBase implements HBaseClinetBase {

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		long l1 = countScan(conf, parameters);
		long l2 = countCoprocessor(conf, parameters);
		return l2;
	}

	private long countScan(Configuration conf, Map<String, String> parameters) throws IOException {
		conf.setLong("hbase.client.scanner.caching", 1000);
		HTable table = HTablePoolUtil.getTable(conf, parameters.get(HBaseClientKeyList.TABLE_NAME));

		Scan scan = new Scan();
		if (parameters.containsKey(HBaseClientKeyList.START_ROWKEY)) {
			scan.setStartRow(Bytes.toBytes(parameters.get(HBaseClientKeyList.START_ROWKEY)));
		}
		if (parameters.containsKey(HBaseClientKeyList.STOP_ROWKEY)) {
			scan.setStopRow(Bytes.toBytes(parameters.get(HBaseClientKeyList.STOP_ROWKEY)));
		}
		scan.setFilter(new KeyOnlyFilter());
		ResultScanner scanner = table.getScanner(scan);

		long count = 0;
		for (Result result : scanner) {
			count++;
		}
		scanner.close();
		HTablePoolUtil.putTable(table);
		return count;
	}

	private long countCoprocessor(Configuration conf, Map<String, String> parameters) {
		conf.setLong("hbase.client.scanner.caching", 1000);
		conf.setLong("hbase.rpc.timeout", 600000);

		AggregationClient client = new AggregationClient(conf);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("stats")); // TODO Coprocessior : require Column Family
		if (parameters.containsKey(HBaseClientKeyList.START_ROWKEY)) {
			scan.setStartRow(Bytes.toBytes(parameters.get(HBaseClientKeyList.START_ROWKEY)));
		}
		if (parameters.containsKey(HBaseClientKeyList.STOP_ROWKEY)) {
			scan.setStopRow(Bytes.toBytes(parameters.get(HBaseClientKeyList.STOP_ROWKEY)));
		}
		long count = -1;
        try {
			count = client.rowCount(Bytes.toBytes(parameters.get(HBaseClientKeyList.TABLE_NAME)), null, scan);
		} catch (Throwable e) {
			e.printStackTrace();
		}
        return count;
	}

}
