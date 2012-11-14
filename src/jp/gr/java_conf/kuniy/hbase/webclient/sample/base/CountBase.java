package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import static jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.*;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTablePoolUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CountBase extends HBaseClinetBase {

	private final String COPROCESSOR_ERR_MSG = "[ERROR] throwable error in AggregationClient#rowCount.";

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		if (! check(conf, parameters)) {
			return CHECK_ERR_MSG;
		}
		if (parameters.containsKey(COPROCESSOR)) {
			long count = countCoprocessor(conf, parameters);
			return (count != -1) ? count : COPROCESSOR_ERR_MSG;
		} else {
			return countScan(conf, parameters);
		}
	}

	@Override
	public boolean check(Configuration conf, Map<String, String> parameters) {
		if (conf == null
				|| ! parameters.containsKey(TABLE_NAME)
				|| (parameters.containsKey(COPROCESSOR) && ! parameters.containsKey(FAMILY_NAME))) {
			return false;
		}
		return true;
	}

	private long countScan(Configuration conf, Map<String, String> parameters) throws IOException {
		conf.setLong("hbase.client.scanner.caching", 1000);
		HTable table = HTablePoolUtil.getTable(conf, parameters.get(TABLE_NAME));

		Scan scan = new Scan();
		if (parameters.containsKey(START_ROWKEY)) {
			scan.setStartRow(Bytes.toBytes(parameters.get(START_ROWKEY)));
		}
		if (parameters.containsKey(STOP_ROWKEY)) {
			scan.setStopRow(Bytes.toBytes(parameters.get(STOP_ROWKEY)));
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
		scan.addFamily(Bytes.toBytes(parameters.get(FAMILY_NAME)));
		if (parameters.containsKey(START_ROWKEY)) {
			scan.setStartRow(Bytes.toBytes(parameters.get(START_ROWKEY)));
		}
		if (parameters.containsKey(STOP_ROWKEY)) {
			scan.setStopRow(Bytes.toBytes(parameters.get(STOP_ROWKEY)));
		}

		long count = -1;
        try {
			count = client.rowCount(Bytes.toBytes(parameters.get(TABLE_NAME)), null, scan);
		} catch (Throwable e) {
			e.printStackTrace();
		}
        return count;
	}

}
