package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.HTableUtil;

public class ListBase extends HBaseClinetBase {

	@Override
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException {
		return check(conf, parameters) ? HTableUtil.list(conf) : CHECK_ERR_MSG;
	}

	@Override
	public boolean check(Configuration conf, Map<String, String> parameters) {
		return (conf != null);
	}

}
