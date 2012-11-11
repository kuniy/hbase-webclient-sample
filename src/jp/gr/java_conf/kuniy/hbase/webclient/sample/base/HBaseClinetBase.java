package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public interface HBaseClinetBase {

	/**
	 * exeute
	 *
	 * @param conf org.apache.hadoop.conf.Configuration
	 * @param parameters Map<String, String>
	 * @return Object
	 * @throws IOException
	 * @throws Throwable
	 */
	public Object execute(Configuration conf, Map<String, String> parameters) throws IOException;

}
