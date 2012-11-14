package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public abstract class HBaseClinetBase {

	public final String CHECK_ERR_MSG = "[ERROR] parameter error in " + this.getClass().getSimpleName() + "#check.";

	/**
	 * exeute
	 *
	 * @param conf org.apache.hadoop.conf.Configuration
	 * @param parameters Map<String, String>
	 * @return Object
	 * @throws IOException
	 * @throws Throwable
	 */
	public abstract Object execute(Configuration conf, Map<String, String> parameters) throws IOException;

	/**
	 * check parameters
	 *
	 * @param conf org.apache.hadoop.conf.Configuration
	 * @param parameters Map<String, String>
	 * @return boolean
	 */
	public abstract boolean check(Configuration conf, Map<String, String> parameters);
}
