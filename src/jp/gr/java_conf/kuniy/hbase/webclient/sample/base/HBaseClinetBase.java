package jp.gr.java_conf.kuniy.hbase.webclient.sample.base;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public interface HBaseClinetBase {

	public Object execute(Configuration conf, Map parameters) throws IOException;

}
