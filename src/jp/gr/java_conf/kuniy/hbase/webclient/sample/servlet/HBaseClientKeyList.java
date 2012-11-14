package jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet;

public class HBaseClientKeyList {

	public static final String ACTION = "action";

	public static enum ACTIONS {scan, put, get, delete, count, list};

	public static final String TABLE_NAME = "table";

	public static final String FAMILY_NAME = "family";

	public static final String START_ROWKEY = "start";

	public static final String STOP_ROWKEY = "stop";

	public static final String COLUMN_NAME = "column";

	public static final String VALUE = "value";

	public static final String COPROCESSOR = "coprocessor";

}
