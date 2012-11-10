package jp.gr.java_conf.kuniy.hbase.webclient.sample.util;

import com.google.gson.Gson;

public class JSONUtil {

	/**
	 * Object -> JSON
	 *
	 * @param result Object
	 * @return JSON String
	 */
	public static String toJSON(Object result) {
		Gson gson = new Gson();
		return gson.toJson(result);
	}

}
