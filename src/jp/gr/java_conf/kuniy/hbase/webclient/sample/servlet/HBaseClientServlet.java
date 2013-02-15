package jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.CountBase;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.GetBase;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.ListBase;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.PutBase;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.ScanBase;
import static jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.*;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.ACTIONS;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.useraction.UserActionHTable;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.util.JSONUtil;

/**
 * HBase WebClient Servlet Class
 */
public class HBaseClientServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public static final String REQUEST_ENCODING = "UTF-8";
	public static final String RESPONSE_CONTENT_TYPE = "application/json; charset=UTF-8";
	public static final String COOKIE_SESSIONID = "sessionid";

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)	throws ServletException, IOException {
		doProcess(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)	throws ServletException, IOException {
		doProcess(req, resp);
	}

	/**
	 * Process HTTP Request
	 *
	 * @param req javax.servlet.http.HttpServletRequest
	 * @param resp javax.servlet.http.HttpServletResponse
	 * @throws IOException
	 */
	private void doProcess(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		// process request
		req.setCharacterEncoding(REQUEST_ENCODING);

		// get/set sessionid
		String sessionid = null;
		Cookie[] cookies = req.getCookies();
		if (cookies != null && cookies.length > 0) {
			for (Cookie cookie : cookies) {
				if (cookie.getName().equals(COOKIE_SESSIONID)) {
					sessionid = cookie.getValue();
				}
			}
		}
		if (sessionid == null) {
			sessionid = req.getSession(true).getId();
			Cookie cookie = new Cookie(COOKIE_SESSIONID, sessionid);
			cookie.setMaxAge(60 * 60 * 24 * 1);
			resp.addCookie(cookie);
		}

		// call execute
		Map<String, String> parameters = new HashMap<String, String>();
		Enumeration<String> names = req.getParameterNames();
		while (names.hasMoreElements()) {
			String key = names.nextElement();
			parameters.put(key, req.getParameter(key));
		}
		String action = req.getParameter(ACTION);
		Long startTime = System.nanoTime();
		Object result = (action != null) ? execute(ACTIONS.valueOf(action), parameters) : "[ERROR] ACTION is not set.";
		Long endTime = System.nanoTime();

		// put action log
		new UserActionHTable().put(HBaseConfiguration.create(), sessionid, action, (endTime - startTime), parameters);

		// return response
		resp.setHeader("Access-Control-Allow-Origin","*");
		resp.setContentType(RESPONSE_CONTENT_TYPE);
		PrintWriter out = resp.getWriter();
		out.write(JSONUtil.toJSON(result));
	}

	private Object execute(ACTIONS action, Map<String, String> parameters) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		switch (action) {
			case scan:  return new ScanBase().execute(conf, parameters);
			case list:  return new ListBase().execute(conf, parameters);
			case count: return new CountBase().execute(conf, parameters);
			case put:   return new PutBase().execute(conf, parameters);
			case get:   return new GetBase().execute(conf, parameters);
			default:
		}
		return "";
	}

}
