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

import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.ListBase;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.base.ScanBase;
import jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet.HBaseClientKeyList.ACTIONS;
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
		String action = req.getParameter(HBaseClientKeyList.ACTION);
		Object result = (action != null) ? execute(ACTIONS.valueOf(action), parameters) : "";

		// return response
		resp.setHeader("Access-Control-Allow-Origin","*");
		resp.setContentType(RESPONSE_CONTENT_TYPE);
		PrintWriter out = resp.getWriter();
		out.write(JSONUtil.toJSON(result));
		//out.write(JSONUtil.toJSON(15));
	}

	private Object execute(ACTIONS action, Map<String, String> parameters) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		switch (action) {
		case scan: return new ScanBase().execute(conf, parameters);
		case list: return new ListBase().execute(conf, parameters);
		case count:
		case put:
		case get:
		default:
			break;
		}
		return "";
	}

}
