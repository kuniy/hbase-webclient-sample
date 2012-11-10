package jp.gr.java_conf.kuniy.hbase.webclient.sample.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

		// access to HBase
		Object result = execute(req.getParameterMap());

		// return response
		resp.setHeader("Access-Control-Allow-Origin","*");
		resp.setContentType(RESPONSE_CONTENT_TYPE);
		PrintWriter out = resp.getWriter();
		//out.write(JSONUtil.toJSON(result));
		out.write(JSONUtil.toJSON(15));
	}

	private Object execute(Map parameters) {

		return null;
	}

}
