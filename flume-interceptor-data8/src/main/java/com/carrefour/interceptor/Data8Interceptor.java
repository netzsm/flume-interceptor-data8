package com.carrefour.interceptor;

import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;

public class Data8Interceptor implements Interceptor {

	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Data8Interceptor.class);

	private String logType;

	private String hostName;

	private String store;

	private String regex;

	private String charset;

	private Pattern pattern;
	private SimpleDateFormat s2d = new SimpleDateFormat("yy/MM/dd,hh:mmaa", java.util.Locale.ENGLISH);
	private SimpleDateFormat d2s = new SimpleDateFormat("yyyyMMdd");
	private SimpleDateFormat t2s = new SimpleDateFormat("HH:mm");

	private Data8Interceptor(Context context) {
		logger.info("init data8 interceptor configure");

		String host_name = "";
		String store_code = "";
		try {
			InetAddress inAddr = InetAddress.getLocalHost();
			host_name = inAddr.getHostName();
			store_code = host_name.substring(2, 5);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage());
		}

		this.hostName = context.getString("hostName", host_name);
		logger.info("Hostname: " + this.hostName);

		this.store = context.getString("storeCode", store_code);
		logger.info("Store: " + this.store);

		this.logType = context.getString("logType");
		logger.info("logType: " + this.logType);

		this.charset = context.getString("charset", "UTF-8");
		logger.info("Charset: " + this.charset);

		this.regex = context.getString("regex", "^(\\d+?),(\\d+?),(\\d+?),(\\d+?),.*?,.*?,.*?,.*?,(.*?),(.+?,.+?),");
		this.pattern = Pattern.compile(this.regex);
		logger.info("regex: " + this.regex);
	}

	public void initialize() {
	}

	public void close() {
	}

	public Event intercept(Event event) {
		String line = "";
		try {
			line = new String(event.getBody(), this.charset);
		} catch (Exception e) {
			line = new String(event.getBody());
		}

		return analysisBaseFields(line);
	}

	public List<Event> intercept(List<Event> events) {
		List<Event> batch = new ArrayList<Event>();
		for (Event event : events) {
			batch.add(intercept(event));
		}
		return batch;
	}

	public Event analysisBaseFields(String line) {
		String type = "UNKNOWN";
		String date = "";
		String time = "";
		Date d = new Date();
		Matcher m1 = this.pattern.matcher(line);
		if (m1.find()) {
			try {
				d = this.s2d.parse(m1.group(6));
			} catch (ParseException e) {
				logger.warn(e.getMessage());
			}
			date = this.d2s.format(d);
			time = this.t2s.format(d);

			type = String.format("D_%s_%s", new Object[] { m1.group(4), m1.group(5) });
		}

		HashMap<String, String> headMap = new HashMap<String, String>();
		headMap.put("recType", type);
		headMap.put("recDate", date);
		headMap.put("logType", this.logType);
		headMap.put("stoCode", this.store);
		String body = String.format("%s,%s,%s,%s", new Object[] { date, time, this.store, line });
		Event event = org.apache.flume.event.EventBuilder.withBody(body.getBytes(), headMap);

		return event;
	}

	public static class Builder implements Interceptor.Builder {
		private Context context;

		public void configure(Context context) {
			this.context = context;
			configureSerializers(context);
		}

		private void configureSerializers(Context context) {
		}

		public Interceptor build() {
			return new Data8Interceptor(this.context);
		}
	}

}
