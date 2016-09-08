package com.apache.flume.filesource.util;

import com.apache.flume.filesource.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by xuyexin on 16/9/8.
 */
public class TimeUtil {
	private static final Logger logger = LoggerFactory.getLogger(TimeUtil.class);

	public static String getNowTime() {
		return Long.toString(System.currentTimeMillis());
	}

	public static String getTime(String origin) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constants.TIME_FORMAT);
		Date date = null;
		try {
			date = simpleDateFormat.parse(origin);
		} catch (ParseException e) {
			logger.error("transfer time error，origin:" + origin, e);
			return getNowTime();
		}
		long time = date.getTime();
		return Long.toString(time);
	}
}
