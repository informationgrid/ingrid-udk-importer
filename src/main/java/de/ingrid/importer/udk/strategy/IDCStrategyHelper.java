/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author joachim
 * 
 */
public class IDCStrategyHelper {

	private static Log log = LogFactory.getLog(JDBCConnectionProxy.class);

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	private static String pad = "00000000000000000";

	static {
		sdf.setLenient(false);
	}

	public static String transDateTime(String src) {
		if (src != null && src.length() > 0) {
			String dst = "";
			if (src.length() <= 17) {
				dst = src.concat(pad.substring(src.length()));
			} else {
				dst = src.substring(0, 16);
			}
			try {
				sdf.parse(dst);
				return dst;
			} catch (ParseException e) {
				log.warn("Cannot convert to date '" + src + "'");
			}
		}
		return "";
	}

	public static String transCountryCode(String code) {
		if (code == null) {
			return "";
		}
		if (code.equalsIgnoreCase("D")) {
			return "de";
		} else {
			log.warn("Cannot translate country code '" + code + "'");
			return "";
		}
	}

	public static String getFK(DataProvider dataProvider, String entity, String field, String value) {
		HashMap<String, String> row = dataProvider.findRow(entity, field, value);
		if (row != null && row.get("primary_key") != null) {
			return row.get("primary_key");
		} else {
			log.warn("Cannot not find FK for '" + entity + "." + field + "='" + value + "'.");
			return "null";
		}
	}

}
