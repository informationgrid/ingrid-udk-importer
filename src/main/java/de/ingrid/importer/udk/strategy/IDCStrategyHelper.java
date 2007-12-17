/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.Row;

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

			if (dst.matches("[0-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][0-9][0-9][0-9]")) {
				return dst;
			} else {
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
			log.info("Cannot translate country code '" + code + "'");
			return "";
		}
	}

	public static int getPK(DataProvider dataProvider, String entity, String field, String value) {
		Row row = dataProvider.findRow(entity, field, value);
		if (row != null && row.get("primary_key") != null) {
			try {
				return Integer.parseInt(row.get("primary_key"));
			} catch (NumberFormatException e) {
				log.info("Cannot parse primary key '" + row.get("primary_key") + "' for " + entity + "." + field + "='"
						+ value + "' to a number.");
				return 0;
			}
		} else {
			log.info("Cannot not find row for " + entity + "." + field + "='" + value + "'.");
			return 0;
		}
	}

}
