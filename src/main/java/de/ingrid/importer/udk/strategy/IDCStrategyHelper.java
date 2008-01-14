/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.Row;

/**
 * @author joachim
 * 
 */
public class IDCStrategyHelper {

	private static Log log = LogFactory.getLog(IDCStrategyHelper.class);

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy");

	private static String pad = "00000000000000000";

	static {
		sdf.setLenient(false);
	}

	public static String transDateTime(String src) {
		if (src != null && src.length() > 0) {
			String dst = "";
			if (src.matches("[0-3][0-9]\\.[0-1][0-9]\\.[0-9][0-9][0-9][0-9]")) {
				df.applyPattern("dd.MM.yyyy");
				try {
					return sdf.format(df.parse(src));
				} catch (ParseException e) {
					log.error("Invalid date format: " + dst, e);
				}
			} else {
				if (src.length() <= 17) {
					dst = src.concat(pad.substring(src.length()));
				} else {
					dst = src.substring(0, 16);
				}

				if (dst
						.matches("[0-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][0-9][0-9][0-9]")) {
					return dst;
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Cannot convert to date '" + src + "'");
					}
				}
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
			if (log.isDebugEnabled()) {
				log.debug("Cannot translate country code '" + code + "'");
			}
			return "";
		}
	}

	public static int getPK(DataProvider dataProvider, String entity, String field, String value) {
		Row row = dataProvider.findRow(entity, field, value);
		if (row != null && row.get("primary_key") != null) {
			try {
				return Integer.parseInt(row.get("primary_key"));
			} catch (NumberFormatException e) {
				return 0;
			}
		} else {
			return 0;
		}
	}

	public static int getPK(DataProvider dataProvider, String entity, String[] fields, String[] values) {
		Row row = dataProvider.findRow(entity, fields, values);
		if (row != null && row.get("primary_key") != null) {
			try {
				return Integer.parseInt(row.get("primary_key"));
			} catch (NumberFormatException e) {
				return 0;
			}
		} else {
			return 0;
		}
	}

	public static String getEntityFieldValue(DataProvider dataProvider, String entity, String fieldWhere,
			String valueWhere, String field) {
		Row row = dataProvider.findRow(entity, fieldWhere, valueWhere);
		if (row != null && row.get(field) != null) {
			if (row.containsKey(field)) {
				return row.get(field);
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Cannot not find key '" + field + "' in row for " + entity + "." + fieldWhere + "='"
							+ valueWhere + "'.");
				}
				return "";
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Cannot not find row for " + entity + "." + fieldWhere + "='" + valueWhere + "'.");
			}
			return "";
		}
	}

	public static double getEntityFieldValueAsDouble(DataProvider dataProvider, String entity, String fieldWhere,
			String valueWhere, String field) {
		Row row = dataProvider.findRow(entity, fieldWhere, valueWhere);
		if (row != null && row.get(field) != null) {
			if (row.containsKey(field)) {
				try {
					return Double.parseDouble(row.get(field));
				} catch (NumberFormatException e) {
					if (log.isDebugEnabled()) {
						log.debug("Cannot convert to double: " + row.get(field));
					}
					return 0;
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Cannot not find key '" + field + "' in row for " + entity + "." + fieldWhere + "='"
							+ valueWhere + "'.");
				}
				return 0;
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Cannot not find row for " + entity + "." + fieldWhere + "='" + valueWhere + "'.");
			}
			return 0;
		}
	}

	public static String getEntityFieldValueStartsWith(DataProvider dataProvider, String entity, String fieldWhere,
			String valueWhere, String field) {
		Row row = dataProvider.findRowStartsWith(entity, fieldWhere, valueWhere);
		if (row != null && row.get(field) != null) {
			if (row.containsKey(field)) {
				return row.get(field);
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Cannot not find key '" + field + "' in row for " + entity + "." + fieldWhere + "='"
							+ valueWhere + "*'.");
				}
				return "";
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Cannot not find row for " + entity + "." + fieldWhere + "='" + valueWhere + "*'.");
			}
			return "";
		}
	}

    public static String transformNativeKey2TopicId(String nativeKey) {
        if (nativeKey == null) {
        	return "";
        } else if (nativeKey.length() == 8) {
        	return "GEMEINDE" + nativeKey.substring(0, 5) + "00" + nativeKey.substring(5); 
        } else if (nativeKey.length() == 5) {
        	return "KREIS" + nativeKey + "00000";
        } else if (nativeKey.length() == 2) {
        	return "BUNDESLAND" + nativeKey;
        } else {
        	log.error("Invalid nativekey: " + nativeKey);
        	return "";
        }
    }

}
