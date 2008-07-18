/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;

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
	
	private static HashMap<String, String> addressTypeLookup = new HashMap<String, String>();
	private static HashMap<Integer, String> udkAddressTypes = new HashMap<Integer, String>();

	static {
		sdf.setLenient(false);
		// lookup table for translating udk address type to idc address type
		// see http://wiki.media-style.com/display/INGRIDII/UDK-Migration
		/*
		 * 		# Auskunft (0) --> Auskunft (7)
		 * 		# Datenhalter (1) --> Halter (3)
				# Datenverantwortlicher (2) --> Datenverantwortung (2)
				# Anbieter (3) --> Anbieter (1)
				# Benutzer(4) --> Benutzer(4)
				# Vertrieb (5) --> Vertrieb (5)
				# Herkunft (6) --> Herkunft (6)
				# Datenerfassung (7) --> Datenerfassung (8)
				# Auswertung (8) --> Auswertung (9)
				# Herausgeber (9) --> Herausgeber (10) 		

		 */
		addressTypeLookup.put("0", "7");
		addressTypeLookup.put("1", "3");
		addressTypeLookup.put("2", "2");
		addressTypeLookup.put("3", "1");
		addressTypeLookup.put("4", "4");
		addressTypeLookup.put("5", "5");
		addressTypeLookup.put("6", "6");
		addressTypeLookup.put("7", "8");
		addressTypeLookup.put("8", "9");
		addressTypeLookup.put("9", "10");
		
		udkAddressTypes.put(new Integer(0), "auskunft");
		udkAddressTypes.put(new Integer(1), "datenhalter");
		udkAddressTypes.put(new Integer(2), "datenverantwortlicher");
		udkAddressTypes.put(new Integer(3), "anbieter");
		udkAddressTypes.put(new Integer(4), "benutzer");
		udkAddressTypes.put(new Integer(5), "vertrieb");
		udkAddressTypes.put(new Integer(6), "herkunft");
		udkAddressTypes.put(new Integer(7), "datenerfassung");
		udkAddressTypes.put(new Integer(8), "auswertung");
		udkAddressTypes.put(new Integer(9), "herausgeber");
	}

	/**
	 * Transforms UDK address type into IDC (ISO) address type.
	 * 
	 * @param type
	 * @return
	 */
	public static Integer transAddressTypeUdk2Idc(Integer type) {
		if (type == null) {
			return null;
		}
		String idcType = addressTypeLookup.get(type.toString());
		if (idcType != null) {
			return Integer.parseInt(idcType);
		} else {
			log.error("Unknown UDK address type: " + type);
			return null;
		}
	}
	
	/**
	 *  Check if the UDK address type is valid. Basically checks if the type corresponds with the name of the type (case insensitive).
	 *  
	 *  Caution: returns also true, if type is no valid UDK address type (valid means: 0 <= value <= 9 || value == 999).
	 *  
	 *  If type == 999 the name will be ignored.
	 * 
	 * @param type The UDK type.
	 * @param name The corresponding UDK type name (German).
	 * @return True if type and name correspond OR if type is no valid UDK address type. False if the values do not correspond.
	 */
	public static boolean isValidUdkAddressType(Integer type, String name) {
		if (type == null || !(type.intValue() >= 0 && type.intValue() <= 9) || name==null || name.length() == 0) {
			return true;
		}
		return udkAddressTypes.get(type).equals(name.toLowerCase());
	}
	
	
	public static String transDateTime(Date date) {
		return sdf.format(date);
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
		return null;
	}

	public static void processObjectTimeReference(String objUuid, HashMap<String, String> timeMap) {
		String timeFrom = IDCStrategyHelper.transDateTime(timeMap.get("time_from"));
		String timeTo = IDCStrategyHelper.transDateTime(timeMap.get("time_to"));
		String timeType = timeMap.get("time_type");

		// default: same values !
		String processedTimeFrom = timeFrom;
		String processedTimeTo = timeTo;
		String processedTimeType = timeType;

		boolean timeValid = false;

		if (timeType == null || timeType.trim().length() == 0) {
			// no time type set, check whether time dates are set, then error
			boolean timeFromSet = (timeFrom != null && timeFrom.trim().length() > 0);
			boolean timeToSet = (timeTo != null && timeTo.trim().length() > 0);
			
			if (!timeFromSet && !timeToSet) {
				// ok: no time data set, we clear all time data to assure no wrong values !
				timeValid = true;
				processedTimeFrom = processedTimeTo = processedTimeType = null;
			}

		} else {
			// process time data according to time type 
			// ----------------------------------------------------------------
			if (timeType.equals("am")) {
				if (timeFrom != null && timeFrom.equals(timeTo)) {
					processedTimeFrom = processTimeFrom(objUuid, timeFrom);
					processedTimeTo = processTimeTo(objUuid, timeTo);
					if (processedTimeFrom != null && 
							processedTimeTo != null &&
							processedTimeFrom.compareTo(processedTimeTo) <= 0)
					{
						timeValid = true;
						if (processedTimeFrom.compareTo(processedTimeTo) < 0) {
							processedTimeType = "von";							
						}
					}
				}
			// ----------------------------------------------------------------
			} else if (timeType.equals("seit")) {
				if (timeFrom != null) {
					processedTimeFrom = processTimeFrom(objUuid, timeFrom);
					if (processedTimeFrom != null) {						
						timeValid = true;
						// we set time_to to NULL, may have wrong data !
						processedTimeTo = null;
					}
				}
				
			// ----------------------------------------------------------------
			} else if (timeType.equals("von")) {
				if (timeFrom != null && timeTo != null) {
					processedTimeFrom = processTimeFrom(objUuid, timeFrom);
					processedTimeTo = processTimeTo(objUuid, timeTo);
					if (processedTimeFrom != null &&
						processedTimeTo != null &&
						processedTimeFrom.compareTo(processedTimeTo) <= 0)
					{
						timeValid = true;
					}
				}
			}
		}
		
		// set return data in map dependent from process result !

		if (timeValid) {
			timeMap.put("time_from", processedTimeFrom);
			timeMap.put("time_to", processedTimeTo);
			timeMap.put("time_type", processedTimeType);			

		} else {
			// invalid time data !
			if (log.isDebugEnabled()) {
				log.debug("Wrong time_from('" + timeFrom +	"'), time_to('" + timeTo +
					"'), time_type('" + timeType + "') in obj('" + objUuid +
					"'), we reset time data in object !");
			}

			// clear all if time data not valid !
			timeMap.put("time_from", null);
			timeMap.put("time_to", null);
			timeMap.put("time_type", null);
		}
	}

	/** Decrease timeFrom to "lowest" DAY if passed timeFrom NOT day specific (e.g. only year set, then will be 01.01. of that year) */
	private static String processTimeFrom(String objUuid, String timeFrom) {
		String processedTime = null;
		
		try {
			StringBuilder tmpTime = new StringBuilder(timeFrom);
			// set lowest month if not set
			if (tmpTime.substring(4, 6).equals("00")) {
				tmpTime.replace(4, 6, "01");
			}
			// set lowest day in month if not set
			if (tmpTime.substring(6, 8).equals("00")) {
				tmpTime.replace(6, 8, "01");
			}
			processedTime = tmpTime.toString();
			
		} catch (Exception exc) {
			if (log.isDebugEnabled()) {
				log.debug("Problems processing time_from('" + timeFrom + "') in obj('" + objUuid + "') !");
			}
		}

		return processedTime;
	}

	/** Increase timeTo to "highest" DAY if passed timeTo NOT day specific (e.g. only year set, then will be 31.12. of that year) */
	private static String processTimeTo(String objUuid, String timeTo) {
		String processedTime = null;
		
		try {
			StringBuilder tmpTime = new StringBuilder(timeTo);
			// set highest month if not set
			if (tmpTime.substring(4, 6).equals("00")) {
				tmpTime.replace(4, 6, "12");
			}
			// set highest day in month if not set
			if (tmpTime.substring(6, 8).equals("00")) {
				// Determining the Number of Days in the specified Month
				int year = Integer.parseInt(tmpTime.substring(0, 4));
				int month = Integer.parseInt(tmpTime.substring(4, 6));
			    Calendar cal = new GregorianCalendar(year, month-1, 1);
			    int days = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
			    
				tmpTime.replace(6, 8, String.valueOf(days));
			}
			processedTime = tmpTime.toString();
			
		} catch (Exception exc) {
			if (log.isDebugEnabled()) {
				log.debug("Problems processing time_to('" + timeTo + "') in obj('" + objUuid + "') !");
			}
		}

		return processedTime;
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

	public static String transLanguageCode(String code) {
		if (code == null) {
			return "";
		}
		if (code.equalsIgnoreCase("121") || code.equalsIgnoreCase("de")) {
			return "de";
		} else if (code.equalsIgnoreCase("94") || code.equalsIgnoreCase("en")) {
				return "en";
		} else {
			if (log.isErrorEnabled()) {
				log.error("Cannot translate country code '" + code + "'");
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
	
	public static Integer getEntityFieldValueAsInteger(DataProvider dataProvider, String entity, String fieldWhere,
	String valueWhere, String field) {
		String value = getEntityFieldValue(dataProvider, entity, fieldWhere,
				valueWhere, field);
		try {
			return Integer.getInteger(value);
		} catch (NumberFormatException e) {
			if (log.isDebugEnabled()) {
				log.debug("Cannot convert to Integer: " + value);
			}
			return null;
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

    public static String transformNativeKey2FullAgs(String nativeKey) {
        if (nativeKey == null) {
        	return "";
        } else if (nativeKey.length() == 8) {
        	return nativeKey; 
        } else if (nativeKey.length() == 5) {
        	return nativeKey + "000";
        } else if (nativeKey.length() == 3) {
        	return nativeKey + "00000";
        } else if (nativeKey.length() == 2) {
        	return nativeKey + "000000";
        } else {
        	log.error("Invalid nativekey: " + nativeKey);
        	return "";
        }
    }
    
    public static String getLocationNameFromNativeAGS(String nativeAGSKey, DataProvider dataProvider) {
    	String locName = null;
		if (nativeAGSKey != null) {
			if (nativeAGSKey.length() == 2) {
				locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
						"loc_town_no", nativeAGSKey, "state");
			} else if (nativeAGSKey.length() == 3) {
				locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
						"loc_town_no", nativeAGSKey, "district").concat(" (District)");
			} else if (nativeAGSKey.length() == 5) {
				locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
						"loc_town_no", nativeAGSKey, "country");
			} else if (nativeAGSKey.length() == 8) {
				locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
						"loc_town_no", nativeAGSKey, "township");
			}
		}

		if (locName == null) {
        	log.error("Invalid AGS nativekey: " + nativeAGSKey);
		}

		return locName;
    }
}
