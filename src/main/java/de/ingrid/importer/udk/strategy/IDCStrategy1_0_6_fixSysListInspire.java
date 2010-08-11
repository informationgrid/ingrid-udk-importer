/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.utils.udk.UtilsLanguageCodelist;

/**
 * Independent Strategy for fixing wrong INSPIRE syslist VALUES.
 * NOTICE: Also updates according values in object data !
 * see http://dev.wemove.com/jira/browse/INGRID-1860
 */
public class IDCStrategy1_0_6_fixSysListInspire extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_6_fixSysListInspire.class);

	/**
	 * Deliver NO Version, this strategy should NOT trigger a strategy workflow and
	 * can be executed on its own ! NOTICE: BUT may be executed in workflow (part of workflow array) !
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Fixing syslist 5100 values ...");
		fixSysList5100();
		System.out.println("done.");
		System.out.print("  Fixing syslist 5200 values ...");
		fixSysList5200();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void fixSysList5100() throws Exception {
		// fix keys and values in sys_list and t011_obj_geo
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 5100 values in sys_list and t011_obj_serv ...");
		}

		// new syslist 5100 values
		HashMap<Integer, String> mapKeyToNewValueList = new HashMap<Integer, String>();
		mapKeyToNewValueList.put(1, "Suchdienste");
		mapKeyToNewValueList.put(2, "Darstellungsdienste");
		mapKeyToNewValueList.put(3, "Download-Dienste");
		mapKeyToNewValueList.put(4, "Transformationsdienste");
		mapKeyToNewValueList.put(5, "Dienste zum Abrufen von Geodatendiensten");
		mapKeyToNewValueList.put(6, "Sonstige Dienste");

		Iterator<Entry<Integer,String>> entryIt = mapKeyToNewValueList.entrySet().iterator();
		while (entryIt.hasNext()) {
			Entry<Integer,String> entry = entryIt.next();

			// fix sys_list
			int numUpdated = jdbc.executeUpdate(
				"UPDATE sys_list SET " +
					"name = '" + entry.getValue() + "' " +
				"where " +
					"lst_id = 5100" +
					" and lang_id = 'de'" +
					" and entry_id = " + entry.getKey());
			if (log.isDebugEnabled()) {
				log.debug("sys_list 5100: updated " + numUpdated + " rows -> entry_id(" + entry.getKey() + "), " +
					"new name(" +	entry.getValue() + ")");
			}

			// fix data: existing keys with wrong value ! ONLY IF GERMAN !
			if ("de".equals(UtilsLanguageCodelist.getShortcutFromCode(readCatalogLanguageKey()))) {
				numUpdated = jdbc.executeUpdate(
						"UPDATE t011_obj_serv SET " +
							"type_value = '" + entry.getValue() + "' " +
						"where " +
							"type_key = " + entry.getKey());
				if (log.isDebugEnabled()) {
					log.debug("t011_obj_serv: updated " + numUpdated + " rows -> existing type_key(" + entry.getKey() + "), " +
						"new type_value(" +	entry.getValue() + ")");
				}				
			}
		}
	}

	protected void fixSysList5200() throws Exception {
		// fix keys and values in sys_list and t011_obj_geo
		if (log.isInfoEnabled()) {
			log.info("Fixing syslist 5200 values in sys_list and t011_obj_serv_type ...");
		}

		// new syslist 5200 values
		HashMap<Integer, String> mapKeyToNewValueListDE = new HashMap<Integer, String>();
		mapKeyToNewValueListDE.put(101, "Katalogdienst (Viewer)");
		mapKeyToNewValueListDE.put(207, "Katalogdienst (Service)");
		
		HashMap<Integer, String> mapKeyToNewValueListEN = new HashMap<Integer, String>();
		mapKeyToNewValueListEN.put(415, "Feature generalisation service (spatial)");
		mapKeyToNewValueListEN.put(513, "Multiband image manipulation");

		String[] langToProcess = new String[] { "de", "en" };
		for (String lang : langToProcess) {
			Iterator<Entry<Integer,String>> entryIt = null;
			if (lang.equals("de")) {
				entryIt = mapKeyToNewValueListDE.entrySet().iterator();
			} else if (lang.equals("en")) {
				entryIt = mapKeyToNewValueListEN.entrySet().iterator();
			}
			String catLang = UtilsLanguageCodelist.getShortcutFromCode(readCatalogLanguageKey());

			while (entryIt.hasNext()) {
				Entry<Integer,String> entry = entryIt.next();

				// fix sys_list
				int numUpdated = jdbc.executeUpdate(
					"UPDATE sys_list SET " +
						"name = '" + entry.getValue() + "' " +
					"where " +
						"lst_id = 5200" +
						" and lang_id = '" + lang + "'" +
						" and entry_id = " + entry.getKey());
				if (log.isDebugEnabled()) {
					log.debug("sys_list 5100: updated " + numUpdated + " rows -> entry_id(" + entry.getKey() + "), " +
						"new name(" +	entry.getValue() + ")");
				}

				// fix data: existing keys with wrong value ! ONLY IF CATALOGLANGUAGE MAPS !
				if (lang.equals(catLang)) {
					numUpdated = jdbc.executeUpdate(
							"UPDATE t011_obj_serv_type SET " +
								"serv_type_value = '" + entry.getValue() + "' " +
							"where " +
								"serv_type_key = " + entry.getKey());
					if (log.isDebugEnabled()) {
						log.debug("t011_obj_serv_type: updated " + numUpdated + " rows -> existing type_key(" + entry.getKey() + "), " +
							"new type_value(" +	entry.getValue() + ")");
					}				
				}
			}
		}
	}

	private int readCatalogLanguageKey() throws Exception {
		int langKey = -1;
		String sql = "SELECT language_key FROM t03_catalogue";
		try {
			Statement st = jdbc.createStatement();
			ResultSet rs = jdbc.executeQuery(sql, st);
			// has to be there !!!
			rs.next();

			langKey = rs.getInt(1);
			
			rs.close();
			st.close();

		} catch (SQLException e) {
			log.error("Error executing SQL: " + sql, e);
			throw e;
		}

		return langKey;
	}
}
