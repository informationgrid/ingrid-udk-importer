/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * IGC Update: Post InGrid 2.0 release
 */
public class IDCStrategy1_0_5 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_5.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_105;

	private static final String OLD_COUNTRY_CODE_GERMANY = "de";
	private static final String OLD_COUNTRY_ZIP_CODE_GERMANY = "D";

	private static final Integer NEW_COUNTRY_KEY_GERMANY = 276;
	private static final String NEW_COUNTRY_VALUE_GERMANY_DE = "Deutschland";

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// then write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Updating sys_list...");
		updateSysList();
		System.out.println("done.");

		System.out.print("  Updating t03_catalogue...");
		updateT03Catalogue();
		System.out.println("done.");

		System.out.print("  Updating t02_address...");
		updateT02Address();
		System.out.println("done.");


		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add columns 'country_key', 'country_value' to table 't03_catalogue' ...");
		}
		jdbc.getDBLogic().addColumn("country_key", ColumnType.INTEGER, "t03_catalogue", false, null, jdbc);
		jdbc.getDBLogic().addColumn("country_value", ColumnType.VARCHAR255, "t03_catalogue", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add columns 'country_key', 'country_value' to table 't02_address' ...");
		}
		jdbc.getDBLogic().addColumn("country_key", ColumnType.INTEGER, "t02_address", false, null, jdbc);
		jdbc.getDBLogic().addColumn("country_value", ColumnType.VARCHAR255, "t02_address", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		int lstId = 6200;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" Country ...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		LinkedHashMap<Integer, String> newSyslistCountry_de = new LinkedHashMap<Integer, String>();
		newSyslistCountry_de.put(new Integer("008"), "Albanien");
		newSyslistCountry_de.put(new Integer("020"), "Andorra");
		newSyslistCountry_de.put(new Integer("040"), "Österreich");
		newSyslistCountry_de.put(new Integer("112"), "Weißrussland");
		newSyslistCountry_de.put(new Integer("056"), "Belgien");
		newSyslistCountry_de.put(new Integer("070"), "Bosnien und Herzegowina");
		newSyslistCountry_de.put(new Integer("100"), "Bulgarien");
		newSyslistCountry_de.put(new Integer("191"), "Kroatien");
		newSyslistCountry_de.put(new Integer("196"), "Zypern");
		newSyslistCountry_de.put(new Integer("203"), "Tschechien");
		newSyslistCountry_de.put(new Integer("208"), "Dänemark");
		newSyslistCountry_de.put(new Integer("233"), "Estland");
		newSyslistCountry_de.put(new Integer("246"), "Finnland");
		newSyslistCountry_de.put(new Integer("250"), "Frankreich");
		newSyslistCountry_de.put(NEW_COUNTRY_KEY_GERMANY, NEW_COUNTRY_VALUE_GERMANY_DE);
		newSyslistCountry_de.put(new Integer("292"), "Gibraltar");
		newSyslistCountry_de.put(new Integer("300"), "Griechenland");
		newSyslistCountry_de.put(new Integer("348"), "Ungarn");
		newSyslistCountry_de.put(new Integer("352"), "Island");
		newSyslistCountry_de.put(new Integer("372"), "Irland");
		newSyslistCountry_de.put(new Integer("380"), "Italien");
		newSyslistCountry_de.put(new Integer("428"), "Lettland");
		newSyslistCountry_de.put(new Integer("438"), "Liechtenstein");
		newSyslistCountry_de.put(new Integer("440"), "Litauen");
		newSyslistCountry_de.put(new Integer("442"), "Luxemburg");
		newSyslistCountry_de.put(new Integer("807"), "Mazedonien");
		newSyslistCountry_de.put(new Integer("450"), "Madagaskar");
		newSyslistCountry_de.put(new Integer("470"), "Malta");
		newSyslistCountry_de.put(new Integer("498"), "Moldawien");
		newSyslistCountry_de.put(new Integer("492"), "Monaco");
		newSyslistCountry_de.put(new Integer("499"), "Montenegro");
		newSyslistCountry_de.put(new Integer("528"), "Niederlande");
		newSyslistCountry_de.put(new Integer("578"), "Norwegen");
		newSyslistCountry_de.put(new Integer("616"), "Polen");
		newSyslistCountry_de.put(new Integer("620"), "Portugal");
		newSyslistCountry_de.put(new Integer("642"), "Rumänien");
		newSyslistCountry_de.put(new Integer("643"), "Russland");
		newSyslistCountry_de.put(new Integer("688"), "Serbien");
		newSyslistCountry_de.put(new Integer("703"), "Slowakei");
		newSyslistCountry_de.put(new Integer("705"), "Slowenien");
		newSyslistCountry_de.put(new Integer("724"), "Spanien");
		newSyslistCountry_de.put(new Integer("752"), "Schweden");
		newSyslistCountry_de.put(new Integer("756"), "Schweiz");
		newSyslistCountry_de.put(new Integer("792"), "Türkei");
		newSyslistCountry_de.put(new Integer("804"), "Ukraine");
		newSyslistCountry_de.put(new Integer("826"), "Vereinigtes Königreich");
		// english syslist
		LinkedHashMap<Integer, String> newSyslistCountry_en = new LinkedHashMap<Integer, String>(); 
		newSyslistCountry_en.put(new Integer("008"), "Albania");
		newSyslistCountry_en.put(new Integer("020"), "Andorra");
		newSyslistCountry_en.put(new Integer("040"), "Austria");
		newSyslistCountry_en.put(new Integer("112"), "Belarus");
		newSyslistCountry_en.put(new Integer("056"), "Belgium");
		newSyslistCountry_en.put(new Integer("070"), "Bosnia and Herzegovina");
		newSyslistCountry_en.put(new Integer("100"), "Bulgaria");
		newSyslistCountry_en.put(new Integer("191"), "Croatia");
		newSyslistCountry_en.put(new Integer("196"), "Cyprus");
		newSyslistCountry_en.put(new Integer("203"), "Czech Republic");
		newSyslistCountry_en.put(new Integer("208"), "Denmark");
		newSyslistCountry_en.put(new Integer("233"), "Estonia");
		newSyslistCountry_en.put(new Integer("246"), "Finland");
		newSyslistCountry_en.put(new Integer("250"), "France");
		newSyslistCountry_en.put(NEW_COUNTRY_KEY_GERMANY, "Germany");
		newSyslistCountry_en.put(new Integer("292"), "Gibraltar");
		newSyslistCountry_en.put(new Integer("300"), "Greece");
		newSyslistCountry_en.put(new Integer("348"), "Hungary");
		newSyslistCountry_en.put(new Integer("352"), "Iceland");
		newSyslistCountry_en.put(new Integer("372"), "Ireland");
		newSyslistCountry_en.put(new Integer("380"), "Italy");
		newSyslistCountry_en.put(new Integer("428"), "Latvia");
		newSyslistCountry_en.put(new Integer("438"), "Liechtenstein");
		newSyslistCountry_en.put(new Integer("440"), "Lithuania");
		newSyslistCountry_en.put(new Integer("442"), "Luxembourg");
		newSyslistCountry_en.put(new Integer("807"), "Macedonia");
		newSyslistCountry_en.put(new Integer("450"), "Madagascar");
		newSyslistCountry_en.put(new Integer("470"), "Malta");
		newSyslistCountry_en.put(new Integer("498"), "Moldova, Republic of");
		newSyslistCountry_en.put(new Integer("492"), "Monaco");
		newSyslistCountry_en.put(new Integer("499"), "Montenegro");
		newSyslistCountry_en.put(new Integer("528"), "Netherlands");
		newSyslistCountry_en.put(new Integer("578"), "Norway");
		newSyslistCountry_en.put(new Integer("616"), "Poland");
		newSyslistCountry_en.put(new Integer("620"), "Portugal");
		newSyslistCountry_en.put(new Integer("642"), "Romania");
		newSyslistCountry_en.put(new Integer("643"), "Russian Federation");
		newSyslistCountry_en.put(new Integer("688"), "Serbia");
		newSyslistCountry_en.put(new Integer("703"), "Slovakia");
		newSyslistCountry_en.put(new Integer("705"), "Slovenia");
		newSyslistCountry_en.put(new Integer("724"), "Spain");
		newSyslistCountry_en.put(new Integer("752"), "Sweden");
		newSyslistCountry_en.put(new Integer("756"), "Switzerland");
		newSyslistCountry_en.put(new Integer("792"), "Turkey");
		newSyslistCountry_en.put(new Integer("804"), "Ukraine");
		newSyslistCountry_en.put(new Integer("826"), "United Kingdom");

		Iterator<Integer> itr = newSyslistCountry_de.keySet().iterator();
		while (itr.hasNext()) {
			Integer key = itr.next();
			// german version
			String isDefault = (key.equals(NEW_COUNTRY_KEY_GERMANY)) ? "'Y'" : "'N'";
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslistCountry_de.get(key) + "', 0, " + isDefault + ")");
			// english version
			isDefault = (key == 826) ? "'Y'" : "'N'";
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslistCountry_en.get(key) + "', 0, " + isDefault + ")");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}

	protected void updateT03Catalogue() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t03_catalogue...");
		}

		if (log.isInfoEnabled()) {
			log.info("Map old 'country_code' to new 'country_key'/'country_value'...");
		}

		// we always set "germany". All catalogs were created with "de" ! Can be edited via IGE. 
		int numUpdated = jdbc.executeUpdate("UPDATE t03_catalogue SET " +
			"country_key = " + NEW_COUNTRY_KEY_GERMANY +
			", country_value = '" + NEW_COUNTRY_VALUE_GERMANY_DE + "'");

		if (log.isInfoEnabled()) {
			log.info("Updated " + numUpdated + " catalog(s) to '" +
					NEW_COUNTRY_KEY_GERMANY + "'/'" + NEW_COUNTRY_VALUE_GERMANY_DE + "'");
			log.info("Updating t03_catalogue... done");
		}
	}

	protected void updateT02Address() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t02_address...");
		}

		if (log.isInfoEnabled()) {
			log.info("Map old 'country_code' to new 'country_key'/'country_value'...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select distinct addrNode.id as addrNodeId, " +
			"addr.id as addrId, addr.country_code, addr.postcode, addr.postbox_pc, addr.adr_uuid " +
			"from t02_address addr, address_node addrNode " +
			"where addr.adr_uuid = addrNode.addr_uuid";

		ResultSet rs = jdbc.executeQuery(sql);
		Set<Long> processedNodeIds = new HashSet<Long>();
		int numProcessed = 0;
		while (rs.next()) {
			String countryCode = rs.getString("country_code");
			if (!OLD_COUNTRY_CODE_GERMANY.equals(countryCode)) {
				continue;
			}

			long addrNodeId = rs.getLong("addrNodeId");
			long addrId = rs.getLong("addrId");
			String addrUuid = rs.getString("adr_uuid");
			String postcode = rs.getString("postcode");
			String postboxPostcode = rs.getString("postbox_pc");

			String newPostcode = null;
			String newPostboxPostcode = null;
			String sqlUpdatePlz = "";
			if (postcode != null && postcode.trim().length() > 0) {
				newPostcode = OLD_COUNTRY_ZIP_CODE_GERMANY + "-" + postcode;
				sqlUpdatePlz = "postcode = '" + newPostcode + "', ";
			}
			if (postboxPostcode != null && postboxPostcode.trim().length() > 0) {
				newPostboxPostcode = OLD_COUNTRY_ZIP_CODE_GERMANY + "-" + postboxPostcode;
				sqlUpdatePlz += "postbox_pc = '" + newPostboxPostcode + "', ";
			}

			jdbc.executeUpdate("UPDATE t02_address SET " +
				sqlUpdatePlz +
				"country_key = " + NEW_COUNTRY_KEY_GERMANY +
				", country_value = '" + NEW_COUNTRY_VALUE_GERMANY_DE + "'" +
				" WHERE id = " + addrId);
			
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.contains(addrNodeId)) {
				JDBCHelper.updateAddressIndex(addrNodeId, NEW_COUNTRY_KEY_GERMANY.toString(), jdbc);
				JDBCHelper.updateAddressIndex(addrNodeId, NEW_COUNTRY_VALUE_GERMANY_DE, jdbc);
				if (newPostcode != null) {
					JDBCHelper.updateAddressIndex(addrNodeId, newPostcode, jdbc);					
				}
				if (newPostboxPostcode != null) {
					JDBCHelper.updateAddressIndex(addrNodeId, newPostboxPostcode, jdbc);					
				}
				
				processedNodeIds.add(addrNodeId);
			}

			numProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Address " + addrUuid + " updated from '" + countryCode +	"' to '" +
					NEW_COUNTRY_KEY_GERMANY + "'/'" + NEW_COUNTRY_VALUE_GERMANY_DE + "'" +
					((newPostcode != null) ? (", postcode '" + newPostcode + "'") : "") +
					((newPostboxPostcode != null) ? (", postbox_pc '" + newPostboxPostcode + "'") : ""));
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " addresses... done");
			log.info("Updating t02_address... done");
		}
	}

	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop column 'country_code' from tables 't03_catalogue', 't02_address' ...");
		}
		jdbc.getDBLogic().dropColumn("country_code", "t03_catalogue", jdbc);
		jdbc.getDBLogic().dropColumn("country_code", "t02_address", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
