/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 2.3.0 = SWITCH OF STRATEGY VERSION ! First two digits correlate now with InGrid project version !
 * <p>
 * Changes InGrid 2.3:<p>
 * - Extend Schema for new DQ Elements, see Fachkonzept:
 * "Steuerung der Klasse Geoinformation/Karte anhand des INSPIRE-Themas"<ul>
 *   <li> new table for data quality elements
 *   <li> new syslists for "name of measure"
 * </ul>
 */
public class IDCStrategy2_3_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_2_3_0;

	int SYSLIST_ENTRY_ID_NO_INSPIRE_THEME = 99999;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Updating sys_list (existing ones)...");
		updateSysList();
		System.out.println("done.");

		System.out.print("  Extending sys_list (new ones)...");
		extendSysList();
		System.out.println("done.");

		System.out.print("  Updating sys_gui...");
		updateSysGui();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------
/*
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");
*/
		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Create table 'object_data_quality'...");
		}
		jdbc.getDBLogic().createTableObjectDataQuality(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Manipulate datastructure... done");
		}
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

// ---------------------------
		int lstId = 6100;
		if (log.isInfoEnabled()) {
			log.info("Changing syslist " + lstId +	"(INSPIRE themes): change order of items...");
		}

		String sql = "SELECT id, entry_id, name " +
			"FROM sys_list " +
			"WHERE lst_id = " + lstId +
			" ORDER BY name";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int lineValue = 10;
		while (rs.next()) {
			long id = rs.getLong("id");
			int entryId = rs.getInt("entry_id");

			int newLineValue = lineValue;
			if (entryId == SYSLIST_ENTRY_ID_NO_INSPIRE_THEME) {
				// lowest line value to be first entry
				newLineValue = 0;
			}

			jdbc.executeUpdate("UPDATE sys_list " +
				"SET line = " + newLineValue + " WHERE id = " + id);
			
			lineValue += 10;
		}
		rs.close();
		st.close();
// ---------------------------

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}

	protected void extendSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending sys_list...");
		}

// ---------------------------
		int lstId = 7109;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 109. DQ_CompletenessComission)...");
		}

		// german syslist
		LinkedHashMap<Integer, String> newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Rate of excess items");
		newSyslistMap_de.put(2, "Number of duplicate feature instances");
		// english syslist
		LinkedHashMap<Integer, String> newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Rate of excess items");
		newSyslistMap_en.put(2, "Number of duplicate feature instances");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, -1);
// ---------------------------
		lstId = 7110;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 110. DQ_CompletenessOmission)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Rate of missing items ");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Rate of missing items ");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------
		lstId = 7112;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 112. DQ_ConceptualConsistency)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Number of invalid overlaps of surfaces");
		newSyslistMap_de.put(2, "Conceptual Schema compliance");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Number of invalid overlaps of surfaces");
		newSyslistMap_en.put(2, "Conceptual Schema compliance");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, -1);
// ---------------------------
		lstId = 7113;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 113. DQ_DomainConsistency)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Value domain conformance rate");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Value domain conformance rate");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------
		lstId = 7114;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 114. DQ_FormatConsistency)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Physical structure conflict rate");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Physical structure conflict rate");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------
		lstId = 7115;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 115. DQ_TopologicalConsistency)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Number of invalid overlaps of surfaces");
		newSyslistMap_de.put(2, "Number of missing connections due to undershoots");
		newSyslistMap_de.put(3, "Number of missing connections due to overshoots");
		newSyslistMap_de.put(4, "Number of invalid slivers");
		newSyslistMap_de.put(5, "Number of invalid self-intersect errors");
		newSyslistMap_de.put(6, "Number of invalid self-overlap errors");
		newSyslistMap_de.put(7, "Number of faulty point-curve connections");
		newSyslistMap_de.put(8, "Number of missing connections due to crossing of bridge/road");
		newSyslistMap_de.put(9, "Number of watercourse links below threshold length");
		newSyslistMap_de.put(10, "Number of closed watercourse links");
		newSyslistMap_de.put(11, "Number of multi-part watercourse links");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Number of invalid overlaps of surfaces");
		newSyslistMap_en.put(2, "Number of missing connections due to undershoots");
		newSyslistMap_en.put(3, "Number of missing connections due to overshoots");
		newSyslistMap_en.put(4, "Number of invalid slivers");
		newSyslistMap_en.put(5, "Number of invalid self-intersect errors");
		newSyslistMap_en.put(6, "Number of invalid self-overlap errors");
		newSyslistMap_en.put(7, "Number of faulty point-curve connections");
		newSyslistMap_en.put(8, "Number of missing connections due to crossing of bridge/road");
		newSyslistMap_en.put(9, "Number of watercourse links below threshold length");
		newSyslistMap_en.put(10, "Number of closed watercourse links");
		newSyslistMap_en.put(11, "Number of multi-part watercourse links");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, -1);
// ---------------------------
		lstId = 7117;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 117. DQ_AbsoluteExternalPositionalAccuracy)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Mean value of positional uncertainties (1D)");
		newSyslistMap_de.put(2, "Mean value of positional uncertainties (2D)");
		newSyslistMap_de.put(3, "mean value of positional uncertainties (3D)");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Mean value of positional uncertainties (1D)");
		newSyslistMap_en.put(2, "Mean value of positional uncertainties (2D)");
		newSyslistMap_en.put(3, "mean value of positional uncertainties (3D)");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, -1);
// ---------------------------
		lstId = 7120;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 120. DQ_TemporalConsistency)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Percentage of items that are correctly events ordered");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Percentage of items that are correctly events ordered");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------
		lstId = 7125;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 125. DQ_ThematicClassificationCorrectness)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Misclassification rate");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Misclassification rate");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------
		lstId = 7126;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 126. DQ_NonQuantitativeAttributeAccuracy)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Rate of incorrect attributes names values");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Rate of incorrect attributes names values");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------
		lstId = 7127;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (\"Name of measure\" for 127. DQ_QuantitativeAttributeAccuracy)...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Attribute value uncertainty at 95 % significance level");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Attribute value uncertainty at 95 % significance level");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------

		if (log.isInfoEnabled()) {
			log.info("Extending sys_list... done");
		}
	}

	/** Add gui ids of all NEW OPTIONAL fields to sys_gui table ! (mandatory fields not added, behavior not changeable) */
	protected void updateSysGui() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add ids of new DataQuality Tables (OPTIONAL by default) !...");
		}

		LinkedHashMap<String, Integer> newSysGuis = new LinkedHashMap<String, Integer>();
		Integer initialBehaviour = -1; // default behaviour, optional field only shown if section expanded ! 
//		Integer remove = 0; // do NOT show if section reduced (use case for optional fields ? never used) !
//		Integer mandatory = 1; // do also show if section reduced (even if field optional) !

		// add  new sysgui Ids (dq tables)
		newSysGuis.put("7509", initialBehaviour);
		newSysGuis.put("7510", initialBehaviour);
		newSysGuis.put("7512", initialBehaviour);
		newSysGuis.put("7513", initialBehaviour);
		newSysGuis.put("7514", initialBehaviour);
		newSysGuis.put("7515", initialBehaviour);
		newSysGuis.put("7517", initialBehaviour);
		newSysGuis.put("7520", initialBehaviour);
		newSysGuis.put("7525", initialBehaviour);
		newSysGuis.put("7526", initialBehaviour);
		newSysGuis.put("7527", initialBehaviour);
		
		Iterator<String> itr = newSysGuis.keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			jdbc.executeUpdate("INSERT INTO sys_gui (id, gui_id, behaviour) VALUES ("
					+ getNextId() + ", '" + key + "', " + newSysGuis.get(key) + ")");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui... done");
		}
	}

	/**
	 * @param defaultEntry pass key of default entry or < 0 if no default entry !
	 * @throws Exception
	 */
	private void writeNewSyslist(int listId,
			LinkedHashMap<Integer, String> syslistMap_de,
			LinkedHashMap<Integer, String> syslistMap_en,
			int defaultEntry) throws Exception {

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + listId;
		jdbc.executeUpdate(sqlStr);

		Iterator<Integer> itr = syslistMap_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			String isDefault = "N";
			if (key == defaultEntry) {
				isDefault = "Y";				
			}
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + listId + ", " + key + ", 'de', '" + syslistMap_de.get(key) + "', 0, '" + isDefault + "')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + listId + ", " + key + ", 'en', '" + syslistMap_en.get(key) + "', 0, '" + isDefault + "')");
		}
	}
}
