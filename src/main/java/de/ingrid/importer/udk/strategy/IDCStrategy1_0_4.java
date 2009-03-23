/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.util.InspireThemesHelper;

/**
 * IGC Update: IMPORT/EXPORT (sys_job_info) etc. 
 * 
 * @author martin
 */
public class IDCStrategy1_0_4 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_4.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_104;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		System.out.println("REMEMBER: sys_list update via csv NOT supported anymore ! " +
			"Assure syslists csv were imported before executing 104 update !");

		jdbc.setAutoCommit(false);

		// SPECIAL: first update structure of generic key table !!! has changed !
		// ---------------------------------
		// NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Recreate sys_generic_key table (new structure)...");
		recreateGenericKeyDataStructure();
		System.out.println("done.");

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

		System.out.print("  Updating sys_gui...");
		updateSysGui();
		System.out.println("done.");

		// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
		// via getNextId() ...

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void recreateGenericKeyDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Recreating table sys_generic_key -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 'sys_generic_key' ...");
		}
		jdbc.getDBLogic().dropTable("sys_generic_key", jdbc);


		if (log.isInfoEnabled()) {
			log.info("Recreate table 'sys_generic_key' with new structure ...");
		}
		jdbc.getDBLogic().createTableSysGenericKey(jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Recreating table sys_generic_key... done");
		}
	}
	
	protected void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Create table 'sys_job_info' ...");
		}
		jdbc.getDBLogic().createTableSysJobInfo(jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Change field type of 't011_obj_geo.datasource_uuid' from TEXT to VARCHAR(255) ...");
		}
		jdbc.getDBLogic().modifyColumn("datasource_uuid", ColumnType.VARCHAR255, "t011_obj_geo", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Change field type of 't021_communication.comm_value' to VARCHAR(255) ...");
		}
		jdbc.getDBLogic().modifyColumn("comm_value", ColumnType.VARCHAR255, "t021_communication", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'entry_id' to table 'searchterm_value'...");
		}
		jdbc.getDBLogic().addColumn("entry_id", ColumnType.INTEGER, "searchterm_value", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'gemet_id' to table 'searchterm_sns'...");
		}
		jdbc.getDBLogic().addColumn("gemet_id", ColumnType.VARCHAR50, "searchterm_sns", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

// ---------------------------
		int lstId = 6100;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" (INSPIRE Themen für Verschlagwortung)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		LinkedHashMap<Integer, String> newSyslist6100_de = InspireThemesHelper.inspireThemes_de; 
		// english syslist
		LinkedHashMap<Integer, String> newSyslist6100_en = InspireThemesHelper.inspireThemes_en; 

		Iterator<Integer> itr = newSyslist6100_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslist6100_de.get(key) + "', 0, 'N')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslist6100_en.get(key) + "', 0, 'N')");
		}

// ---------------------------
		lstId = 527;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" (ISO)Themenkategorie-Codeliste (ISO B.5.27 MD_TopicCategoryCode)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		LinkedHashMap<Integer, String> newSyslist527_de = new LinkedHashMap<Integer, String>();
		newSyslist527_de.put(1, "Landwirtschaft");
		newSyslist527_de.put(2, "Biologie");
		newSyslist527_de.put(3, "Grenzen");
		newSyslist527_de.put(4, "Atmosphäre");
		newSyslist527_de.put(5, "Wirtschaft");
		newSyslist527_de.put(6, "Höhenangaben");
		newSyslist527_de.put(7, "Umwelt");
		newSyslist527_de.put(8, "Geowissenschaften");
		newSyslist527_de.put(9, "Gesundheitswesen");
		newSyslist527_de.put(10, "Oberflächenbeschreibung");
		newSyslist527_de.put(11, "Militär und Aufklärung");
		newSyslist527_de.put(12, "Binnengewässer");
		newSyslist527_de.put(13, "Ortsangaben");
		newSyslist527_de.put(14, "Meere");
		newSyslist527_de.put(15, "Planungsunterlagen, Kataster");
		newSyslist527_de.put(16, "Gesellschaft");
		newSyslist527_de.put(17, "Bauwerke");
		newSyslist527_de.put(18, "Verkehrswesen");
		newSyslist527_de.put(19, "Ver- und Entsorgung, Kommunikation");
		// english syslist
		LinkedHashMap<Integer, String> newSyslist527_en = new LinkedHashMap<Integer, String>(); 
		newSyslist527_en.put(1, "farming");
		newSyslist527_en.put(2, "biota");
		newSyslist527_en.put(3, "boundaries");
		newSyslist527_en.put(4, "climatologyMeteorologyAtmosphere");
		newSyslist527_en.put(5, "economy");
		newSyslist527_en.put(6, "elevation");
		newSyslist527_en.put(7, "environment");
		newSyslist527_en.put(8, "geoscientificInformation");
		newSyslist527_en.put(9, "health");
		newSyslist527_en.put(10, "imageryBaseMapsEarthCover");
		newSyslist527_en.put(11, "intelligenceMilitary");
		newSyslist527_en.put(12, "inlandWaters");
		newSyslist527_en.put(13, "location");
		newSyslist527_en.put(14, "oceans");
		newSyslist527_en.put(15, "planningCadastre");
		newSyslist527_en.put(16, "society");
		newSyslist527_en.put(17, "structure");
		newSyslist527_en.put(18, "transportation");
		newSyslist527_en.put(19, "utilitiesCommunication");

		itr = newSyslist527_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslist527_de.get(key) + "', 0, 'N')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslist527_en.get(key) + "', 0, 'N')");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}

	protected void updateSysGui() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui...");
		}

		// TODO: integrate new UI fields !

/*
		if (log.isInfoEnabled()) {
			log.info("Inserting initial sys_gui entries...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_gui";
		jdbc.executeUpdate(sqlStr);

		LinkedHashMap<String, Integer> initialSysGuis = new LinkedHashMap<String, Integer>();
		Integer initialBehaviour = -1;
		initialSysGuis.put("1130", initialBehaviour);
		
		Iterator<String> itr = initialSysGuis.keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			jdbc.executeUpdate("INSERT INTO sys_gui (id, gui_id, behaviour) VALUES ("
					+ getNextId() + ", '" + key + "', " + initialSysGuis.get(key) + ")");
		}
*/		
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui... done");
		}
	}

	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 'sys_export' ...");
		}
		jdbc.getDBLogic().dropTable("sys_export", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
