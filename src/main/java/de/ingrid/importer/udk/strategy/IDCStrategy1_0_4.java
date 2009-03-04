/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

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

		int lstId = 6100;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" (INSPIRE Themen für Verschlagwortung)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		LinkedHashMap<Integer, String> newSyslist6200_de = new LinkedHashMap<Integer, String>(); 
		newSyslist6200_de.put(101, "Koordinatenreferenzsysteme");
		newSyslist6200_de.put(102, "Geografische Gittersysteme");
		newSyslist6200_de.put(103, "Geografische Bezeichnungen");
		newSyslist6200_de.put(104, "Verwaltungseinheiten");
		newSyslist6200_de.put(105, "Adressen");
		newSyslist6200_de.put(106, "Flurstücke/Grundstücke (Katasterparzellen)");
		newSyslist6200_de.put(107, "Verkehrsnetze");
		newSyslist6200_de.put(108, "Gewässernetz");
		newSyslist6200_de.put(109, "Schutzgebiete");
		newSyslist6200_de.put(201, "Höhe");
		newSyslist6200_de.put(202, "Bodenbedeckung");
		newSyslist6200_de.put(203, "Orthofotografie");
		newSyslist6200_de.put(204, "Geologie");
		newSyslist6200_de.put(301, "Statistische Einheiten");
		newSyslist6200_de.put(302, "Gebäude");
		newSyslist6200_de.put(303, "Boden");
		newSyslist6200_de.put(304, "Bodennutzung");
		newSyslist6200_de.put(305, "Gesundheit und Sicherheit");
		newSyslist6200_de.put(306, "Versorgungswirtschaft und staatliche Dienste");
		newSyslist6200_de.put(307, "Umweltüberwachung");
		newSyslist6200_de.put(308, "Produktions- und Industrieanlagen");
		newSyslist6200_de.put(309, "Landwirtschaftliche Anlagen und Aquakulturanlagen");
		newSyslist6200_de.put(310, "Verteilung der Bevölkerung — Demografie");
		newSyslist6200_de.put(311, "Bewirtschaftungsgebiete/Schutzgebiete/geregelte Gebiete und Berichterstattungseinheiten");
		newSyslist6200_de.put(312, "Gebiete mit naturbedingten Risiken");
		newSyslist6200_de.put(313, "Atmosphärische Bedingungen");
		newSyslist6200_de.put(314, "Meteorologisch-geografische Kennwerte");
		newSyslist6200_de.put(315, "Ozeanografisch-geografische Kennwerte");
		newSyslist6200_de.put(316, "Meeresregionen");
		newSyslist6200_de.put(317, "Biogeografische Regionen");
		newSyslist6200_de.put(318, "Lebensräume und Biotope");
		newSyslist6200_de.put(319, "Verteilung der Arten");
		newSyslist6200_de.put(320, "Energiequellen");
		newSyslist6200_de.put(321, "Mineralische Bodenschätze");
		newSyslist6200_de.put(99999, "Kein INSPIRE-Thema");

		// english syslist
		LinkedHashMap<Integer, String> newSyslist6200_en = new LinkedHashMap<Integer, String>(); 
		newSyslist6200_en.put(101, "Coordinate reference systems");
		newSyslist6200_en.put(102, "Geographical grid systems");
		newSyslist6200_en.put(103, "Geographical names");
		newSyslist6200_en.put(104, "Administrative units");
		newSyslist6200_en.put(105, "Addresses");
		newSyslist6200_en.put(106, "Cadastral parcels");
		newSyslist6200_en.put(107, "Transport networks");
		newSyslist6200_en.put(108, "Hydrography");
		newSyslist6200_en.put(109, "Protected sites");
		newSyslist6200_en.put(201, "Elevation");
		newSyslist6200_en.put(202, "Land cover");
		newSyslist6200_en.put(203, "Orthoimagery");
		newSyslist6200_en.put(204, "Geology");
		newSyslist6200_en.put(301, "Statistical units");
		newSyslist6200_en.put(302, "Buildings");
		newSyslist6200_en.put(303, "Soil");
		newSyslist6200_en.put(304, "Land use");
		newSyslist6200_en.put(305, "Human health and safety");
		newSyslist6200_en.put(306, "Utility and governmental services");
		newSyslist6200_en.put(307, "Environmental monitoring facilities");
		newSyslist6200_en.put(308, "Production and industrial facilities");
		newSyslist6200_en.put(309, "Agricultural and aquaculture facilities");
		newSyslist6200_en.put(310, "Population distribution — demography");
		newSyslist6200_en.put(311, "Area management/restriction/regulation zones and reporting units");
		newSyslist6200_en.put(312, "Natural risk zones");
		newSyslist6200_en.put(313, "Atmospheric conditions");
		newSyslist6200_en.put(314, "Meteorological geographical features");
		newSyslist6200_en.put(315, "Oceanographic geographical features");
		newSyslist6200_en.put(316, "Sea regions");
		newSyslist6200_en.put(317, "Bio-geographical regions");
		newSyslist6200_en.put(318, "Habitats and biotopes");
		newSyslist6200_en.put(319, "Species distribution");
		newSyslist6200_en.put(320, "Energy resources");
		newSyslist6200_en.put(321, "Mineral resources");
		newSyslist6200_en.put(99999, "No INSPIRE Theme");

		Iterator<Integer> itr = newSyslist6200_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslist6200_de.get(key) + "', 0, 'N')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslist6200_en.get(key) + "', 0, 'N')");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}

	protected void updateSysGui() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui...");
		}
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
