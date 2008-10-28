/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * IGC Update: INSPIRE + sortKey in syslist + tree path etc. etc.
 * 
 * @author martin
 */
public class IDCStrategy1_0_3 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_3.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_103;

	private int defaultSyslist6000EntryId = 3;
	private String defaultSyslist6000EntryValue = "nicht evaluiert";

	private int noData_Syslist6010EntryId = 1;
	private String noData_Syslist6010EntryValue = "keine";
	private int existingData_Syslist6010EntryId = 6;
	private String existingData_Syslist6010EntryValue = "aufgrund der Rechte des geistigen Eigentums";

	private Integer wmsSyslist5100EntryId = 2;
	private String wmsSyslist5100EntryValue = "Visualisierungsdienste (WMS)";
	private Integer wfsSyslist5100EntryId = 3;
	private String wfsSyslist5100EntryValue = "Zugriffsdienste (WFS)";
	private int defaultSyslist5100EntryId = 6;
	private String defaultSyslist5100EntryValue = "Andere Dienste";

	private LinkedHashMap<Integer, String> newSyslist5120 = new LinkedHashMap<Integer, String>(); 
	private HashMap<Integer, Integer> oldToNewKeySyslist5120 = new HashMap<Integer, Integer>(); 

	private LinkedHashMap<Integer, String> newSyslist5200_de = new LinkedHashMap<Integer, String>(); 
	private HashMap<Integer, Integer> oldToNewKeySyslist5200 = new HashMap<Integer, Integer>(); 

	private final static Integer COMMUNICATION_TYPE_EMAIL_KEY = 3;
	private final static String COMMUNICATION_TYPE_EMAIL_VALUE = "Email";

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// FIRST EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !

		// write IDC structure version !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		System.out.print("  Updating sys_list...");
		updateSysList();
		System.out.println("done.");

		System.out.print("  Updating object_conformity...");
		updateObjectConformity();
		System.out.println("done.");

		System.out.print("  Updating t011_obj_geo...");
		updateT011ObjGeo();
		System.out.println("done.");

		System.out.print("  Updating object_access...");
		updateObjectAccess();
		System.out.println("done.");

		System.out.print("  Updating t011_obj_serv...");
		updateT011ObjServ();
		System.out.println("done.");

		System.out.print("  Updating t011_obj_serv_operation...");
		updateT011ObjServOperation();
		System.out.println("done.");

		System.out.print("  Updating t011_obj_serv_type...");
		updateT011ObjServType();
		System.out.println("done.");

		System.out.print("  Updating t021_communication...");
		updateT021Communication();
		System.out.println("done.");

		System.out.print("  Updating sys_gui...");
		updateSysGui();
		System.out.println("done.");

		System.out.print("  Updating new object/address metadata tables ...");
		updateTablesMetadata();
		System.out.println("done.");

		System.out.print("  Updating object_comment/address_comment new line attribute ...");
		updateComments();
		System.out.println("done.");

		System.out.print("  Updating object_node/address_node new tree_path attribute ...");
		updateTreePath();
		System.out.println("done.");

		// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
		// via getNextId() ...

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
		System.out.println("REMEMBER: full sys_lists (including localization) have to be imported via csv.");
		System.out.println("REMEMBER: run FIX 1.0.2_fix_syslist_100_101 to fix all values of syslist 100/101");
	}

	protected void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Create table 'object_conformity'...");
		}
		jdbc.getDBLogic().createTableObjectConformity(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'datasource_uuid' to table 't011_obj_geo'...");
		}
		// don't add "not null", can be empty in working version !
		jdbc.getDBLogic().addColumn("datasource_uuid", ColumnType.TEXT, "t011_obj_geo", false, null, jdbc);
		
		if (log.isInfoEnabled()) {
			log.info("Create table 'object_access'...");
		}
		jdbc.getDBLogic().createTableObjectAccess(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create table 't011_obj_serv_type'...");
		}
		jdbc.getDBLogic().createTableT011ObjServType(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create table 't011_obj_serv_scale'...");
		}
		jdbc.getDBLogic().createTableT011ObjServScale(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create table 'sys_gui'...");
		}
		jdbc.getDBLogic().createTableSysGui(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'line' to table 'sys_list'...");
		}
		jdbc.getDBLogic().addColumn("line", ColumnType.INTEGER, "sys_list", false, 0, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Create tables 'object_metadata', 'address_metadata'...");
		}
		jdbc.getDBLogic().createTablesMetadata(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add metadata association to object/address tables (FKey and index)...");
		}
		jdbc.getDBLogic().addColumn("obj_metadata_id", ColumnType.BIGINT, "t01_object", false, null, jdbc);
		jdbc.getDBLogic().addIndex("obj_metadata_id", "t01_object", "idxObj_ObjMeta", jdbc);
		jdbc.getDBLogic().addColumn("addr_metadata_id", ColumnType.BIGINT, "t02_address", false, null, jdbc);
		jdbc.getDBLogic().addIndex("addr_metadata_id", "t02_address", "idxAddr_AddrMeta", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'line' to tables 'object_comment', 'address_comment'...");
		}
		jdbc.getDBLogic().addColumn("line", ColumnType.INTEGER, "object_comment", false, 0, jdbc);
		jdbc.getDBLogic().addColumn("line", ColumnType.INTEGER, "address_comment", false, 0, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'tree_path' to tables 'object_node', 'address_node'...");
		}
		jdbc.getDBLogic().addColumn("tree_path", ColumnType.MEDIUMTEXT, "object_node", false, null, jdbc);
		jdbc.getDBLogic().addColumn("tree_path", ColumnType.MEDIUMTEXT, "address_node", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
	
	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		int lstId = 6000;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Grad der Konformität)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 1, 'de', 'konform', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 2, 'de', 'nicht konform', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", " + defaultSyslist6000EntryId + ", 'de', '"
			+ defaultSyslist6000EntryValue + "', 0, 'Y');");

		// --------------------

		lstId = 6010;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Zugangsbeschränkungen)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", " + noData_Syslist6010EntryId + ", 'de', '" + noData_Syslist6010EntryValue 
			+ "', 0, 'Y');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 2, 'de', 'aufgrund der Vertraulichkeit der Verfahren von Behörden', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 3, 'de', 'aufgrund internationaler Beziehungen, der öffentliche Sicherheit oder der Landesverteidigung', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 4, 'de', 'aufgrund laufender Gerichtsverfahren', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 5, 'de', 'aufgrund der Vertraulichkeit von Geschäfts- oder Betriebsinformationen', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", " + existingData_Syslist6010EntryId + ", 'de', '" + existingData_Syslist6010EntryValue
			+ "', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 7, 'de', 'aufgrund der Vertraulichkeit personenbezogener Daten', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 8, 'de', 'aufgrund des Schutzes einer Person', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 9, 'de', 'aufgrund des Schutzes von Umweltbereichen', 0, 'N');");

		// --------------------

		lstId = 5100;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" (Service-Klassifikation)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'de', 'Suchdienste (CSW)', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'en', 'Discovery Service', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + wmsSyslist5100EntryId + ", 'de', '" + wmsSyslist5100EntryValue + "', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("					
				+ getNextId() + ", " + lstId + ", " + wmsSyslist5100EntryId + ", 'en', 'View Service', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + wfsSyslist5100EntryId + ", 'de', '" + wfsSyslist5100EntryValue + "', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("					
				+ getNextId() + ", " + lstId + ", " + wfsSyslist5100EntryId + ", 'en', 'Download Service', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 4, 'de', 'Transformationsdienste (WCTS)', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("					
				+ getNextId() + ", " + lstId + ", 4, 'en', 'Transformation Service', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 5, 'de', 'Verkettete Geodatendienste', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("					
				+ getNextId() + ", " + lstId + ", 5, 'en', 'Invoke Spatial Data Service', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", " + defaultSyslist5100EntryId + ", 'de', '" + defaultSyslist5100EntryValue + "', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("					
				+ getNextId() + ", " + lstId + ", " + defaultSyslist5100EntryId + ", 'en', 'Other Service', 0, 'N');");

		// --------------------

		lstId = 5105;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Operations for CSW Service)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'de', 'GetCapabilities', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 2, 'de', 'GetRecords', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 3, 'de', 'GetRecordById', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 4, 'de', 'DescribeRecord', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 5, 'de', 'GetDomain', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 6, 'de', 'Transaction', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 7, 'de', 'Harvest', 0, 'N');");

		// --------------------

		lstId = 5130;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" (Operations for WCTS Service)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// insert new syslist
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 1, 'de', 'GetCapabilities', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 2, 'de', 'Transform', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 3, 'de', 'IsTransformable', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 4, 'de', 'GetTransformation', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
				+ getNextId() + ", " + lstId + ", 5, 'de', 'GetResourceById', 0, 'N');");

		// --------------------

		lstId = 5120;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" (Operations for WFS Service)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		newSyslist5120.put(1, "GetCapabilities");
		newSyslist5120.put(2, "DescribeFeatureType");
		newSyslist5120.put(3, "GetFeature");
		newSyslist5120.put(4, "LockFeature");
		newSyslist5120.put(5, "Transaction");
		
		oldToNewKeySyslist5120.put(1, 2);
		// both old "GetFeature" mapped to new "GetFeature" (error in old syslist)
		oldToNewKeySyslist5120.put(2, 3);
		oldToNewKeySyslist5120.put(3, 3);
		oldToNewKeySyslist5120.put(4, 4);
		oldToNewKeySyslist5120.put(5, 5);
		
		Iterator<Integer> itr = newSyslist5120.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslist5120.get(key) + "', 0, 'N')");
		}
		
		// --------------------

		lstId = 5200;
		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + lstId +	" (INSPIRE: Classification of spatial data services)...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + lstId;
		jdbc.executeUpdate(sqlStr);

		// german syslist
		newSyslist5200_de.put(101, "Katalogdienst (humanCatalogueViewer)");
		newSyslist5200_de.put(102, "Dienst für geografische Visualisierung");
		newSyslist5200_de.put(103, "Dienst für geografische Tabellenkalkulation");
		newSyslist5200_de.put(104, "Editor für Verarbeitungsdienste");
		newSyslist5200_de.put(105, "Editor für die Definition von Bearbeitungsketten");
		newSyslist5200_de.put(106, "Aufrufprogramm für Bearbeitungsketten");
		newSyslist5200_de.put(107, "Editor für geografische Objekte");
		newSyslist5200_de.put(108, "Editor für geografische Symbole");
		newSyslist5200_de.put(109, "Editor für die Objektgeneralisierung");
		newSyslist5200_de.put(110, "Betrachter für geografische Datenstrukturen");
		newSyslist5200_de.put(201, "Dienst für den Zugriff auf Objekte");
		newSyslist5200_de.put(202, "Dienst für den Zugriff auf grafische Darstellungen");
		newSyslist5200_de.put(203, "Dienst für den Zugriff auf Rasterdaten");
		newSyslist5200_de.put(204, "Dienst für die Beschreibung von Sensoren");
		newSyslist5200_de.put(205, "Dienst für den Zugriff auf Produkte");
		newSyslist5200_de.put(206, "Dienst für den Zugriff auf Objektarten");
		newSyslist5200_de.put(207, "Katalogdienst (infoCatalogueService)");
		newSyslist5200_de.put(208, "Registerdienst");
		newSyslist5200_de.put(209, "Gazetteerdienst");
		newSyslist5200_de.put(210, "Auftragsdienst");
		newSyslist5200_de.put(211, "Dauerauftragsdienst");
		newSyslist5200_de.put(301, "Dienst für die Definition von Bearbeitungsketten");
		newSyslist5200_de.put(302, "Dienst für die Ausführung von Bearbeitungsketten ");
		newSyslist5200_de.put(303, "Abonnementdienst");
		newSyslist5200_de.put(401, "Dienst für die Konversion von Koordinaten");
		newSyslist5200_de.put(402, "Dienst für die Transformation von Koordinaten");
		newSyslist5200_de.put(403, "Dienst für die Umwandlung zwischen Raster- und Vektordaten");
		newSyslist5200_de.put(404, "Dienst für die Konversion von Bildkoordinaten");
		newSyslist5200_de.put(405, "Entzerrungsdienst");
		newSyslist5200_de.put(406, "Ortho-Entzerrungsdienst");
		newSyslist5200_de.put(407, "Dienst für die Justierung von Geometriemodellen von Sensoren");
		newSyslist5200_de.put(408, "Dienst für die Konversion von Geometriemodellen");
		newSyslist5200_de.put(409, "Geografischer Ausschnittsdienst");
		newSyslist5200_de.put(410, "Raumbezogener Auswahldienst");
		newSyslist5200_de.put(411, "Kachelungsdienst");
		newSyslist5200_de.put(412, "Messungsdienst");
		newSyslist5200_de.put(413, "Objektbearbeitungsdienste");
		newSyslist5200_de.put(414, "Vergleichsdienst");
		newSyslist5200_de.put(415, "Generalisierungsdienst");
		newSyslist5200_de.put(416, "Routensuchdienst");
		newSyslist5200_de.put(417, "Positionierungsdienst");
		newSyslist5200_de.put(418, "Analysedienst für räumliche Nachbarschaftsbeziehungen");
		newSyslist5200_de.put(501, "Berechnungsdienst für Geoparameter");
		newSyslist5200_de.put(502, "Dienst für die thematische Klassifizierung");
		newSyslist5200_de.put(503, "Generalisierungsdienst für Objektarten");
		newSyslist5200_de.put(504, "Themenbezogener Ausschnittsdienst");
		newSyslist5200_de.put(505, "Zähldienst");
		newSyslist5200_de.put(506, "Erkennungsdienst für Veränderungen");
		newSyslist5200_de.put(507, "Auszugsdienste für geografische Informationen");
		newSyslist5200_de.put(508, "Themenbezogener Bildverarbeitungsdienst");
		newSyslist5200_de.put(509, "Auflösungsreduzierungsdienst");
		newSyslist5200_de.put(510, "Bildbearbeitungsdienste");
		newSyslist5200_de.put(511, "Interpretationsdienste für Bilder");
		newSyslist5200_de.put(512, "Bildsynthesedienste");
		newSyslist5200_de.put(513, "Multiband-Bildbearbeitung");
		newSyslist5200_de.put(514, "Objekterkennungsdienst");
		newSyslist5200_de.put(515, "Geoparserdienst");
		newSyslist5200_de.put(516, "Geocodierungsdienst");
		newSyslist5200_de.put(601, "Transformationsdienst für den Zeitbezug");
		newSyslist5200_de.put(602, "Zeitbezogener Ausschnittsdienst");
		newSyslist5200_de.put(603, "Zeitbezogener Auswahldienst");
		newSyslist5200_de.put(604, "Analysedienst für zeitbezogene Nachbarschaftsbeziehungen");
		newSyslist5200_de.put(701, "Dienst für statistische Berechnungen");
		newSyslist5200_de.put(702, "Ergänzungsdienste für Geodaten");
		newSyslist5200_de.put(801, "Codierungsdienst");
		newSyslist5200_de.put(802, "Übertragungsdienst");
		newSyslist5200_de.put(803, "Kompressionsdienst für Geodaten");
		newSyslist5200_de.put(804, "Umformatierungsdienst für Geodaten");
		newSyslist5200_de.put(805, "Nachrichtenübermittlungsdienst");
		newSyslist5200_de.put(806, "Dienst für den Zugriff auf externe Daten und Programme");
		newSyslist5200_de.put(901, "Kein geografischer Dienst");

		// english syslist
		LinkedHashMap<Integer, String> newSyslist5200_en = new LinkedHashMap<Integer, String>(); 
		newSyslist5200_en.put(101, "Catalogue viewer");
		newSyslist5200_en.put(102, "Geographic viewer");
		newSyslist5200_en.put(103, "Geographic spreadsheet viewer");
		newSyslist5200_en.put(104, "Service editor");
		newSyslist5200_en.put(105, "Chain definition editor");
		newSyslist5200_en.put(106, "Workflow enactment manager");
		newSyslist5200_en.put(107, "Geographic feature editor");
		newSyslist5200_en.put(108, "Geographic symbol editor");
		newSyslist5200_en.put(109, "Feature generalization editor");
		newSyslist5200_en.put(110, "Geographic data-structure viewer");
		newSyslist5200_en.put(201, "Feature access service");
		newSyslist5200_en.put(202, "Map access service");
		newSyslist5200_en.put(203, "Coverage access service");
		newSyslist5200_en.put(204, "Sensor description service");
		newSyslist5200_en.put(205, "Product access service");
		newSyslist5200_en.put(206, "Feature type service");
		newSyslist5200_en.put(207, "Catalogue service");
		newSyslist5200_en.put(208, "Registry Service");
		newSyslist5200_en.put(209, "Gazetteer service");
		newSyslist5200_en.put(210, "Order handling service");
		newSyslist5200_en.put(211, "Standing order service");
		newSyslist5200_en.put(301, "Chain definition service");
		newSyslist5200_en.put(302, "Workflow enactment service");
		newSyslist5200_en.put(303, "Subscription service");
		newSyslist5200_en.put(401, "Coordinate conversion service");
		newSyslist5200_en.put(402, "Coordinate transformation service");
		newSyslist5200_en.put(403, "Coverage/vector conversion service");
		newSyslist5200_en.put(404, "Image coordinate conversion service");
		newSyslist5200_en.put(405, "Rectification service");
		newSyslist5200_en.put(406, "Orthorectification service");
		newSyslist5200_en.put(407, "Sensor geometry model adjustment service");
		newSyslist5200_en.put(408, "Image geometry model conversion service");
		newSyslist5200_en.put(409, "Subsetting service (spatial)");
		newSyslist5200_en.put(410, "Sampling service (spatial)");
		newSyslist5200_en.put(411, "Tiling change service");
		newSyslist5200_en.put(412, "Dimension measurement service");
		newSyslist5200_en.put(413, "Feature manipulation services");
		newSyslist5200_en.put(414, "Feature matching service");
		newSyslist5200_en.put(415, "Feature generalization service (spatial)");
		newSyslist5200_en.put(416, "Route determination service");
		newSyslist5200_en.put(417, "Positioning service");
		newSyslist5200_en.put(418, "Proximity analysis service");
		newSyslist5200_en.put(501, "Geoparameter calculation service");
		newSyslist5200_en.put(502, "Thematic classification service");
		newSyslist5200_en.put(503, "Feature generalization service (thematic)");
		newSyslist5200_en.put(504, "Subsetting service (thematic)");
		newSyslist5200_en.put(505, "Spatial counting service");
		newSyslist5200_en.put(506, "Change detection service");
		newSyslist5200_en.put(507, "Geographic information extraction services");
		newSyslist5200_en.put(508, "Image processing service");
		newSyslist5200_en.put(509, "Reduced resolution generation service");
		newSyslist5200_en.put(510, "Image Manipulation Services");
		newSyslist5200_en.put(511, "Image understanding services");
		newSyslist5200_en.put(512, "Image synthesis services");
		newSyslist5200_en.put(513, "Multi-band image manipulation");
		newSyslist5200_en.put(514, "Object detection service");
		newSyslist5200_en.put(515, "Geoparsing service");
		newSyslist5200_en.put(516, "Geocoding service");
		newSyslist5200_en.put(601, "Temporal reference system transformation service");
		newSyslist5200_en.put(602, "Subsetting service (temporal)");
		newSyslist5200_en.put(603, "Sampling service (temporal)");
		newSyslist5200_en.put(604, "Temporal proximity analysis service");
		newSyslist5200_en.put(701, "Statistical calculation service");
		newSyslist5200_en.put(702, "Geographic annotation services");
		newSyslist5200_en.put(801, "Encoding service");
		newSyslist5200_en.put(802, "Transfer service");
		newSyslist5200_en.put(803, "Geographic compression service");
		newSyslist5200_en.put(804, "Geographic format conversion service");
		newSyslist5200_en.put(805, "Messaging service");
		newSyslist5200_en.put(806, "Remote file and executable management");
		newSyslist5200_en.put(901, "Non Geographic Service");

		oldToNewKeySyslist5200.put(1, 207);
		oldToNewKeySyslist5200.put(2, 202);
		oldToNewKeySyslist5200.put(3, 201);
		oldToNewKeySyslist5200.put(6, 901);

		itr = newSyslist5200_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslist5200_de.get(key) + "', 0, 'N')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'en', '" + newSyslist5200_en.get(key) + "', 0, 'N')");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}
	
	protected void updateObjectConformity() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add default entries for every object...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select distinct objNode.id as objNodeId, obj.id as objId " +
			"from t01_object obj, object_node objNode " +
			"where obj.obj_uuid = objNode.obj_uuid";

		ResultSet rs = jdbc.executeQuery(sql);
		HashMap<Long, Boolean> processedNodeIds = new HashMap<Long,Boolean>();
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objId = rs.getLong("objId");

			// publication_date of INSPIRE is 14.03.2007, see email 22.10.2008 13:57, "AW: AW: PortalU Aktualisierung!!"
			String defaultSpecification = "INSPIRE-Richtlinie";
			String defaultPubDate = "20070314000000000";

			jdbc.executeUpdate("INSERT INTO object_conformity (id, obj_id, line, specification, degree_key, degree_value, publication_date) " +
				"VALUES (" + getNextId() + ", " + objId + ", 1, '" + defaultSpecification + "', "
				+ defaultSyslist6000EntryId + ", '" + defaultSyslist6000EntryValue + "', '" + defaultPubDate + "');");
			
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.containsKey(objNodeId)) {
				JDBCHelper.updateObjectIndex(objNodeId, defaultSpecification, jdbc); // ObjectConformity.specification
				JDBCHelper.updateObjectIndex(objNodeId, defaultSyslist6000EntryValue, jdbc); // ObjectConformity.degreeValue
				
				processedNodeIds.put(objNodeId, true);
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity... done");
		}
	}

	protected void updateT011ObjGeo() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_geo...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add 'datasource_uuid' default values, 'special_base' defaults...");
		}

		// get catalog name and create prefix for unique datasource_uuid
		String sql = "select cat_name from t03_catalogue";
		ResultSet rs = jdbc.executeQuery(sql);
		rs.next();
		String catName = rs.getString("cat_name");
		rs.close();
		
		String datasourceUuidPrefix = catName.replace(' ', '_');
		datasourceUuidPrefix += ":";

		// then add default data for ALL t011_obj_geo 
		sql = "select distinct objGeo.id as objGeoId, objGeo.special_base, obj.obj_uuid as objUuid " +
			"from t011_obj_geo objGeo, t01_object obj " +
			"where objGeo.obj_id = obj.id";

		rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objGeoId = rs.getLong("objGeoId");
			String objUuid = rs.getString("objUuid");
			String objGeoSpecialBase = rs.getString("special_base");

			String datasourceUuid = datasourceUuidPrefix + objUuid;
			jdbc.executeUpdate("UPDATE t011_obj_geo SET datasource_uuid = '" + datasourceUuid + "' " +
				"where id = " + objGeoId);
			
			// special_base is now mandatory ! supply default value
			if (objGeoSpecialBase == null || objGeoSpecialBase.trim().length() == 0) {
				jdbc.executeUpdate("UPDATE t011_obj_geo SET special_base = 'Unbekannt' " +
						"where id = " + objGeoId);				
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_geo... done");
		}
	}

	protected void updateObjectAccess() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_access...");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate obj.avail_access_note, obj.fees to table object_access...");
		}

		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, obj.avail_access_note, obj.fees " +
			"from t01_object obj, object_node objNode " +
			"where obj.obj_uuid = objNode.obj_uuid";

		// Node may contain different object versions (working and published version), just to be sure 
		// we track written data in hash maps to avoid multiple writing for same object (or should we trust upper sql ;)
		HashMap<Long, Boolean> processedObjIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objWorkId = rs.getLong("objWorkId");
			long objId = rs.getLong("objId");
			String availAccessNote = rs.getString("avail_access_note");
			availAccessNote = (availAccessNote == null) ? "" : availAccessNote.trim();
			String fees = rs.getString("fees");
			fees = (fees == null) ? "" : fees.trim();

			// write values if not written yet !
			if (!processedObjIds.containsKey(objId)) {
				// default: NO access data set in object
				int syslist6010EntryId = noData_Syslist6010EntryId;
				String syslist6010EntryValue = noData_Syslist6010EntryValue;
				String termsOfUse = "keine Einschränkungen";

				// values when access data set in object
				if (availAccessNote.length() > 0 || fees.length() > 0) {
					syslist6010EntryId = existingData_Syslist6010EntryId;
					syslist6010EntryValue = existingData_Syslist6010EntryValue;
					termsOfUse = "";
					if (availAccessNote.length() > 0) {
						termsOfUse += availAccessNote;
					}
					if (fees.length() > 0) {
						if (termsOfUse.length() > 0) {
							termsOfUse += " // ";						
						}
						termsOfUse += fees;
					}
				}

				jdbc.executeUpdate("INSERT INTO object_access (id, obj_id, line, restriction_key, restriction_value, terms_of_use) "
					+ "VALUES (" + getNextId() + ", " + objId + ", 1, " + syslist6010EntryId + ", '" + syslist6010EntryValue
					+ "', '" + termsOfUse + "');");
				
				processedObjIds.put(objId, true);

				// extend object index (index contains only data of working versions !)
				if (objWorkId == objId) {
					JDBCHelper.updateObjectIndex(objNodeId, syslist6010EntryValue, jdbc); // ObjectAccess.restrictionValue
					JDBCHelper.updateObjectIndex(objNodeId, termsOfUse, jdbc); // ObjectAccess.termsOfUse
				}
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating object_access... done");
		}
	}

	protected void updateT011ObjServ() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv...");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate type_key, type_value to new syslist (Service-Klassifikation) ...");
		}

		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, " +
			"objServ.id as objServId, objServ.type_key, objServ.type_value, objServ.description " +
			"from t011_obj_serv objServ, t01_object obj, object_node objNode " +
			"where objServ.obj_id = obj.id " +
			"and obj.obj_uuid = objNode.obj_uuid";

		// Node may contain different object versions (working and published version), just to be sure 
		// we track written data in hash maps to avoid multiple writing for same object (or should we trust upper sql ;)
		HashMap<Long, Boolean> processedObjIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objWorkId = rs.getLong("objWorkId");
			long objId = rs.getLong("objId");

			long objServId = rs.getLong("objServId");
			Integer objServTypeKey = rs.getInt("type_key");
			objServTypeKey = (objServTypeKey == null) ? -1 : objServTypeKey;
			String objServTypeValue = rs.getString("type_value");
			objServTypeValue = (objServTypeValue == null) ? "" : objServTypeValue.trim();
			String objServDescr = rs.getString("description");

			// write values if not written yet !
			if (!processedObjIds.containsKey(objId)) {
				// default
				int syslist5100EntryId = defaultSyslist5100EntryId;
				String syslist5100EntryValue = defaultSyslist5100EntryValue;

				// values if data set in object
				if (objServTypeKey == 1) {
					syslist5100EntryId = wmsSyslist5100EntryId;
					syslist5100EntryValue = wmsSyslist5100EntryValue;
				} else if (objServTypeKey == 2) {
					syslist5100EntryId = wfsSyslist5100EntryId;
					syslist5100EntryValue = wfsSyslist5100EntryValue;
				} else {
					// migrate free entry into description !
					if (objServTypeValue.length() > 0) {
						objServDescr = (objServDescr == null) ? "" : objServDescr.trim();
						if (objServDescr.length() > 0) {
							objServDescr = "\n\n" + objServDescr;
						}
						objServDescr = objServTypeValue + objServDescr;
					}
				}

				jdbc.executeUpdate("UPDATE t011_obj_serv SET type_key = " + syslist5100EntryId
					+ ", type_value = '" + syslist5100EntryValue + "', description = '" + objServDescr +
							"' where id = " + objServId);
				
				processedObjIds.put(objId, true);

				// extend object index (index contains only data of working versions !)
				if (objWorkId == objId) {
					JDBCHelper.updateObjectIndex(objNodeId, syslist5100EntryValue, jdbc); // objServ.type_value
				}
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv... done");
		}
	}

	/** migrate old WFS operation entries to new ones (old ones had bug -> two times "GetFeature" in syslist) */
	protected void updateT011ObjServOperation() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv_operation...");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate buggy old WFS operation key/names to new syslist 5120 (WFS Operations) ...");
		}

		// then add entries for ALL t01_objects (no matter whether working or published version) 
		String sql = "select objServOp.id as objServOpId, objServ.type_key, objServOp.name_key " +
			"from t011_obj_serv objServ, t011_obj_serv_operation objServOp " +
			"where objServ.id = objServOp.obj_serv_id";

		HashMap<Long, Boolean> processedServOpIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objServOpId = rs.getLong("objServOpId");

			Integer servTypeKey = rs.getInt("type_key");
			Integer servOpKey = rs.getInt("name_key");
			
			// only process WFS Operations
			if (!wfsSyslist5100EntryId.equals(servTypeKey)) {
				continue;
			}

			// write values if not written yet !
			if (!processedServOpIds.containsKey(objServOpId)) {
				Integer newKey = oldToNewKeySyslist5120.get(servOpKey);
				String newValue = newSyslist5120.get(newKey);

				if (newKey != null) {
					jdbc.executeUpdate("UPDATE t011_obj_serv_operation SET name_key = " + newKey
							+ ", name_value = '" + newValue + "' where id = " + objServOpId);
				}

				processedServOpIds.put(objServOpId, true);
				
				// NO UPDATE OF INDEX, should already contain old values !
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv... done");
		}
	}

	protected void updateT011ObjServType() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv_type...");
		}

		if (log.isInfoEnabled()) {
			log.info("Migrate t011_obj_serv.type to table t011_obj_serv_type.serv_type ...");
		}

		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, " +
		"objServ.id as objServId, objServ.type_key " +
		"from t011_obj_serv objServ, t01_object obj, object_node objNode " +
		"where objServ.obj_id = obj.id " +
		"and obj.obj_uuid = objNode.obj_uuid";

		// Node may contain different object versions (working and published version), just to be sure 
		// we track written data in hash maps to avoid multiple writing for same object (or should we trust upper sql ;)
		HashMap<Long, Boolean> processedObjIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objWorkId = rs.getLong("objWorkId");
			long objId = rs.getLong("objId");

			long objServId = rs.getLong("objServId");
			Integer objServTypeKey = rs.getInt("type_key");

			// write values if not written yet !
			if (!processedObjIds.containsKey(objId)) {
				Integer syslist5200EntryId = oldToNewKeySyslist5200.get(objServTypeKey);
				String syslist5200EntryValue = newSyslist5200_de.get(syslist5200EntryId);

				jdbc.executeUpdate("INSERT INTO t011_obj_serv_type (id, obj_serv_id, line, serv_type_key, serv_type_value) "
					+ "VALUES (" + getNextId() + ", " + objServId + ", 1, " + syslist5200EntryId + ", '"
					+ syslist5200EntryValue	+ "');");

				if (log.isDebugEnabled()) {
					log.debug("Obj-Service id(" + objServId+ "), wrote type " + syslist5200EntryId + "/" + syslist5200EntryValue);
				}
				
				// extend object index (index contains only data of working versions !)
				if (objWorkId == objId) {
					JDBCHelper.updateObjectIndex(objNodeId, syslist5200EntryValue, jdbc); // t011_obj_serv_type.serv_type_value
					
					if (log.isDebugEnabled()) {
						log.debug("Also updated according index objNodeId(" + objNodeId + ") with '" + syslist5200EntryValue + "'");
					}
				}

				processedObjIds.put(objId, true);
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv_type... done");
		}
	}

	protected void updateT021Communication() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating t021_communication...");
		}

		String defaultEmail = importDescriptor.getIdcEmailDefault();
		if (defaultEmail == null || defaultEmail.trim().length() == 0) {
			throw new Exception("Default email missing for updating t021_communication, email is now mandatory");
		}
		String defaultEmailDescr = "Migration INSPIRE: Email hinzugefügt";

		if (log.isInfoEnabled()) {
			log.info("Check every object for email address and add if missing ...");
		}

		// select all addresses !
		String sql = "select addrNode.id as addrNodeId, " +
			"addrNode.addr_id as addrWorkId, addr.id as addrId, " +
			"addrNode.fk_addr_uuid as parentUuid " +
			"from t02_address addr, address_node addrNode " +
			"where addr.adr_uuid = addrNode.addr_uuid";

		// Node may contain different address versions (working and published version), just to be sure 
		// we track written data in hash maps to avoid multiple writing for same address (or should we trust upper sql ;)
		HashMap<Long, Boolean> processedAddrIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long addrNodeId = rs.getLong("addrNodeId");

			long addrWorkId = rs.getLong("addrWorkId");
			long addrId = rs.getLong("addrId");
			boolean isWorkingVersion = (addrWorkId == addrId); 

			String parentAddrUuid = rs.getString("parentUuid");

			// already processed ?
			if (!processedAddrIds.containsKey(addrId)) {
				// check whether address has email
				HashMap returnData = new HashMap();
				String email = getAddressEmail(addrId, returnData);
				if (email == null) {
					// this is the line number the new email is added with
					int line = ((Integer) returnData.get("MAX_LINE")) + 1;
					
					// get email from parent(s), use default email if no email set
					String emailToAdd = getParentAddressEmail(parentAddrUuid, isWorkingVersion);				
					emailToAdd = (emailToAdd == null) ? defaultEmail : emailToAdd;
					
					jdbc.executeUpdate(
						"INSERT INTO t021_communication (id, adr_id, line, commtype_key, commtype_value, comm_value, descr) "
						+ "VALUES (" + getNextId() + ", " + addrId + ", " + line + ", " + COMMUNICATION_TYPE_EMAIL_KEY + ", '"
						+ COMMUNICATION_TYPE_EMAIL_VALUE + "', '" + emailToAdd + "', '" + defaultEmailDescr+ "');");					

					if (log.isDebugEnabled()) {
						log.debug("Updated Address id(" + addrId + ") with email '" + emailToAdd + "'");
					}
					
					// extend address index (index contains only data of working versions !)
					if (isWorkingVersion) {
						JDBCHelper.updateAddressIndex(addrNodeId, emailToAdd, jdbc); // t021_communication.comm_value
						JDBCHelper.updateAddressIndex(addrNodeId, defaultEmailDescr, jdbc); // t021_communication.descr
						
						if (log.isDebugEnabled()) {
							log.debug("Also updated according index addressNodeId(" + addrNodeId + ") with '" + emailToAdd + "', '" + defaultEmailDescr + "'");
						}
					}
				}

				processedAddrIds.put(addrId, true);
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t021_communication... done");
		}
	}
	
	private String getAddressEmail(long addrId, HashMap returnData) throws Exception {
		// select all communication values of address
		String sql = "select commtype_key, comm_value, line " +
		"from t021_communication " +
		"where adr_id = " + addrId + " order by line";

		String email = null;
		ResultSet rs = jdbc.executeQuery(sql);
		int maxLine = 0;
		while (rs.next()) {
			if (COMMUNICATION_TYPE_EMAIL_KEY.equals(rs.getInt("commtype_key"))) {
				email = rs.getString("comm_value");
			}
			maxLine = rs.getInt("line");
		}
		rs.close();
		
		if (returnData != null) {
			returnData.put("MAX_LINE", maxLine);
		}

		return email;
	}

	private String getParentAddressEmail(String parentAddrUuid, boolean isWorkingVersion) throws Exception {
		if (parentAddrUuid == null) {
			return null;
		}

		String sql;
		if (isWorkingVersion) {
			sql = "select addr_id as addrId";
		} else {
			sql = "select addr_id_published as addrId"; 
		}
		sql += ", fk_addr_uuid as parentUuid " +
			"from address_node " +
			"where addr_uuid = '" + parentAddrUuid + "'"; 

		String email = null;
		String parentUuid = null;
		ResultSet rs = jdbc.executeQuery(sql);
		if (rs.next()) {
			parentUuid = rs.getString("parentUuid");
			email = getAddressEmail(rs.getLong("addrId"), null);
		}
		rs.close();
		
		// recursive call to parent if no email yet 
		if (email == null) {
			getParentAddressEmail(parentUuid, isWorkingVersion);
		}
		
		return email;
	}

	protected void updateSysGui() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui...");
		}

		if (log.isInfoEnabled()) {
			log.info("Inserting initial sys_gui entries...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_gui";
		jdbc.executeUpdate(sqlStr);

		LinkedHashMap<String, Integer> initialSysGuis = new LinkedHashMap<String, Integer>();
		Integer initialBehaviour = -1;
		initialSysGuis.put("1130", initialBehaviour);
		initialSysGuis.put("1140", initialBehaviour);
		initialSysGuis.put("1220", initialBehaviour);
		initialSysGuis.put("1230", initialBehaviour);
		initialSysGuis.put("1240", initialBehaviour);
		initialSysGuis.put("1250", initialBehaviour);
		initialSysGuis.put("1310", initialBehaviour);
		initialSysGuis.put("1320", initialBehaviour);
		initialSysGuis.put("1350", initialBehaviour);
		initialSysGuis.put("1410", initialBehaviour);
		initialSysGuis.put("3100", initialBehaviour);
		initialSysGuis.put("3110", initialBehaviour);
		initialSysGuis.put("3120", initialBehaviour);
		initialSysGuis.put("3200", initialBehaviour);
		initialSysGuis.put("3210", initialBehaviour);
		initialSysGuis.put("3230", initialBehaviour);
		initialSysGuis.put("3240", initialBehaviour);
		initialSysGuis.put("3250", initialBehaviour);
		initialSysGuis.put("3300", initialBehaviour);
		initialSysGuis.put("3310", initialBehaviour);
		initialSysGuis.put("3320", initialBehaviour);
		initialSysGuis.put("3330", initialBehaviour);
		initialSysGuis.put("3340", initialBehaviour);
		initialSysGuis.put("3345", initialBehaviour);
		initialSysGuis.put("3350", initialBehaviour);
		initialSysGuis.put("3355", initialBehaviour);
		initialSysGuis.put("3360", initialBehaviour);
		initialSysGuis.put("3365", initialBehaviour);
		initialSysGuis.put("3370", initialBehaviour);
		initialSysGuis.put("3375", initialBehaviour);
		initialSysGuis.put("3380", initialBehaviour);
		initialSysGuis.put("3385", initialBehaviour);
		initialSysGuis.put("3400", initialBehaviour);
		initialSysGuis.put("3410", initialBehaviour);
		initialSysGuis.put("3420", initialBehaviour);
		initialSysGuis.put("3500", initialBehaviour);
		initialSysGuis.put("3515", initialBehaviour);
		initialSysGuis.put("3520", initialBehaviour);
		initialSysGuis.put("3530", initialBehaviour);
		initialSysGuis.put("3535", initialBehaviour);
		initialSysGuis.put("3555", initialBehaviour);
		initialSysGuis.put("3565", initialBehaviour);
		initialSysGuis.put("3570", initialBehaviour);
		initialSysGuis.put("5000", initialBehaviour);
		initialSysGuis.put("5020", initialBehaviour);
		initialSysGuis.put("5021", initialBehaviour);
		initialSysGuis.put("5022", initialBehaviour);
		initialSysGuis.put("5040", initialBehaviour);
		initialSysGuis.put("5040", initialBehaviour);
		initialSysGuis.put("5052", initialBehaviour);
		initialSysGuis.put("5062", initialBehaviour);
		initialSysGuis.put("5063", initialBehaviour);
		initialSysGuis.put("5069", initialBehaviour);
		initialSysGuis.put("5070", initialBehaviour);
		initialSysGuis.put("N001", initialBehaviour);
		initialSysGuis.put("N002", initialBehaviour);
		initialSysGuis.put("N003", initialBehaviour);
		initialSysGuis.put("N004", initialBehaviour);
		initialSysGuis.put("N005", initialBehaviour);
		initialSysGuis.put("N007", initialBehaviour);
		initialSysGuis.put("N009", initialBehaviour);
		initialSysGuis.put("N010", initialBehaviour);
		initialSysGuis.put("N011", initialBehaviour);
		initialSysGuis.put("N012", initialBehaviour);
		initialSysGuis.put("N013", initialBehaviour);
		initialSysGuis.put("N014", initialBehaviour);
		initialSysGuis.put("N015", initialBehaviour);
		initialSysGuis.put("N016", initialBehaviour);
		initialSysGuis.put("N017", initialBehaviour);
		initialSysGuis.put("N018", initialBehaviour);
		initialSysGuis.put("4400", initialBehaviour);
		initialSysGuis.put("4405", initialBehaviour);
		initialSysGuis.put("4410", initialBehaviour);
		initialSysGuis.put("4415", initialBehaviour);
		initialSysGuis.put("4420", initialBehaviour);
		initialSysGuis.put("4425", initialBehaviour);
		initialSysGuis.put("4435", initialBehaviour);
		initialSysGuis.put("4440", initialBehaviour);
		initialSysGuis.put("4510", initialBehaviour);
		initialSysGuis.put("4500", initialBehaviour);
		initialSysGuis.put("N019", initialBehaviour);
		initialSysGuis.put("N020", initialBehaviour);
		initialSysGuis.put("N023", initialBehaviour);
		
		Iterator<String> itr = initialSysGuis.keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			jdbc.executeUpdate("INSERT INTO sys_gui (id, gui_id, behaviour) VALUES ("
					+ getNextId() + ", '" + key + "', " + initialSysGuis.get(key) + ")");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Updating sys_gui... done");
		}
	}

	protected void updateTablesMetadata() throws Exception {
		// update all objects !
		if (log.isInfoEnabled()) {
			log.info("Updating object_metadata...");
		}
		ResultSet rs = jdbc.executeQuery("select id from t01_object");
		while (rs.next()) {
			long id = rs.getLong("id");
			long metaId = getNextId();

			jdbc.executeUpdate("INSERT INTO object_metadata (id) "	+ "VALUES (" + metaId + ");");
			jdbc.executeUpdate("UPDATE t01_object SET obj_metadata_id = " + metaId + " where id = " + id);
		}
		rs.close();
		if (log.isInfoEnabled()) {
			log.info("Updating object_metadata... done");
		}

		
		// update all addresses !
		if (log.isInfoEnabled()) {
			log.info("Updating address_metadata...");
		}
		rs = jdbc.executeQuery("select id from t02_address");
		while (rs.next()) {
			long id = rs.getLong("id");
			long metaId = getNextId();

			jdbc.executeUpdate("INSERT INTO address_metadata (id) "	+ "VALUES (" + metaId + ");");
			jdbc.executeUpdate("UPDATE t02_address SET addr_metadata_id = " + metaId + " where id = " + id);
		}
		rs.close();
		if (log.isInfoEnabled()) {
			log.info("Updating address_metadata... done");
		}
	}
	
	protected void updateComments() throws Exception {
		// update all objects !
		if (log.isInfoEnabled()) {
			log.info("Updating object_comment new line attribute...");
		}
		ResultSet rs = jdbc.executeQuery(
				"select id, obj_id from object_comment order by obj_id, create_time");
		int currLine = 1;
		long currEntityId = -1;
		while (rs.next()) {
			long id = rs.getLong("id");
			long objId = rs.getLong("obj_id");
			if (objId != currEntityId) {
				currEntityId = objId;
				currLine = 1;
			}

			jdbc.executeUpdate("UPDATE object_comment SET line = " + currLine + " where id = " + id);
			
			currLine++;
		}
		rs.close();
		if (log.isInfoEnabled()) {
			log.info("Updating object_comment... done");
		}

		
		// update all addresses !
		if (log.isInfoEnabled()) {
			log.info("Updating address_comment new line attribute...");
		}
		rs = jdbc.executeQuery(
				"select id, addr_id from address_comment order by addr_id, create_time");
		currEntityId = -1;
		while (rs.next()) {
			long id = rs.getLong("id");
			long addrId = rs.getLong("addr_id");
			if (addrId != currEntityId) {
				currEntityId = addrId;
				currLine = 1;
			}

			jdbc.executeUpdate("UPDATE address_comment SET line = " + currLine + " where id = " + id);
			
			currLine++;
		}
		rs.close();
		if (log.isInfoEnabled()) {
			log.info("Updating address_comment... done");
		}
	}
	
	protected void updateTreePath() throws Exception {
		String NODE_SEPARATOR = "|";  

		// update all objects !
		if (log.isInfoEnabled()) {
			log.info("Updating tree_path in object_node...");
		}

		// first set up map representing tree structure
		HashMap<String, String> nodeToParentMap = new HashMap<String, String>();
		ResultSet rs = jdbc.executeQuery("select obj_uuid, fk_obj_uuid from object_node");
		while (rs.next()) {
			String uuid = rs.getString("obj_uuid");
			String parentUuid = rs.getString("fk_obj_uuid");

			// check for nodes referencing itself as parent !!! (as in sh catalog !)
			if (uuid.equals(parentUuid)) {
				String msg = "\nWARN: Object node '" + uuid + "' references itself as parent ! WE MOVE THIS NODE TO TOP !!!";
				System.out.println(msg);
				if (log.isWarnEnabled()) {
					log.warn(msg);
				}

				// set id of parent to null in database and map !

				jdbc.executeUpdate("UPDATE object_node SET fk_obj_uuid = NULL " +
					"where obj_uuid = '" + uuid + "'");
				parentUuid = null;
			}
			
			nodeToParentMap.put(uuid, parentUuid);
		}
		rs.close();

		// then process all nodes and write their path !
		Iterator<String> nodeIt = nodeToParentMap.keySet().iterator();
		while (nodeIt.hasNext()) {
			String nodeUuid = nodeIt.next();
			String parentUuid = nodeToParentMap.get(nodeUuid);
			String path = "";
			
			// set up path
			ArrayList<String> pathUuids = new ArrayList<String>();
			pathUuids.add(nodeUuid);
			while (parentUuid != null) {
				// test for loops in hierarchy !!! corrupt data !
				if (pathUuids.contains(parentUuid)) {
					if (log.isWarnEnabled()) {
						log.warn("Object node '" + nodeUuid + "' contains loop in tree hierarchy !!! " +
								"Current path: '" + path + "', next parent: '" + parentUuid + "'");
						log.warn("We only write path till loop: '" + path + "'");
					}
					break;
				} else {
					pathUuids.add(parentUuid);
				}

				// insert parent at front !
				path = NODE_SEPARATOR + parentUuid + NODE_SEPARATOR + path;
				parentUuid = nodeToParentMap.get(parentUuid);
			}
			
			// write path. NOTICE: top nodes have path ''
			jdbc.executeUpdate("UPDATE object_node SET tree_path = '" + path + "' " +
				"where obj_uuid = '" + nodeUuid + "'");				
		}
		
		if (log.isInfoEnabled()) {
			log.info("Updating tree_path in object_node... done");
		}

		
		// update all addresses !
		if (log.isInfoEnabled()) {
			log.info("Updating tree_path in address_node...");
		}

		// first set up map representing tree structure
		nodeToParentMap = new HashMap<String, String>();
		rs = jdbc.executeQuery("select addr_uuid, fk_addr_uuid from address_node");
		while (rs.next()) {
			String uuid = rs.getString("addr_uuid");
			String parentUuid = rs.getString("fk_addr_uuid");

			// check for nodes referencing itself as parent !!! (as in sh catalog !)
			if (uuid.equals(parentUuid)) {
				String msg = "\nWARN: Address node '" + uuid + "' references itself as parent ! WE MOVE THIS NODE TO TOP !!!";
				System.out.println(msg);
				if (log.isWarnEnabled()) {
					log.warn(msg);
				}

				// set id of parent to null in database and map !

				jdbc.executeUpdate("UPDATE address_node SET fk_addr_uuid = NULL " +
					"where addr_uuid = '" + uuid + "'");
				parentUuid = null;
			}
			
			nodeToParentMap.put(uuid, parentUuid);
		}
		rs.close();

		// then process all nodes and write their path !
		nodeIt = nodeToParentMap.keySet().iterator();
		while (nodeIt.hasNext()) {
			String nodeUuid = nodeIt.next();
			String parentUuid = nodeToParentMap.get(nodeUuid);
			String path = "";
			
			// set up path
			ArrayList<String> pathUuids = new ArrayList<String>();
			pathUuids.add(nodeUuid);
			while (parentUuid != null) {
				// test for loops in hierarchy !!! corrupt data !
				if (pathUuids.contains(parentUuid)) {
					if (log.isWarnEnabled()) {
						log.warn("Address node '" + nodeUuid + "' contains loop in tree hierarchy !!! " +
								"Current path: '" + path + "', next parent: '" + parentUuid + "'");
						log.warn("We only write path till loop: '" + path + "'");
					}
					break;
				} else {
					pathUuids.add(parentUuid);
				}

				// insert parent at front !
				path = NODE_SEPARATOR + parentUuid + NODE_SEPARATOR + path;
				parentUuid = nodeToParentMap.get(parentUuid);
			}
			
			// write path. NOTICE: top nodes have path ''
			jdbc.executeUpdate("UPDATE address_node SET tree_path = '" + path + "' " +
				"where addr_uuid = '" + nodeUuid + "'");				
		}
		
		if (log.isInfoEnabled()) {
			log.info("Updating tree_path in address_node... done");
		}
	}
	
	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop 't01_object.avail_access_note' ...");
		}
		jdbc.getDBLogic().dropColumn("avail_access_note", "t01_object", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop 't01_object.fees' ...");
		}
		jdbc.getDBLogic().dropColumn("fees", "t01_object", jdbc);
/*
// don't add "not null", can be empty in working version !
		if (log.isInfoEnabled()) {
			log.info("Add not null constraint to 't011_obj_geo.special_base' ...");
		}
		jdbc.getDBLogic().modifyColumn("special_base", ColumnType.TEXT, "t011_obj_geo", true, jdbc);
*/
		if (log.isInfoEnabled()) {
			log.info("Drop columns in 't01_object' (moved to 'object_metadata' table)...");
		}
		jdbc.getDBLogic().dropColumn("lastexport_time", "t01_object", jdbc);
		jdbc.getDBLogic().dropColumn("expiry_time", "t01_object", jdbc);
		jdbc.getDBLogic().dropColumn("mark_deleted", "t01_object", jdbc);
		jdbc.getDBLogic().dropColumn("work_version", "t01_object", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop columns in 't02_address' (moved to 'address_metadata' table)...");
		}
		jdbc.getDBLogic().dropColumn("lastexport_time", "t02_address", jdbc);
		jdbc.getDBLogic().dropColumn("expiry_time", "t02_address", jdbc);
		jdbc.getDBLogic().dropColumn("mark_deleted", "t02_address", jdbc);
		jdbc.getDBLogic().dropColumn("work_version", "t02_address", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
