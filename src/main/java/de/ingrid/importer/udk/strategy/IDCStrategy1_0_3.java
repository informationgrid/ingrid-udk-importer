/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_3 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_3.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_103;

	private int defaultSyslist6000EntryId = 3;
	private String defaultSyslist6000EntryValue =
		"nicht evaluiert: Die Konformität der Datenquelle wurde noch nicht evaluiert";

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

	private LinkedHashMap<Integer, String> newSyslist5200 = new LinkedHashMap<Integer, String>(); 
	private HashMap<Integer, Integer> oldToNewKeySyslist5200 = new HashMap<Integer, Integer>(); 

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

		// Updating of HI/LO table not necessary anymore ! is checked and updated when fetching next id
		// via getNextId() ...

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
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
			log.info("Create table 'object_conformity'...");
		}
		jdbc.getDBLogic().createTableObjectConformity(jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add column 'datasource_uuid' to 'table t011_obj_geo'...");
		}
		// don't add "not null", can be empty in working version !
		jdbc.getDBLogic().addColumn("datasource_uuid", ColumnType.TEXT, "t011_obj_geo", false, jdbc);
		
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
			+ getNextId() + ", " + lstId + ", 1, 'de', 'konform: Die Datenquelle ist vollständig konform zur zitierten Spezifikation', 0, 'N');");
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
			+ getNextId() + ", " + lstId + ", 2, 'de', 'nicht konform: Die Datenquelle ist nicht konform zur zitierten Spezifikation', 0, 'N');");
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

		newSyslist5200.put(101, "Catalogue viewer");
		newSyslist5200.put(102, "Geographic viewer");
		newSyslist5200.put(103, "Geographic spreadsheet viewer");
		newSyslist5200.put(104, "Service editor");
		newSyslist5200.put(105, "Chain definition editor");
		newSyslist5200.put(106, "Workflow enactment manager");
		newSyslist5200.put(107, "Geographic feature editor");
		newSyslist5200.put(108, "Geographic symbol editor");
		newSyslist5200.put(109, "Feature generalization editor");
		newSyslist5200.put(110, "Geographic data-structure viewer");
		newSyslist5200.put(201, "Feature access service");
		newSyslist5200.put(202, "Map access service");
		newSyslist5200.put(203, "Coverage access service");
		newSyslist5200.put(204, "Sensor description service");
		newSyslist5200.put(205, "Product access service");
		newSyslist5200.put(206, "Feature type service");
		newSyslist5200.put(207, "Catalogue service");
		newSyslist5200.put(208, "Registry Service");
		newSyslist5200.put(209, "Gazetteer service");
		newSyslist5200.put(210, "Order handling service");
		newSyslist5200.put(211, "Standing order service");
		newSyslist5200.put(301, "Chain definition service");
		newSyslist5200.put(302, "Workflow enactment service");
		newSyslist5200.put(303, "Subscription service");
		newSyslist5200.put(401, "Coordinate conversion service");
		newSyslist5200.put(402, "Coordinate transformation service");
		newSyslist5200.put(403, "Coverage/vector conversion service");
		newSyslist5200.put(404, "Image coordinate conversion service");
		newSyslist5200.put(405, "Rectification service");
		newSyslist5200.put(406, "Orthorectification service");
		newSyslist5200.put(407, "Sensor geometry model adjustment service");
		newSyslist5200.put(408, "Image geometry model conversion service");
		newSyslist5200.put(409, "Subsetting service");
		newSyslist5200.put(410, "Sampling service");
		newSyslist5200.put(411, "Tiling change service");
		newSyslist5200.put(412, "Dimension measurement service");
		newSyslist5200.put(413, "Feature manipulation services");
		newSyslist5200.put(414, "Feature matching service");
		newSyslist5200.put(415, "Feature generalization service");
		newSyslist5200.put(416, "Route determination service");
		newSyslist5200.put(417, "Positioning service");
		newSyslist5200.put(418, "Proximity analysis service");
		newSyslist5200.put(501, "Geoparameter calculation service");
		newSyslist5200.put(502, "Thematic classification service");
		newSyslist5200.put(503, "Feature generalization service");
		newSyslist5200.put(504, "Subsetting service");
		newSyslist5200.put(505, "Spatial counting service");
		newSyslist5200.put(506, "Change detection service");
		newSyslist5200.put(507, "Geographic information extraction services");
		newSyslist5200.put(508, "Image processing service");
		newSyslist5200.put(509, "Reduced resolution generation service");
		newSyslist5200.put(510, "Image Manipulation Services");
		newSyslist5200.put(511, "Image understanding services");
		newSyslist5200.put(512, "Image synthesis services");
		newSyslist5200.put(513, "Multi-band image manipulation");
		newSyslist5200.put(514, "Object detection service");
		newSyslist5200.put(515, "Geoparsing service");
		newSyslist5200.put(516, "Geocoding service");
		newSyslist5200.put(601, "Temporal reference system transformation service");
		newSyslist5200.put(602, "Subsetting service");
		newSyslist5200.put(603, "Sampling service");
		newSyslist5200.put(604, "Temporal proximity analysis service");
		newSyslist5200.put(701, "Statistical calculation service");
		newSyslist5200.put(702, "Geographic annotation services");
		newSyslist5200.put(801, "Encoding service");
		newSyslist5200.put(802, "Transfer service");
		newSyslist5200.put(803, "Geographic compression service");
		newSyslist5200.put(804, "Geographic format conversion service");
		newSyslist5200.put(805, "Messaging service");
		newSyslist5200.put(806, "Remote file and executable management");
		newSyslist5200.put(901, "Non Geographic Service");

		oldToNewKeySyslist5200.put(1, 207);
		oldToNewKeySyslist5200.put(2, 202);
		oldToNewKeySyslist5200.put(3, 201);
		oldToNewKeySyslist5200.put(6, 901);

		itr = newSyslist5200.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + lstId + ", " + key + ", 'de', '" + newSyslist5200.get(key) + "', 0, 'N')");
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

			String defaultSpecification = "INSPIRE-Richtlinie";

			jdbc.executeUpdate("INSERT INTO object_conformity (id, obj_id, line, specification, degree_key, degree_value) " +
				"VALUES (" + getNextId() + ", " + objId + ", 1, '" + defaultSpecification + "', "
				+ defaultSyslist6000EntryId + ", '" + defaultSyslist6000EntryValue + "');");
			
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
			log.info("Migrate WFS name_key, name_value to new syslist 5120 (WFS Operations) ...");
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
				String syslist5200EntryValue = newSyslist5200.get(syslist5200EntryId);

				jdbc.executeUpdate("INSERT INTO t011_obj_serv_type (id, obj_serv_id, line, serv_type_key, serv_type_value) "
					+ "VALUES (" + getNextId() + ", " + objServId + ", 1, " + syslist5200EntryId + ", '"
					+ syslist5200EntryValue	+ "');");
				
				processedObjIds.put(objId, true);

				// extend object index (index contains only data of working versions !)
				if (objWorkId == objId) {
					JDBCHelper.updateObjectIndex(objNodeId, syslist5200EntryValue, jdbc); // t011_obj_serv_type.serv_type_value
				}
			}
		}
		rs.close();

		if (log.isInfoEnabled()) {
			log.info("Updating t011_obj_serv_type... done");
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
			log.info("Cleaning up datastructure... done");
		}
	}
}
