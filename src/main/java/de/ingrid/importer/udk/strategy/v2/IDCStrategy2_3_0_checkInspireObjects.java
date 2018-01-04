/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
/**
 * 
 */
package de.ingrid.importer.udk.strategy.v2;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * 2.3.0 = SWITCH OF STRATEGY VERSION ! Strategy Version (first two digits) correlates now with InGrid project version !!!
 * <br>
 * InGrid 2.3:<br>
 * - Protokollierung aller NICHT INSPIRE konformen Objekte (fehlende Daten) !
 * <br>WRITES NO VERSION (can be executed on its own) !
 */
public class IDCStrategy2_3_0_checkInspireObjects extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy2_3_0_checkInspireObjects.class);

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

		System.out.println("  Check objects with INSPIRE themes for INSPIRE compatibility (-> PROTOCOL of objects with missing data)...");
		checkInspireObjectsConformity();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Check finished successfully.");
	}

	protected void checkInspireObjectsConformity() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Check objects with INSPIRE themes for INSPIRE compatibility (-> PROTOCOL of objects with missing data)...");
		}

		String sql = "select " +
			"oNode.id as oNodeId, oNode.obj_uuid, oNode.obj_id, oNode.obj_id_published, " + // object node
			"obj.obj_name, " + // object
			"objSearchTermValue.entry_id as inspireKey, " + // INSPIRE theme
			"objGeo.referencesystem_key, " + // Raumbezugssystem: mandatory
			"objFormat.format_key, objFormat.format_value, objFormat.ver as formatVersion, " + // Datenformat: Name + Version: mandatory
			"objAdr.type as adrRefKey, objAdr.special_ref as adrRefListId, objAdr.adr_uuid " + // Adressen: "Auskunft" + "Datenverantwortung" : mandatory
			"from " +
			"object_node oNode " +
			// always join "working version" ! equals published version, if no working version
			"join t01_object obj on (oNode.obj_id = obj.id) " +
			"join searchterm_obj objSearchTerm on (obj.id = objSearchTerm.obj_id) " +
			"join searchterm_value objSearchTermValue on (objSearchTerm.searchterm_id = objSearchTermValue.id) " +
			"left join t011_obj_geo objGeo on (obj.id = objGeo.obj_id) " +
			"left join t0110_avail_format objFormat on (obj.id = objFormat.obj_id) " +
			"left join t012_obj_adr objAdr on (obj.id = objAdr.obj_id) " +
			"where " +
			"objSearchTermValue.type = 'I' " +
			"and objSearchTermValue.entry_id != 99999 " + // "Kein INSPIRE Thema"
			"order by obj_id";

		HelperStatistics stats = new HelperStatistics();
		HelperObject currentObj = null;

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long nextObjId = rs.getLong("obj_id");
			
			// check whether all data of an object is read, then process object !
			boolean objChange = false;
			if (currentObj != null && currentObj.objId != nextObjId) {
				// object changed, process finished object
				objChange = true;
				processObject(currentObj, stats);
			}
			
			if (currentObj == null || objChange) {
				// set up next object
				currentObj = new HelperObject(rs.getLong("oNodeId"), rs.getString("obj_uuid"),
					nextObjId, rs.getLong("obj_id_published"), rs.getString("obj_name"));
			}

			// pass read stuff to object
			currentObj.addInspireTheme(rs.getInt("inspireKey"));
			currentObj.addReferencesystem(rs.getInt("referencesystem_key"));
			currentObj.addDataFormat(rs.getInt("format_key"), rs.getString("format_value"), rs.getString("formatVersion"));
			currentObj.addAddress(rs.getInt("adrRefKey"), rs.getInt("adrRefListId"), rs.getString("adr_uuid"));
		}
		// also process last object ! not done in loop due to end of loop !
		if (currentObj != null) {
			processObject(currentObj, stats);			
		}

		rs.close();
		st.close();

		// Protocol also to System.out !

		String msg = "\nChecked " + stats.numInspire + " INSPIRE objects on missing data.";
		System.out.println("\n" + msg + " See also log file.");
		log.info(msg);
		
		if (stats.objsMissingData.size() > 0) {
			msg = "The following " + stats.objsMissingData.size() +
				" objects are not INSPIRE conform due to missing data. Please edit manually and publish again !\n\n" +
				stats.getObjMissingDataAsString();
			System.out.println("\n" + msg + "See also log file (WARN).");
			log.warn(msg);
		} else {
			msg = "No INSPIRE objects with missing data found !";
			System.out.println("\n" + msg);
			log.info(msg);
		}

		if (log.isInfoEnabled()) {
			log.info("Check objects with INSPIRE themes for INSPIRE compatibility (-> PROTOCOL of objects with missing data)... done");
		}
	}

	/** Process the given object. Pass stats for counting. */
	protected void processObject(HelperObject obj, HelperStatistics stats) throws Exception {
		stats.numInspire++;
		
		if (!obj.isInspireConform()) {
			stats.addObjMissingData(obj);
		}

		// if published version different from working version WARN !
		if (obj.isPublished() && obj.hasWorkingVersion()) {
			String msg = "!!! object '" + obj.uuid + ":" + obj.name + "' has separate WORKING VERSION, WE ONLY CHECK WORKING VERSION !!!";
			System.out.println("\n" + msg);
			log.warn(msg);
		}
	}

	/** Helper class encapsulating statistics */
	class HelperStatistics {
		int numInspire = 0;
		ArrayList<HelperObject> objsMissingData = new ArrayList<HelperObject>();

		void addObjMissingData(HelperObject obj) {
			objsMissingData.add(obj);				
		}
		String getObjMissingDataAsString() {
			String output = "";
			for (HelperObject obj : objsMissingData) {
				output = output + obj.uuid + ":" + obj.name + " (INSPIRE themes: " +
					obj.getInspireKeysAsString() + ")\n";
			}
			return output;
		}
	}
	/** Helper class encapsulating all needed data of an object for INSPIRE compatibility */
	class HelperObject {
		long objNodeId;
		String uuid;
		long objId;
		long objIdPublished;
		String name;		
		ArrayList<Integer> inspireKeys;
		ArrayList<Integer> refsystemKeys;
		ArrayList<Integer> dataformatKeys;
		ArrayList<Integer> addrRefKeys;

		HelperObject(long objNodeId, String uuid, long objId, long objIdPublished,
				String name) {
			this.objNodeId = objNodeId;
			this.uuid = uuid;
			this.objId = objId;
			this.objIdPublished = objIdPublished;
			this.name = name;
			this.inspireKeys = new ArrayList<Integer>();
			this.refsystemKeys = new ArrayList<Integer>();
			this.dataformatKeys = new ArrayList<Integer>();
			this.addrRefKeys = new ArrayList<Integer>();
		}
		void addInspireTheme(int inspireKey) {
			// NOTICE: key should never be 0 (NULL) due to inner join fetching in select but we check 0 to be sure ...
			if (inspireKey != 0 && !inspireKeys.contains(inspireKey)) {
				inspireKeys.add(inspireKey);				
			}
		}
		void addReferencesystem(int refKey) {
			// key may be 0 (NULL) due to outer join fetching in select
			if (refKey != 0 && !refsystemKeys.contains(refKey)) {
				refsystemKeys.add(refKey);				
			}
		}
		void addDataFormat(int formatKey, String formatValue, String formatVersion) {
			// key may be 0 (NULL) due to outer join fetching in select
			if (formatKey != 0 && hasContent(formatValue) && hasContent(formatVersion)) {
				if (!dataformatKeys.contains(formatKey)) {
					dataformatKeys.add(formatKey);				
				}
			}
		}
		void addAddress(int addrRefKey, int addrRefListId, String addrUuid) {
			// key may be 0 (NULL) due to outer join fetching in select
			if (addrRefKey != 0 && addrRefListId == 505 && hasContent(addrUuid)) {
				if (!addrRefKeys.contains(addrRefKey)) {
					addrRefKeys.add(addrRefKey);				
				}
			}
		}
		boolean isInspireConform() {
			// check referencesystem
			if (refsystemKeys.size() == 0) {
				return false;
			}
			if (dataformatKeys.size() == 0) {
				return false;
			}
			if (!addrRefKeys.contains(2) || // Datenverantwortung
				!addrRefKeys.contains(7)) { // Auswertung
				return false;
			}
			return true;
		}
		String getInspireKeysAsString() {
			String inspKeys = "";
			for (Integer key : inspireKeys) {
				inspKeys = inspKeys + key + ",";
			}
			return inspKeys;
		}
		boolean isPublished() {
			// if obj_id_published = null then jdbc returns 0 !
			if (objIdPublished == 0) {
				return false;
			}
			return true;
		}
		boolean hasWorkingVersion() {
			// NOTICE: objId always set, never 0 (null) !
			if (objId == objIdPublished) {
				return false;
			}
			return true;
		}
	}
	
	boolean hasContent(String str) {
		return (str != null && str.trim().length() > 0);
	}
}
