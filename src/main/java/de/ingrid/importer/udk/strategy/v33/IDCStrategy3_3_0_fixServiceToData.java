/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2016 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v33;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>New Verweistyp 'Gekoppelte Daten' in syslist 2000, migrate 'Basisdaten (3210)' to 'Gekoppelte Ressource', see INGRID33-26
 * </ul>
 */
public class IDCStrategy3_3_0_fixServiceToData extends IDCStrategyDefault3_3 {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0_fixServiceToData.class);

    /**
     * Deliver NO Version, this strategy should NOT trigger a strategy workflow (of missing former
     * versions) and can be executed on its own !
     * NOTICE: BUT may be executed in workflow (part of workflow array) !
     * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
     */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Updating sys_list...");
		updateSysList();
		System.out.println("done.");

		System.out.print("  Updating object_reference...");
		updateObjectReference();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void updateSysList() throws Exception {
		log.info("\nUpdating sys_list ...");

// ---------------------------
		int lstId = 2000;
		log.info("Insert new entry \"3600/Gekoppelte Daten\" to syslist " + lstId +	" (link type) ...");

		// german syslist
		LinkedHashMap<Integer, String> newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(3600, "Gekoppelte Daten");
		// english syslist
		LinkedHashMap<Integer, String> newSyslistMap_en = new LinkedHashMap<Integer, String>();
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(3600, "Coupled Data");

		try {
			writeNewSyslist(lstId, false, newSyslistMap_de, newSyslistMap_en, -1, -1, null, null);
		} catch (Exception ex) {
			log.warn("Problems adding new entry \"3600/Gekoppelte Daten\" to syslist " + lstId + " (already there !?), we ignore !", ex);
		}

		log.info("Updating sys_list... done\n");
	}

	private void updateObjectReference() throws Exception {
		log.info("\nUpdating object_reference...");

		final int CLASS_DATA = 1; // Geo-Information/Karte
		final int CLASS_SERVICE = 3; // Geodatendienst

		final int REF_TYPE_BASISDATEN = 3210;
		final int REF_TYPE_GEKOPPELTE_DATEN = 3600;
		final String REF_NAME_GEKOPPELTE_DATEN = "Gekoppelte Daten";

		// Die Verweise von Dienstobjekten der Klasse 3, die auf ein Objekt der Klasse 1 (Geoinformation/-karte) verweisen
		// und den Typ Basisdaten (3210) besitzen, sollen auf den Typ "Gekoppelte Resource" (3600) gesetzt werden.
		// Hintergrund: Es wurde ein neuer Verweistyp (3600) eingeführt, der die gekoppelten Daten zu einem Dienst beschreibt.
		// Vorher wurde der Typ Basisdaten verwendet, welcher nun jedoch migriert werden muss.
		// s. INGRID33-26

		log.info("Change 'Basisdaten' to '" + REF_NAME_GEKOPPELTE_DATEN + "' in 'Service to Data' references (in WORKING and PUBLISHED version of Service !) ...");

		PreparedStatement psUpdate = jdbc.prepareStatement(
				"UPDATE object_reference SET " +
				"special_ref = " + REF_TYPE_GEKOPPELTE_DATEN + ", " +
				"special_name = '" + REF_NAME_GEKOPPELTE_DATEN + "' " +
				"WHERE id = ?");

		Statement st = jdbc.createStatement();
		// Fetch 'Service to Data' references.
		// NOTICE: This fetches WORKING and PUBLISHED version of the service !
		ResultSet rs = jdbc.executeQuery(
				"SELECT oRef.id as refId, " +
				" objFrom.id as fromId, objFrom.obj_uuid as fromUuid, " +
				" objTo.id as toId, objTo.obj_uuid as toUuid " +
				"FROM object_reference oRef, " +
				" t01_object objFrom, " +
				" t01_object objTo " +
				"WHERE " +
				" oRef.special_ref = " + REF_TYPE_BASISDATEN +
				" AND oRef.obj_from_id = objFrom.id " +
				" AND oRef.obj_to_uuid = objTo.obj_uuid " +
				" AND objFrom.obj_class = " + CLASS_SERVICE +
				" AND objTo.obj_class = " + CLASS_DATA +
				" order by fromUuid, toUuid"
				, st);

		int numUpdated = 0;
		while (rs.next()) {
			// NOTICE: id of object_reference is fetched MULTIPLE times, e.g. when 
			// TO-Service-UUID has working version, then both versions are fetched via UUID
			long refId = rs.getLong("refId");
			long fromId = rs.getLong("fromId");
			String fromUuid = rs.getString("fromUuid");
			long toId = rs.getLong("toId");
			String toUuid = rs.getString("toUuid");

			// UPDATE REFERENCE
			log.info("We set type '" + REF_NAME_GEKOPPELTE_DATEN + "' (" + REF_TYPE_GEKOPPELTE_DATEN +
					") in object_reference [Service(uuid:" + fromUuid + "/id:" + fromId + ") -> Data(uuid:" + toUuid + "/id:" + toId + ")]");

			psUpdate.setLong(1, refId);
			psUpdate.executeUpdate();

			numUpdated++;
		}
		rs.close();
		st.close();
		psUpdate.close();

		log.info("Changed " + numUpdated + " object_references 'Service to Data' from type 'Basisdaten' (" + REF_TYPE_BASISDATEN + ") " +
				"to type '" + REF_NAME_GEKOPPELTE_DATEN + "' (" + REF_TYPE_GEKOPPELTE_DATEN + ")");  
		log.info("Updating object_reference... done\n");		
	}
}
