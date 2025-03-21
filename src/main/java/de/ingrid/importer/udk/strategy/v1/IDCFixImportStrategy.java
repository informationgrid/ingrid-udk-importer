/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
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
package de.ingrid.importer.udk.strategy.v1;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * Single Strategy for fixing former importer bugs on first imported catalogues (bw, sh)
 * - multiple referenced free spatial ref values (from different objects)
 * - free spatial ref value not written into search index
 * @author martin
 */
public class IDCFixImportStrategy extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCFixImportStrategy.class);

	/**
	 * Write NO Version, this strategy should be executed on its own on chosen catalogues
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Fixing spatial_ref_value: all free spatial refs for single object only / also update search index...");
		fixSpatialRefValue();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Fix finished successfully.");
	}

	protected void fixSpatialRefValue() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Fixing spatial_ref_value...");
		}

		// all free spatial_ref_values referenced multiple times ! 
		String sql = "SELECT distinct val.id, val.name_value, val.nativekey, val.x1, val.y1, val.x2, val.y2 " +
			"FROM " +
			"t01_object obj1, t01_object obj2, " +
			"spatial_reference ref1, spatial_reference ref2, " +
			"spatial_ref_value val " +
			"WHERE " +
			"obj1.id = ref1.obj_id " +
			"and obj2.id = ref2.obj_id " +
			"and ref1.id != ref2.id " +
			"and ref1.spatial_ref_id = ref2.spatial_ref_id " +
			"and ref1.spatial_ref_id = val.id " +
			"and val.type = 'F' " +
			"order by val.name_value";

		// according spatial_references and nodes
		String psSql = "SELECT distinct spRef.id, " +
			"objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, objNode.obj_uuid " +
			"FROM spatial_reference spRef, t01_object obj, object_node objNode " +
			"WHERE " +
			"spRef.spatial_ref_id = ? " +
			"and spRef.obj_id = obj.id " +
			"and obj.obj_uuid = objNode.obj_uuid";
		PreparedStatement psSpRefs = jdbc.prepareStatement(psSql);

		// insert spatial_ref_value
		psSql = "INSERT INTO spatial_ref_value " +
			"(id, type, name_key, name_value, nativekey, x1, y1, x2, y2) "
			+ "VALUES "
			+ "(?, 'F', -1, ?, ?, ?, ?, ?, ?)";
		PreparedStatement psInsertSpRefValue = jdbc.prepareStatement(psSql);

		// update spatial_reference
		psSql = "UPDATE spatial_reference SET spatial_ref_id = ? WHERE id = ?";
		PreparedStatement psUpdateSpRef = jdbc.prepareStatement(psSql);

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			long spRefValId = rs.getLong("id");
			String spRefValNameValue = rs.getString("name_value");
			String spRefValNativeKey = rs.getString("nativekey");
			Double spRefValX1 = (Double) rs.getObject("x1");
			Double spRefValY1 = (Double) rs.getObject("y1");
			Double spRefValX2 = (Double) rs.getObject("x2");
			Double spRefValY2 = (Double) rs.getObject("y2");

			// read all spatial_references referencing spatial_ref_value
			psSpRefs.setLong(1, spRefValId); // spatial_reference.spatial_ref_id
			ResultSet rsSpRefs = psSpRefs.executeQuery();
			boolean firstOne = true; 
			while (rsSpRefs.next()) {
				// keep first one unchanged !
				if (firstOne) {
					firstOne = false;
					continue;
				}

				long spRefId = rsSpRefs.getLong("id");
				long objNodeId = rsSpRefs.getLong("objNodeId");
				String objUuid = rsSpRefs.getString("obj_uuid");
				long objWorkId = rsSpRefs.getLong("objWorkId");
				long objId = rsSpRefs.getLong("objId");
				boolean isWorkingVersion = (objWorkId == objId); 

				// copy spatial_ref_value
				long newSpRefValId = getNextId();
				int cnt = 1;
				psInsertSpRefValue.setLong(cnt++, newSpRefValId); // spatial_ref_value.id
				psInsertSpRefValue.setString(cnt++, spRefValNameValue); // spatial_ref_value.name_value
				psInsertSpRefValue.setString(cnt++, spRefValNativeKey); // spatial_ref_value.nativekey
				JDBCHelper.addDouble(psInsertSpRefValue, cnt++, spRefValX1); // spatial_ref_value.x1
				JDBCHelper.addDouble(psInsertSpRefValue, cnt++, spRefValY1); // spatial_ref_value.y1
				JDBCHelper.addDouble(psInsertSpRefValue, cnt++, spRefValX2); // spatial_ref_value.x2
				JDBCHelper.addDouble(psInsertSpRefValue, cnt++, spRefValY2); // spatial_ref_value.y2
				psInsertSpRefValue.executeUpdate();

				// update spatial_reference
				cnt = 1;
				psUpdateSpRef.setLong(cnt++, newSpRefValId); // spatial_reference.spatial_ref_id
				psUpdateSpRef.setLong(cnt++, spRefId); // spatial_reference.id
				psUpdateSpRef.executeUpdate();

				if (log.isDebugEnabled()) {
					log.debug("Updated spatial reference of object(" + objUuid + 
						") to new separated ref value '" + spRefValNameValue + "'");
				}

				// extend object index (index contains only data of working versions !)
				if (isWorkingVersion) {
					JDBCHelper.updateObjectIndex(objNodeId, spRefValNameValue, jdbc); // spatial_ref_value.name_value				
					if (log.isDebugEnabled()) {
						log.debug("Also updated according search index objNodeId(" + objNodeId + ") " +
							"with '" + spRefValNameValue + "'");
					}
				}
			}
			rsSpRefs.close();
		}
		rs.close();
		st.close();
		
		if (log.isInfoEnabled()) {
			log.info("Fixing spatial_ref_value... done");
		}
	}
}
