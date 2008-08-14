/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;

/**
 * @author michael
 * 
 */
public class IDCSNSSpatialTypeStrategy extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCSNSSpatialTypeStrategy.class);

	public String getIDCVersion() {
		return VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);
		// FIRST EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		System.out.print("  Updating spatial_ref_value...");
		updateSpatialRefValue();
		System.out.println("done.");

		// Nothing to cleanup yet

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void updateSpatialRefValue() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating spatial_ref_value...");
		}

		// Get all entries (id and nativekey)
//		String sql = "select id, nativekey from spatial_ref_value;";
		// Extended query so we can update the index
		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, " +
		"spatialRefVal.id as spatialRefValId, spatialRefVal.nativekey as spatialRefNativekey, spatialRefVal.type as spatialRefValType, " +
		"spatialRef.id as spatialRefId " +
		"from spatial_ref_value spatialRefVal, spatial_reference spatialRef, t01_object obj, object_node objNode " +
		"where spatialRefVal.id = spatialRef.spatial_ref_id " +
		"and spatialRef.obj_id = obj.id " +
		"and obj.obj_uuid = objNode.obj_uuid";

		// we track written data in hash maps to avoid multiple writing for same spatial reference values
		HashMap<Long, Boolean> processedSpatialRefIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objWorkId = rs.getLong("objWorkId");
			long objId = rs.getLong("objId");
			long id = rs.getLong("spatialRefValId");
			long spatialRefId = rs.getLong("spatialRefId");
			String nativeKey = rs.getString("spatialRefNativekey");
			boolean isSNSTopic = rs.getString("spatialRefValType").equalsIgnoreCase("G");
			String type = "";

			// convert & write values if not written yet !
			if (isSNSTopic && !processedSpatialRefIds.containsKey(spatialRefId)) {
				// Extract the topic type from the native key.
				// This is problematic since the ags/rs native keys are NOT unique!
				if (nativeKey.endsWith("00000000")) {
					type = "nationType";
				} else if (nativeKey.endsWith("000000")) {
					type = "use2Type";
				} else if (nativeKey.endsWith("00000")) {
					type = "use3Type";
				} else if (nativeKey.endsWith("000")) {
					type = "use4Type";
				} else if (nativeKey.length() == 8) {
					type = "use6Type";
				}

				if (log.isDebugEnabled()) {
					log.debug("Updating spatial_ref_value entry: ["+id+", "+nativeKey+", "+type+"]");
				}
	
				String sqlStr = "update spatial_ref_value set topic_type = '"+type+"' where id = "+id;
				jdbc.executeUpdate(sqlStr);

				processedSpatialRefIds.put(spatialRefId, true);

				// extend object index (index contains only data of working versions !)
				if (objWorkId == objId) {
					JDBCHelper.updateObjectIndex(objNodeId, type, jdbc); // spatial_ref_value.topic_type
				}
			}
		}
		rs.close();
		
		if (log.isInfoEnabled()) {
			log.info("Updating spatial_ref_value... done");
		}
	}

	protected void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add column 'topic_type' to table 'spatial_ref_value'...");
		}
		jdbc.getDBLogic().addColumn("topic_type", ColumnType.VARCHAR50, "spatial_ref_value", false, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}
}
