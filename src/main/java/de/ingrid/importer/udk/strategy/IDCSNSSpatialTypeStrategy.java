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

	private static final String MY_VERSION = VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE;

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

		// Get all geothesaurus entries (id and nativekey)
		// Extended query so we can update the index
		String sql = "select distinct objNode.id as objNodeId, objNode.obj_id as objWorkId, obj.id as objId, " +
		"spatialRefVal.id as spatialRefValId, spatialRefVal.nativekey as spatialRefNativekey, spatialRef.id as spatialRefId, " +
		"spatialRefSNS.sns_id as spatialRefSNSId " +
		"from spatial_ref_value spatialRefVal, spatial_reference spatialRef, spatial_ref_sns spatialRefSNS, t01_object obj, object_node objNode " +
		"where spatialRefVal.id = spatialRef.spatial_ref_id " +
		"and spatialRefSNS.id = spatialRefVal.spatial_ref_sns_id " +
		"and spatialRef.obj_id = obj.id " +
		"and obj.obj_uuid = objNode.obj_uuid " +
		"and spatialRefVal.type = 'G'";

		// we track written data in hash maps to avoid multiple writing for same spatial reference values
		HashMap<Long, Boolean> processedSpatialRefIds = new HashMap<Long,Boolean>();

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objWorkId = rs.getLong("objWorkId");
			long objId = rs.getLong("objId");
			long id = rs.getLong("spatialRefValId");
			long spatialRefId = rs.getLong("spatialRefId");
			String spatialRefSNSId = rs.getString("spatialRefSNSId");
			String nativeKey = rs.getString("spatialRefNativekey");
			String type = "";

			// convert & write values if not written yet !
			if (!processedSpatialRefIds.containsKey(spatialRefId)) {
				// Extract the topic type from the native key.
				// This is problematic since the ags/rs native keys are NOT unique!
				type = getSNSTopicTypeFor(spatialRefSNSId, nativeKey);

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
		jdbc.getDBLogic().addColumn("topic_type", ColumnType.VARCHAR50, "spatial_ref_value", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	// Determine the SNS Topic Type (use2Type, use4Type, ...) from the given snsId and nativeKey.
	// The SNS ID is checked first for 'KREIS', 'GEMEINDE', ...
	// If no type could be constructed from the SNS ID, the nativekey is analyzed. 
	private String getSNSTopicTypeFor(String snsId, String nativeKey) {
		// First try to determine the type from the given sns id
		if (snsId.startsWith("BUNDESLAND")) {
			return "use2Type";

		} else if (snsId.startsWith("KREIS")) {
			return "use4Type";

		} else if (snsId.startsWith("GEMEINDE")) {
			return "use6Type";
		}

		// Try to determine the type from the given rs nativeKey
		if (nativeKey.endsWith("00000000")) {
			return "nationType";
		} else if (nativeKey.endsWith("000000")) {
			return "use2Type";
		} else if (nativeKey.endsWith("00000")) {
			return "use3Type";
		} else if (nativeKey.endsWith("000")) {
			return "use4Type";
		} else if (nativeKey.length() == 8) {
			return "use6Type";
		}

		// Could not determine native key
		log.warn("Could not determine type for SNS Topic with id '"+snsId+"' and nativekey: '"+nativeKey+"'");
		return null;
	}
}
