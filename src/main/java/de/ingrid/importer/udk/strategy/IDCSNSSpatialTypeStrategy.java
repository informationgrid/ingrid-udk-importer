/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.ResultSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author michael
 * 
 */
public class IDCSNSSpatialTypeStrategy extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCSNSSpatialTypeStrategy.class);

	public String getIDCVersion() {
		// no version ! keep current version when adding sns spatial types !
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);
		// FIRST EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		System.out.print("  Updating spatial_ref_value...");
		updateSpatialRefValue();
		System.out.println("done.");
/*
// Nothing to cleanup yet
		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");
*/
		// THEN PERFORM DATA MANIPULATIONS !
		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	protected void updateSpatialRefValue() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating spatial_ref_value...");
		}

		// Get all entries (id and nativekey)
		String sql = "select id, nativekey from spatial_ref_value;";

		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			long id = rs.getLong("id");
			String nativeKey = rs.getString("nativekey");
			String type = "";

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

/*
// Nothing to cleanup yet	
	protected void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
*/
}
