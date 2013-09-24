/**
 * 
 */
package de.ingrid.importer.udk.strategy.v34;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

/**
 * <p>
 * Changes InGrid 3.4<p>
 * <ul>
 *   <li>Remove duplicated Orig IDs in copies of objects, see INGRID-2299
 * </ul>
 */
public class IDCStrategy3_4_0_fixOrigId extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_4_0_fixOrigId.class);

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

		// PERFORM DATA MANIPULATIONS !
		// ----------------------------

		System.out.print("  Fixing t01_object.org_obj_id...");
		fixObjectOrigId();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Fix finished successfully. See log.log for further details.");
	}

	private void fixObjectOrigId() throws Exception {
		log.info("\nFixing t01_object.org_obj_id...");

		PreparedStatement psUpdate = jdbc.prepareStatement(
				"UPDATE t01_object SET " +
				"org_obj_id = NULL " +
				"WHERE id = ?");

		PreparedStatement psSelectObjs = jdbc.prepareStatement(
				"SELECT id, obj_uuid, obj_name, create_time " +
				"FROM t01_object " +
				"WHERE org_obj_id = ? " +
				"ORDER BY create_time");

		// Fetch all OrigIds then process every ID and according objects
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(
				"SELECT distinct org_obj_id " +
				"FROM t01_object " +
				"ORDER BY org_obj_id"
				, st);

		int numUpdated = 0;
		while (rs.next()) {
			String orgId = rs.getString("org_obj_id");

			// fetch all objects ordered by creation date (first one keeps origId, is the oldest one)
			psSelectObjs.setString(1, orgId);
			ResultSet rsSelectObjs = psSelectObjs.executeQuery();

			boolean firstObject = true;
			while (rsSelectObjs.next()) {
				long objId = rsSelectObjs.getLong("id");
				String objUuid = rsSelectObjs.getString("obj_uuid");
				String objName = rsSelectObjs.getString("obj_name");
				String objCreateTime = rsSelectObjs.getString("create_time");

				if (firstObject) {
					firstObject = false;
					log.info("KEEP org_obj_id '" + orgId + "' in OBJECT [id:" + objId + "/uuid:" + objUuid +
						"/name:'" + objName + "'/createTime:" + objCreateTime + ") !");
					continue;
				}

				log.info("REMOVE org_obj_id '" + orgId + "' from OBJECT [id:" + objId + "/uuid:" + objUuid +
					"/name:'" + objName + "'/createTime:" + objCreateTime + ") !");

				psUpdate.setLong(1, objId);
				psUpdate.executeUpdate();

				numUpdated++;
			}

			rsSelectObjs.close();
		}
		rs.close();
		st.close();
		psSelectObjs.close();
		psUpdate.close();

		log.info("Removed " + numUpdated + " Original Ids from objects");  
		log.info("Fixing t01_object.org_obj_id... done\n");		
	}
}

