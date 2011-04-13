/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Changes InGrid 3.0:<p>
 * - fix data "Geoinformation/Karte -> Erfassungsgrad" (t011_obj_geo.rec_grade) from Commission to Omission, see https://dev.wemove.com/jira/browse/INGRID23-147
 */
public class IDCStrategy3_0_0_fixErfassungsgrad extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_0_0_fixErfassungsgrad.class);

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

		System.out.print("  Fix data \"Geoinformation/Karte -> Erfassungsgrad\" from Commission to Omission...");
		fixErfassungsgrad();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void fixErfassungsgrad() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Fix \"Erfassungsgrad\" (t011_obj_geo.rec_grade) from Commission to Omission...");
		}

		// sql
		String sql = "select * from t011_obj_geo where rec_grade IS NOT NULL";

		PreparedStatement pS = jdbc.prepareStatement("UPDATE t011_obj_geo SET rec_grade = ? where id = ?");
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numFixed = 0;
		while (rs.next()) {
			long id = rs.getLong("id");
			long objId = rs.getLong("obj_id");
			double oldRecGrade = rs.getDouble("rec_grade");
			double newRecGrade = 100.0 - oldRecGrade;
			if (newRecGrade < 0.0) {
				newRecGrade = 0.0;
				log.warn("New value " + newRecGrade + " < 0, we set new value to 0.0.");
			}
			if (newRecGrade > 100.0) {
				newRecGrade = 100.0;
				log.warn("New value " + newRecGrade + " > 100, we set new value to 100.0.");
			}

			try {
				// round 2 decimals after digit
				newRecGrade = new BigDecimal(newRecGrade).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			} catch (Exception ex) {
				log.error("Problems rounding " + newRecGrade + " to 2 decimals after digit, we keep unrounded value." + ex);
			}

			pS.setDouble(1, newRecGrade);
			pS.setLong(2, id);
			int numUpdated = pS.executeUpdate();
			if (log.isDebugEnabled()) {
				log.debug("Fixed t011_obj_geo.rec_grade from " + oldRecGrade + " to " + newRecGrade +
					" (" + numUpdated + " row(s), objectId: " + objId + ")");
			}
			numFixed++;
		}

		pS.close();
		rs.close();
		st.close();

		if (log.isInfoEnabled()) {
			log.info("Fixed " + numFixed + " times \"Erfassungsgrad\"");
		}

		if (log.isInfoEnabled()) {
			log.info("Fix \"Erfassungsgrad\" (t011_obj_geo.rec_grade) from Commission to Omission...done");
		}
	}
}