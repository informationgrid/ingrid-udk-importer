/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v30;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;

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
				newRecGrade = new BigDecimal(newRecGrade).setScale(2, RoundingMode.HALF_UP).doubleValue();
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
