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
package de.ingrid.importer.udk.strategy.v431;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 * Changes InGrid 4.3.1
 * <p>
 * <ul>
 * <li>Fix references to removed topic 'Kein INSPIRE-Thema'</li>
 * </ul>
 */
public class IDCStrategy4_3_1_fixSearchtermReferences extends IDCStrategyDefault {

	private static final Log LOG = LogFactory.getLog(IDCStrategy4_3_1_fixSearchtermReferences.class);

	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		LOG.info("Update Profile in database...");
		fixSearchTermReferences();
		LOG.info("done.");

		jdbc.commit();
		LOG.info("Update finished successfully.");
	}

	private void fixSearchTermReferences() throws SQLException {
		PreparedStatement psDeleteUnreferencedValues = jdbc.prepareStatement(
				"DELETE FROM searchterm_obj " +
						"WHERE NOT EXISTS (SELECT * FROM searchterm_value sv WHERE sv.id = searchterm_obj.searchterm_id)");

		psDeleteUnreferencedValues.executeUpdate();
	}
}
