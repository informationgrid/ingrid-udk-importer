/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2019 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v1;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.UtilsInspireThemes;

/**
 * Assign INSPIRE themes to objects/addresses according to assigned searchterms. 
 */
public class IDCStrategy1_0_4_fixInspireThemes extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_4_fixInspireThemes.class);

	private int numObjThemesAdded = 0;

	/**
	 * Deliver NO Version, this strategy should NOT trigger a strategy workflow and
	 * can be executed on its own ! NOTICE: BUT is executed in workflow (part of workflow array) !
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Updating INSPIRE Themes in entities...");
		updateInspireThemes();
		System.out.println("done.");
		System.out.println("    Added " + numObjThemesAdded + " INSPIRE Themes to objects");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void updateInspireThemes() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating INSPIRE Themes in entities...");
		}

		// here we store the mapping of INSPIRE ids to the according searchterm id ("searchterm_value").
		// NOTICE: only one entry in searchterm_value per theme. Will be assigned to multiple entities
		// via searchterm_obj
		HashMap<Integer, Long> themeIdToSearchtermId = getThemeIdToSearchtermIdMap();

		// update objects
		numObjThemesAdded = 0;
		updateInspireThemesOfObjects(themeIdToSearchtermId);
		log.info("Added " + numObjThemesAdded + " INSPIRE Themes to objects");

		// NO INSPIRE Themes in addresses !

		if (log.isInfoEnabled()) {
			log.info("Updating INSPIRE Themes... done");
		}
	}

	/** analyze all object searchterms and assign fitting INSPIRE themes */
	private void updateInspireThemesOfObjects(HashMap<Integer, Long> themeIdToSearchtermId) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating INSPIRE Themes of objects...");
		}

		// all INSPIRE themes of object ordered by LINE asc
		String psSql = "SELECT termObj.line, val.entry_id " +
			"FROM searchterm_obj termObj, searchterm_value val " +
			"WHERE termObj.searchterm_id = val.id " +
			"and val.type = 'I' " +
			"and termObj.obj_id = ? " +
			"order by termObj.line";
		PreparedStatement psReadObjInspireTerms = jdbc.prepareStatement(psSql);
		
		// insert INSPIRE theme into searchterm_value
		psSql = "INSERT INTO searchterm_value " +
			"(id, type, term, entry_id) "
			+ "VALUES "
			+ "(?, 'I', ?, ?)";
		PreparedStatement psInsertTerm = jdbc.prepareStatement(psSql);

		// connect INSPIRE theme with object (insert searchterm_obj)
		psSql = "INSERT INTO searchterm_obj " +
			"(id, obj_id, line, searchterm_id) "
			+ "VALUES "
			+ "(?, ?, ?, ?)";
		PreparedStatement psInsertTermObj = jdbc.prepareStatement(psSql);

		// remove INSPIRE theme from object
		psSql = "DELETE FROM searchterm_obj " +
			"WHERE obj_id=? " +
			"and searchterm_id=?";
		PreparedStatement psRemoveTermObj = jdbc.prepareStatement(psSql);

		// here we track ids of assigned INSPIRE themes of current object
		Set<Integer> currThemeIds = new HashSet<Integer>();
		int currLine = 0;
		long currObjId = -1;
		String currObjUuid = null;
		String currObjWorkState = null;

		// iterate over all searchterms applied to objects ordered by object
		String sqlAllObjTerms = "SELECT termObj.obj_id, val.term, obj.obj_uuid, obj.work_state " +
			"FROM t01_object obj, searchterm_obj termObj, searchterm_value val " +
			"WHERE obj.id = termObj.obj_id " +
			"and termObj.searchterm_id = val.id " +
			"order by termObj.obj_id";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlAllObjTerms, st);
		while (rs.next()) {
			long readObjId = rs.getLong("obj_id");

			// check whether object changed
			if (readObjId != currObjId) {
				// object changed !

				// "finish" former object
				if (currObjId != -1) {
					// remove former "NO INSPIRE THEME" if new theme was added !
					if (currThemeIds.size() > 1 &&
							currThemeIds.contains(UtilsInspireThemes.noInspireThemeId)) {
						removeInspireThemeFromObject(UtilsInspireThemes.noInspireThemeId, currObjId,
								themeIdToSearchtermId, psRemoveTermObj, currThemeIds,
								currObjUuid, currObjWorkState);
					}
					// check whether former object has INSPIRE Theme, else add "NO INSPIRE THEME"
					if (currThemeIds.isEmpty()) {
						addInspireThemeToObject(UtilsInspireThemes.noInspireThemeId, currObjId, 1,
								themeIdToSearchtermId, psInsertTerm, psInsertTermObj,
								currThemeIds,
								null, currObjUuid, currObjWorkState);
					}
				}

				// process "new" object
				currObjId = readObjId;
				// needed for logging
				currObjUuid = rs.getString("obj_uuid");
				currObjWorkState = rs.getString("work_state");
				currLine = 0;
				currThemeIds.clear();

				// fetch all assigned INSPIRE themes and max line
				psReadObjInspireTerms.setLong(1, currObjId);
				ResultSet rsObjInspireTerms = psReadObjInspireTerms.executeQuery();
				while (rsObjInspireTerms.next()) {
					currLine = rsObjInspireTerms.getInt("line");
					Integer entryId = rsObjInspireTerms.getInt("entry_id");
					currThemeIds.add(entryId);
				}
				rsObjInspireTerms.close();
			}

			// analyze read searchterm. Check whether contains inspire term !
			// add according INSPIRE themes if not added yet !

			// read searchterm, lower case for comparison
			String searchTerm = rs.getString("term");
			Set<Integer> newThemeIds = UtilsInspireThemes.getThemeIdsOfTerm(searchTerm, null);
			for (Integer newThemeId : newThemeIds) {
				if (!currThemeIds.contains(newThemeId)) {
					addInspireThemeToObject(newThemeId, currObjId, ++currLine,
							themeIdToSearchtermId, psInsertTerm, psInsertTermObj,
							currThemeIds,
							searchTerm, currObjUuid, currObjWorkState);
				}
			}
		}
		rs.close();
		st.close();
		psReadObjInspireTerms.close();
		psInsertTerm.close();
		psInsertTermObj.close();
		psRemoveTermObj.close();

		if (log.isInfoEnabled()) {
			log.info("Updating INSPIRE Themes of objects... done");
		}
	}

	/** Get mapping of INSPIRE theme ids to according searchtermValues. */
	private HashMap<Integer,Long> getThemeIdToSearchtermIdMap() throws Exception {
		HashMap<Integer,Long> map = new HashMap<Integer,Long>();

		String sql = "SELECT id, entry_id " +
			"FROM searchterm_value " +
			"WHERE type = 'I' ";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);

		while (rs.next()) {
			int themeId = rs.getInt("entry_id");
			long searchtermId = rs.getLong("id");

			map.put(themeId, searchtermId);
		}
		rs.close();
		st.close();

		return map;
	}

	/** Create "searchterm_value" of INSPIRE theme if not present and assign to object via "searchterm_obj". */
	private void addInspireThemeToObject(int themeIdToAdd, long objectId, int line,
			HashMap<Integer,Long> themeIdToSearchtermId,
			PreparedStatement psInsertTerm, PreparedStatement psInsertTermObj,
			Set<Integer> currThemeIds,
			String searchtermForLog, String objUuidForLog, String workStateForLog) throws Exception {

		String themeName = UtilsInspireThemes.inspireThemes_de.get(themeIdToAdd);

		// first add inspire term to searchterms if not present yet !
		Long searchtermId = themeIdToSearchtermId.get(themeIdToAdd);
		if (searchtermId == null) {
			int cnt = 1;

			searchtermId = getNextId();
			psInsertTerm.setLong(cnt++, searchtermId); // searchterm_value.id
			psInsertTerm.setString(cnt++, themeName); // searchterm_value.term
			psInsertTerm.setInt(cnt++, themeIdToAdd); // searchterm_value.entry_id
			psInsertTerm.executeUpdate();

			themeIdToSearchtermId.put(themeIdToAdd, searchtermId);
		}
		
		// assign searchterm to object
		int cnt = 1;
		psInsertTermObj.setLong(cnt++, getNextId()); // searchterm_obj.id
		psInsertTermObj.setLong(cnt++, objectId); // searchterm_obj.obj_id
		psInsertTermObj.setInt(cnt++, line); // searchterm_obj.line
		psInsertTermObj.setLong(cnt++, searchtermId); // searchterm_obj.searchterm_id
		psInsertTermObj.executeUpdate();

		// also update our set !
		currThemeIds.add(themeIdToAdd);			
		numObjThemesAdded++;

		if (log.isDebugEnabled()) {
			String msg = "Added INSPIRE Theme: '" + themeName + "'";
			if (searchtermForLog != null) {
				msg = msg + ", Searchterm: '" + searchtermForLog + "'";
			}
			msg = msg + ", Obj-UUUID: " + objUuidForLog + ", workState: '" + workStateForLog + "'";

			log.debug(msg);
		}
	}

	/** Remove INSPIRE theme from object via "searchterm_obj". */
	private void removeInspireThemeFromObject(int themeIdToRemove, long objectId,
			HashMap<Integer,Long> themeIdToSearchtermId,
			PreparedStatement psRemoveTermObj,
			Set<Integer> currThemeIds,
			String objUuidForLog, String workStateForLog) throws Exception {

		Long searchtermId = themeIdToSearchtermId.get(themeIdToRemove);
		
		// remove only, if term exists
		if (searchtermId != null) {
			int cnt = 1;
			psRemoveTermObj.setLong(cnt++, objectId); // searchterm_obj.obj_id
			psRemoveTermObj.setLong(cnt++, searchtermId); // searchterm_obj.searchterm_id
			psRemoveTermObj.executeUpdate();
			
			// also update our set !
			currThemeIds.remove(themeIdToRemove);

			if (log.isDebugEnabled()) {
				String inspireTheme = UtilsInspireThemes.inspireThemes_de.get(themeIdToRemove);
				String msg = "Removed INSPIRE Theme: '" + inspireTheme + "'";
				msg = msg + ", Obj-UUUID: " + objUuidForLog + ", workState: '" + workStateForLog + "'";

				log.debug(msg);
			}
		}
	}
}
