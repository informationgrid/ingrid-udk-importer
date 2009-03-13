package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.util.InspireThemesHelper;

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
		HashMap<Integer, Long> inspireIdToSearchtermId = getInspireIdToSearchtermIdMap();

		// update objects
		numObjThemesAdded = 0;
		updateInspireThemesOfObjects(inspireIdToSearchtermId);
		log.info("Added " + numObjThemesAdded + " INSPIRE Themes to objects");

		// NO INSPIRE Themes in addresses !

		if (log.isInfoEnabled()) {
			log.info("Updating INSPIRE Themes... done");
		}
	}

	/** analyze all object searchterms and assign fitting INSPIRE themes */
	private void updateInspireThemesOfObjects(HashMap<Integer, Long> inspireIdToSearchtermId) throws Exception {
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
		List<Integer> currInspireIds = new ArrayList<Integer>();
		int currLine = 0;
		long currObjId = -1;
		String currObjUuid = null;
		String currObjWorkState = null;
		// all terms to compare with searchterms
		Set<String> inspireTerms = InspireThemesHelper.termToInspireIds_de.keySet();

		// iterate over all searchterms applied to objects ordered by object
		String sqlAllObjTerms = "SELECT termObj.obj_id, val.term, obj.obj_uuid, obj.work_state " +
			"FROM t01_object obj, searchterm_obj termObj, searchterm_value val " +
			"WHERE obj.id = termObj.obj_id " +
			"and termObj.searchterm_id = val.id " +
			"order by termObj.obj_id";
		ResultSet rs = jdbc.executeQuery(sqlAllObjTerms);
		while (rs.next()) {
			long readObjId = rs.getLong("obj_id");

			// check whether object changed
			if (readObjId != currObjId) {
				// object changed !

				// "finish" former object
				if (currObjId != -1) {
					// remove former "NO INSPIRE THEME" if new theme was added !
					if (currInspireIds.size() > 1 &&
							currInspireIds.contains(InspireThemesHelper.noInspireThemeId)) {
						removeInspireThemeFromObject(InspireThemesHelper.noInspireThemeId, currObjId,
								inspireIdToSearchtermId, psRemoveTermObj, currInspireIds,
								currObjUuid, currObjWorkState);
					}
					// check whether former object has INSPIRE Theme, else add "NO INSPIRE THEME"
					if (currInspireIds.isEmpty()) {
						addInspireThemeToObject(InspireThemesHelper.noInspireThemeId, currObjId, 1,
								inspireIdToSearchtermId, psInsertTerm, psInsertTermObj,
								currInspireIds,
								null, currObjUuid, currObjUuid);
					}
				}

				// process "new" object
				currObjId = readObjId;
				// needed for logging
				currObjUuid = rs.getString("obj_uuid");
				currObjWorkState = rs.getString("work_state");
				currLine = 0;
				currInspireIds.clear();

				// fetch all assigned INSPIRE themes and max line
				psReadObjInspireTerms.setLong(1, currObjId);
				ResultSet rsObjInspireTerms = psReadObjInspireTerms.executeQuery();
				while (rsObjInspireTerms.next()) {
					currLine = rsObjInspireTerms.getInt("line");
					Integer entryId = rsObjInspireTerms.getInt("entry_id");
					if (!currInspireIds.contains(entryId)) {
						currInspireIds.add(entryId);
					}
				}
				rsObjInspireTerms.close();
			}

			// analyze read searchterm. Check whether contains inspire term !
			// add according INSPIRE themes if not added yet !

			// read searchterm, lower case for comparison
			String searchTerm = rs.getString("term").toLowerCase();
			
			// analyze term. contains INSPIRE term ?
			for (String inspireTerm : inspireTerms) {
				if (searchTerm.contains(inspireTerm)) {
					// yes ! fetch according theme Ids
					Integer[] inspireIds = InspireThemesHelper.termToInspireIds_de.get(inspireTerm);
					// add new themes if not assigned yet
					for (Integer inspireId : inspireIds) {
						if (!currInspireIds.contains(inspireId)) {
							addInspireThemeToObject(inspireId, currObjId, ++currLine,
									inspireIdToSearchtermId, psInsertTerm, psInsertTermObj,
									currInspireIds,
									searchTerm, currObjUuid, currObjWorkState);
						}
					}
				}
			}
		}
		rs.close();
		psReadObjInspireTerms.close();
		psInsertTerm.close();
		psInsertTermObj.close();
		psRemoveTermObj.close();

		if (log.isInfoEnabled()) {
			log.info("Updating INSPIRE Themes of objects... done");
		}
	}

	/** Get mapping of INSPIRE theme ids to according searchtermValues. */
	private HashMap<Integer,Long> getInspireIdToSearchtermIdMap() throws Exception {
		HashMap<Integer,Long> map = new HashMap<Integer,Long>();

		String sql = "SELECT id, entry_id " +
			"FROM searchterm_value " +
			"WHERE type = 'I' ";
		ResultSet rs = jdbc.executeQuery(sql);

		while (rs.next()) {
			int inspireId = rs.getInt("entry_id");
			long searchtermId = rs.getLong("id");

			map.put(inspireId, searchtermId);
		}
		rs.close();

		return map;
	}

	/** Create "searchterm_value" of INSPIRE theme if not present and assign to object via "searchterm_obj". */
	private void addInspireThemeToObject(int inspireId, long objectId, int line,
			HashMap<Integer,Long> inspireIdToSearchtermId,
			PreparedStatement psInsertTerm, PreparedStatement psInsertTermObj,
			List<Integer> currInspireIds,
			String searchtermForLog, String objUuidForLog, String workStateForLog) throws Exception {

		String inspireTheme = InspireThemesHelper.inspireThemes_de.get(inspireId);

		// first add inspire term to searchterms if not present yet !
		Long searchtermId = inspireIdToSearchtermId.get(inspireId);
		if (searchtermId == null) {
			int cnt = 1;

			searchtermId = getNextId();
			psInsertTerm.setLong(cnt++, searchtermId); // searchterm_value.id
			psInsertTerm.setString(cnt++, inspireTheme); // searchterm_value.term
			psInsertTerm.setInt(cnt++, inspireId); // searchterm_value.entry_id
			psInsertTerm.executeUpdate();

			inspireIdToSearchtermId.put(inspireId, searchtermId);
		}
		
		// assign searchterm to object
		int cnt = 1;
		psInsertTermObj.setLong(cnt++, getNextId()); // searchterm_obj.id
		psInsertTermObj.setLong(cnt++, objectId); // searchterm_obj.obj_id
		psInsertTermObj.setInt(cnt++, line); // searchterm_obj.line
		psInsertTermObj.setLong(cnt++, searchtermId); // searchterm_obj.searchterm_id
		psInsertTermObj.executeUpdate();

		// also update our list !
		currInspireIds.add(inspireId);			
		numObjThemesAdded++;

		if (log.isDebugEnabled()) {
			String msg = "Added INSPIRE Theme: '" + inspireTheme + "'";
			if (searchtermForLog != null) {
				msg = msg + ", Searchterm: '" + searchtermForLog + "'";
			}
			msg = msg + ", Obj-UUUID: " + objUuidForLog + ", workState: '" + workStateForLog + "'";

			log.debug(msg);
		}
	}

	/** Remove INSPIRE theme from object via "searchterm_obj". */
	private void removeInspireThemeFromObject(int inspireId, long objectId,
			HashMap<Integer,Long> inspireIdToSearchtermId,
			PreparedStatement psRemoveTermObj,
			List<Integer> currInspireIds,
			String objUuidForLog, String workStateForLog) throws Exception {

		Long searchtermId = inspireIdToSearchtermId.get(inspireId);
		
		// remove only, if term exists
		if (searchtermId != null) {
			int cnt = 1;
			psRemoveTermObj.setLong(cnt++, objectId); // searchterm_obj.obj_id
			psRemoveTermObj.setLong(cnt++, searchtermId); // searchterm_obj.searchterm_id
			psRemoveTermObj.executeUpdate();
			
			// also update our list !
			while (currInspireIds.contains((Object)inspireId)) {
				currInspireIds.remove((Object)inspireId);
			}

			if (log.isDebugEnabled()) {
				String inspireTheme = InspireThemesHelper.inspireThemes_de.get(inspireId);
				String msg = "Removed INSPIRE Theme: '" + inspireTheme + "'";
				msg = msg + ", Obj-UUUID: " + objUuidForLog + ", workState: '" + workStateForLog + "'";

				log.debug(msg);
			}
		}
	}
}
