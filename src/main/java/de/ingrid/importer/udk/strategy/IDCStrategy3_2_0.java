/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.mdek.beans.ProfileBean;
import de.ingrid.mdek.beans.Rubric;
import de.ingrid.mdek.beans.controls.Controls;
import de.ingrid.mdek.profile.ProfileMapper;
import de.ingrid.mdek.util.MdekProfileUtils;

/**
 * <p>
 * Changes InGrid 3.2:<p>
 * <ul>
 *   <li> adding NEW syslists for "Spezifikation der Konformität" (6005) and "Nutzungsbedingungen"
 *   (6020), modify according tables (add _key/_value), see https://dev.wemove.com/jira/browse/INGRID32-28
 * </ul>
 * Changes AK-IGE:<p>
 * <ul>
 *   <li>Profile: Move rubric "Verschlagwortung" after rubric "Allgemeines", move table "INSPIRE-Themen" from "Allgemeines" to "Verschlagwortung", see INGRID32-44  
 *   <li>Profile: Add Javascript for "ISO-Themenkategorie", "INSPIRE-Themen"/"INSPIRE-relevanter Datensatz" handling visibility and behaviour, see INGRID32-44, INGRID32-49
 *   <li>Profile: Add Javascript for "Sprache der Ressource" and "Zeichensatz des Datensatzes" handling visibility and behaviour, see INGRID32-43  
 *   <li>Move field "Datendefizit" to rubric "Datenqualität" (Profile), migrate data from table "Datendefizit" to field, remove table/data/syslist 7110, see INGRID32-48
 *   <li>Profile: Add Javascript for "Datendefizit" handling visibility of rubric "Datenqualität", see INGRID32-48  
 *   <li>Profile: Move fields "Lagegenauigkeit" and "Höhengenauigkeit" to rubric "Datenqualität", see INGRID32-48
 *   <li>Profile: Move field "Geoinformation/Karte - Sachdaten/Attributinformation" next to "Schlüsselkatalog", on Input make "Schlüsselkatalog" mandatory, see INGRID32-50
 *   <li>Change Syslist 505 (Address Rollenbezeichner), see INGRID32-46
 * </ul>
 */
public class IDCStrategy3_2_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_2_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_2_0;

	String profileXml = null;
    ProfileMapper profileMapper;
	ProfileBean profileBean = null;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Extend datastructure...");
		extendDataStructure();
		System.out.println("done.");

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Updating sys_list...");
		updateSysList();
		System.out.println("done.");

		System.out.print("  Updating object_use...");
		updateObjectUse();
		System.out.println("done.");

		System.out.print("  Updating object_conformity...");
		updateObjectConformity();
		System.out.println("done.");

		System.out.print("  Updating object_data_quality...");
		updateObjectDataQuality();
		System.out.println("done.");

		System.out.print("  Update Profile in database...");
		updateProfile();
		System.out.println("done.");

		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void extendDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Add columns 'terms_of_use_key/_value' to table 'object_use' ...");
		}
		jdbc.getDBLogic().addColumn("terms_of_use_key", ColumnType.INTEGER, "object_use", false, null, jdbc);
		// we use TEXT_NO_CLOB because current free entries ARE > 255 chars !
		jdbc.getDBLogic().addColumn("terms_of_use_value", ColumnType.TEXT_NO_CLOB, "object_use", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Add columns 'specification_key/_value' to table 'object_conformity' ...");
		}
		jdbc.getDBLogic().addColumn("specification_key", ColumnType.INTEGER, "object_conformity", false, null, jdbc);
		// we use TEXT_NO_CLOB because free entries may be > 255 !
		jdbc.getDBLogic().addColumn("specification_value", ColumnType.TEXT_NO_CLOB, "object_conformity", false, null, jdbc);

		if (log.isInfoEnabled()) {
			log.info("Extending datastructure... done");
		}
	}

	protected void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

// ---------------------------
		int lstId = 6005;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" = \"Spezifikation der Konformität\"...");
		}

		// NOTICE: SYSLIST contains date at end of syslist value (yyyy-MM-dd), has to be cut off in IGE ! But used for mapping in DSC-Scripted !
		// german syslist
		LinkedHashMap<Integer, String> newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "INSPIRE Data Specification on Addresses – Guidelines, 2010-05-03");
		newSyslistMap_de.put(2, "INSPIRE Data Specification on Administrative units --Guidelines, 2010-05-03");
		newSyslistMap_de.put(3, "INSPIRE Data Specification on Cadastral parcels --Guidelines, 2010-05-03");
		newSyslistMap_de.put(4, "INSPIRE Data Specification on Geographical names – Guidelines, 2010-05-03");
		newSyslistMap_de.put(5, "INSPIRE Data Specification on Hydrography – Guidelines, 2010-05-03");
		newSyslistMap_de.put(6, "INSPIRE Data Specification on Protected Sites – Guidelines, 2010-05-03");
		newSyslistMap_de.put(7, "INSPIRE Data Specification on Transport Networks – Guidelines, 2010-05-03");
		newSyslistMap_de.put(8, "INSPIRE Specification on Coordinate Reference Systems – Guidelines, 2010-05-03");
		newSyslistMap_de.put(9, "INSPIRE Specification on Geographical Grid Systems – Guidelines, 2010-05-03");
		newSyslistMap_de.put(10, "INSPIRE Durchführungsbestimmung Netzdienste, 2009-10-19");
		newSyslistMap_de.put(11, "INSPIRE Durchführungsbestimmung Metadaten, 2008-12-03");
		newSyslistMap_de.put(12, "INSPIRE Durchführungsbestimmung Interoperabilität von Geodatensätzen und --diensten, 2010-11-21");
		newSyslistMap_de.put(13, "INSPIRE Richtlinie, 2007-03-14");
		// english syslist
		LinkedHashMap<Integer, String> newSyslistMap_en = new LinkedHashMap<Integer, String>();
		newSyslistMap_en.put(1, "INSPIRE Data Specification on Addresses – Guidelines, 2010-05-03");
		newSyslistMap_en.put(2, "INSPIRE Data Specification on Administrative units --Guidelines, 2010-05-03");
		newSyslistMap_en.put(3, "INSPIRE Data Specification on Cadastral parcels --Guidelines, 2010-05-03");
		newSyslistMap_en.put(4, "INSPIRE Data Specification on Geographical names – Guidelines, 2010-05-03");
		newSyslistMap_en.put(5, "INSPIRE Data Specification on Hydrography – Guidelines, 2010-05-03");
		newSyslistMap_en.put(6, "INSPIRE Data Specification on Protected Sites – Guidelines, 2010-05-03");
		newSyslistMap_en.put(7, "INSPIRE Data Specification on Transport Networks – Guidelines, 2010-05-03");
		newSyslistMap_en.put(8, "INSPIRE Specification on Coordinate Reference Systems – Guidelines, 2010-05-03");
		newSyslistMap_en.put(9, "INSPIRE Specification on Geographical Grid Systems – Guidelines, 2010-05-03");
		newSyslistMap_en.put(10, "INSPIRE Durchführungsbestimmung Netzdienste, 2009-10-19");
		newSyslistMap_en.put(11, "INSPIRE Durchführungsbestimmung Metadaten, 2008-12-03");
		newSyslistMap_en.put(12, "INSPIRE Durchführungsbestimmung Interoperabilität von Geodatensätzen und --diensten, 2010-11-21");
		newSyslistMap_en.put(13, "INSPIRE Richtlinie, 2007-03-14");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 13, 13, null, null);
// ---------------------------
		lstId = 6020;
		if (log.isInfoEnabled()) {
			log.info("Inserting new syslist " + lstId +	" = \"Nutzungsbedingungen\"...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Keine");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "No conditions apply");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1, 1, null, null);
// ---------------------------
		lstId = 505;
		if (log.isInfoEnabled()) {
			log.info("Update syslist " + lstId +	" = \"Address Rollenbezeichner\"...");
		}

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Ressourcenanbieter");
		newSyslistMap_de.put(2, "Verwalter");
		newSyslistMap_de.put(3, "Eigentümer");
		newSyslistMap_de.put(4, "Nutzer");
		newSyslistMap_de.put(5, "Vertrieb");
		newSyslistMap_de.put(6, "Urheber");
		newSyslistMap_de.put(7, "Ansprechpartner");
		newSyslistMap_de.put(8, "Projektleitung");
		newSyslistMap_de.put(9, "Bearbeiter");
		newSyslistMap_de.put(10, "Herausgeber");
		newSyslistMap_de.put(11, "Autor");

		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Resource Provider");
		newSyslistMap_en.put(2, "Custodian");
		newSyslistMap_en.put(3, "Owner");
		newSyslistMap_en.put(4, "User");
		newSyslistMap_en.put(5, "Distributor");
		newSyslistMap_en.put(6, "Originator");
		newSyslistMap_en.put(7, "Point of Contact");
		newSyslistMap_en.put(8, "Principal Investigator");
		newSyslistMap_en.put(9, "Processor");
		newSyslistMap_en.put(10, "Publisher");
		newSyslistMap_en.put(11, "Author");

		// DESCRIPTION DE syslist (just for completeness)
		LinkedHashMap<Integer, String> newSyslistMap_description_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_description_de.put(1, "Anbieter der Ressource");
		newSyslistMap_description_de.put(2, "Person/Stelle, welche die Zuständigkeit und Verantwortlichkeit für einen Datensatz " +
				"übernommen hat und seine sachgerechte Pflege und Wartung sichert");
		newSyslistMap_description_de.put(3, "Eigentümer der Ressource");
		newSyslistMap_description_de.put(4, "Nutzer der Ressource");
		newSyslistMap_description_de.put(5, "Person oder Stelle für den Vertrieb");
		newSyslistMap_description_de.put(6, "Erzeuger der Ressource");
		newSyslistMap_description_de.put(7, "Kontakt für Informationen zur Ressource oder deren Bezugsmöglichkeiten");
		newSyslistMap_description_de.put(8, "Person oder Stelle, die verantwortlich für die Erhebung der Daten und die Untersuchung ist");
		newSyslistMap_description_de.put(9, "Person oder Stelle, welche die Ressource modifiziert");
		newSyslistMap_description_de.put(10, "Person oder Stelle, welche die Ressource veröffentlicht");
		newSyslistMap_description_de.put(11, "Verfasser der Ressource");

		// DESCRIPTION EN syslist (just for completeness)
		LinkedHashMap<Integer, String> newSyslistMap_description_en = new LinkedHashMap<Integer, String>();
		newSyslistMap_description_en.put(1, "Party that supplies the resource");
		newSyslistMap_description_en.put(2, "Party that accepts accountability and responsibility for the data and ensures " +
				"appropriate care and maintenance of the resource");
		newSyslistMap_description_en.put(3, "Party that owns the resource");
		newSyslistMap_description_en.put(4, "Party who uses the resource");
		newSyslistMap_description_en.put(5, "Party who distributes the resource");
		newSyslistMap_description_en.put(6, "Party who created the resource");
		newSyslistMap_description_en.put(7, "Party who can be contacted for acquiring knowledge about or acquisition of the resource");
		newSyslistMap_description_en.put(8, "Key party responsible for gathering information and conducting research");
		newSyslistMap_description_en.put(9, "Party who has processed the data in a manner such that the resource has been modified");
		newSyslistMap_description_en.put(10, "Party who published the resource");
		newSyslistMap_description_en.put(11, "Party who authored the resource");

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, -1, -1, newSyslistMap_description_de, newSyslistMap_description_en);
// ---------------------------
		if (log.isInfoEnabled()) {
			log.info("Delete syslist 7110 (DQ_110_CompletenessOmission nameOfMeasure)...");
		}

		sqlStr = "DELETE FROM sys_list where lst_id = 7110";
		int numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Deleted " + numDeleted +	" entries (all languages).");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}

	/**
	 * Also drops all old values (if syslist already exists) !
	 * @param listId id of syslist
	 * @param syslistMap_de german entries
	 * @param syslistMap_en english entries
	 * @param defaultEntry_de pass key of GERMAN default entry or -1 if no default entry !
	 * @param defaultEntry_en pass key of ENGLISH default entry or -1 if no default entry !
	 * @param syslistMap_descr_de pass null if no GERMAN description available
	 * @param syslistMap_descr_en pass null if no ENGLISH description available
	 * @throws Exception
	 */
	private void writeNewSyslist(int listId,
			LinkedHashMap<Integer, String> syslistMap_de,
			LinkedHashMap<Integer, String> syslistMap_en,
			int defaultEntry_de,
			int defaultEntry_en,
			LinkedHashMap<Integer, String> syslistMap_descr_de,
			LinkedHashMap<Integer, String> syslistMap_descr_en) throws Exception {
		
		if (syslistMap_descr_de == null) {
			syslistMap_descr_de = new LinkedHashMap<Integer, String>();
		}
		if (syslistMap_descr_en == null) {
			syslistMap_descr_en = new LinkedHashMap<Integer, String>();
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + listId;
		jdbc.executeUpdate(sqlStr);

		String psSql = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default, description) " +
				"VALUES (?,?,?,?,?,?,?,?)";		
		PreparedStatement psInsert = jdbc.prepareStatement(psSql);

		Iterator<Integer> itr = syslistMap_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			// german version
			String isDefault = "N";
			if (key == defaultEntry_de) {
				isDefault = "Y";				
			}
			psInsert.setLong(1, getNextId());
			psInsert.setInt(2, listId);
			psInsert.setInt(3, key);
			psInsert.setString(4, "de");
			psInsert.setString(5, syslistMap_de.get(key));
			psInsert.setInt(6, 0);
			psInsert.setString(7, isDefault);
			psInsert.setString(8, syslistMap_descr_de.get(key));
			psInsert.executeUpdate();

			// english version
			isDefault = "N";
			if (key == defaultEntry_en) {
				isDefault = "Y";				
			}
			psInsert.setLong(1, getNextId());
			psInsert.setString(4, "en");
			psInsert.setString(5, syslistMap_en.get(key));
			psInsert.setString(7, isDefault);
			psInsert.setString(8, syslistMap_descr_en.get(key));
			psInsert.executeUpdate();
		}

		psInsert.close();
	}

	private void updateObjectUse() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_use...");
		}

		if (log.isInfoEnabled()) {
			log.info("Transfer old 'terms_of_use' as free entry to new 'terms_of_use_key/_value' ...");
		}
		
		// NOTICE: No mapping of former values to new syslists. Every value becomes a free entry !!!
		// We "keep" type TEXT_NO_CLOB of values, so we do not have to reduce size !
		// But we copy every entry, to avoid database problems (e.g. on ORACLE we transfer CLOB -> VARCHAR(4000))
		// We do NOT update search index due to same values (but keep that commented !)

		String sql = "select id as objectUseId, terms_of_use from object_use";
/*
		// We read from node to determine working version to update search index ! 
		String sql = "select objNode.id as objNodeId, objNode.obj_id as objIdWorking, " +
				"obj.id as objId, obj.obj_uuid, " +
				"objectUse.id as objectUseId, objectUse.terms_of_use " +
				"from object_node objNode, t01_object obj, object_use objectUse " +
				"where objNode.obj_uuid = obj.obj_uuid " +
				"and obj.id = objectUse.obj_id";
*/
		// use PreparedStatement to avoid problems when value String contains "'" !!!
		String psSql = "UPDATE object_use SET " +
				"terms_of_use_key = -1, " +
				"terms_of_use_value = ? " +
				"WHERE id = ?";		
		PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
//		Set<Long> processedNodeIds = new HashSet<Long>();
		int numProcessed = 0;
		while (rs.next()) {
//			long objNodeId = rs.getLong("objNodeId");
//			long objIdWorking = rs.getLong("objIdWorking");
//			long objId = rs.getLong("objId");
//			String objUuid = rs.getString("obj_uuid");
			long objectUseId = rs.getLong("objectUseId");
			String termsOfUseText = rs.getString("terms_of_use");

			String termsOfUseVarchar = termsOfUseText;
/*
			if (termsOfUseText != null && termsOfUseText.length() > 255) {
				termsOfUseVarchar = termsOfUseText.substring(0, 255);
				if (log.isWarnEnabled()) {
					log.warn("Object '" + objUuid +	"', we reduce terms_of_use TEXT: '" + 
						termsOfUseText + "' --> VARCHAR255: '" + termsOfUseVarchar + "'");
				}
			}
*/
			psUpdate.setString(1, termsOfUseVarchar);
			psUpdate.setLong(2, objectUseId);
			psUpdate.executeUpdate();
/*
			// Node may contain different object versions, then we receive nodeId multiple times.
			// Write Index only once (index contains data of working version!) !
			if (!processedNodeIds.contains(objNodeId) && objIdWorking == objId) {
				JDBCHelper.updateObjectIndex(objNodeId, termsOfUseVarchar, jdbc);

				processedNodeIds.add(objNodeId);
			}
*/
			numProcessed++;
			if (log.isDebugEnabled()) {
//				log.debug("Object " + objUuid + " updated terms_of_use: '" + 
				log.debug("Updated terms_of_use: '" + termsOfUseText + "' --> '-1'/'" + termsOfUseVarchar + "'");
			}
		}
		rs.close();
		st.close();
		psUpdate.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " entries... done");
			log.info("Updating object_use... done");
		}
	}

	private void updateObjectConformity() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_conformity...");
		}

		if (log.isInfoEnabled()) {
			log.info("Transfer old 'specification' as free entry to new 'specification_key/_value' ...");
		}

		// NOTICE: No mapping of former values to new syslists. Every value becomes a free entry !!!
		// We "keep" type TEXT_NO_CLOB of values, so we do not have to reduce size !
		// But we copy every entry, to avoid database problems (e.g. on ORACLE we transfer CLOB -> VARCHAR(4000))
		// We do NOT update search index due to same values.

		String sql = "select id, specification from object_conformity";

		// use PreparedStatement to avoid problems when value String contains "'" !!!
		String psSql = "UPDATE object_conformity SET " +
				"specification_key = -1, " +
				"specification_value = ? " +
				"WHERE id = ?";		
		PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		int numProcessed = 0;
		while (rs.next()) {
			long id = rs.getLong("id");
			String specification = rs.getString("specification");

			psUpdate.setString(1, specification);
			psUpdate.setLong(2, id);
			psUpdate.executeUpdate();

			numProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Updated specification: '" + specification + "' --> '-1'/'" + specification + "'");
			}
		}
		rs.close();
		st.close();
		psUpdate.close();

		if (log.isInfoEnabled()) {
			log.info("Updated " + numProcessed + " entries... done");
			log.info("Updating object_conformity... done");
		}
	}

	private void updateObjectDataQuality() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating object_data_quality...");
		}

		if (log.isInfoEnabled()) {
			log.info("Transfer 'Datendefizit' value from DQ table (object_data_quality) to DQ field (t011_obj_geo.rec_grade) if field is empty ...");
		}

		// NOTICE: We do NOT update search index due to same values.

		// select all relevant entries in DQ Table
		String sqlSelectDQTable = "select obj_id, result_value from object_data_quality where dq_element_id = 110";

		// select according value in DQ Field
		PreparedStatement psSelectDQField = jdbc.prepareStatement(
				"SELECT rec_grade FROM t011_obj_geo WHERE obj_id = ?");

		// update according value in DQ Field
		PreparedStatement psUpdateDQField = jdbc.prepareStatement(
				"UPDATE t011_obj_geo SET " +
				"rec_grade = ? " +
				"WHERE obj_id = ?");

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlSelectDQTable, st);
		int numProcessed = 0;
		while (rs.next()) {
			long objId = rs.getLong("obj_id");
			String dqTableValue = rs.getString("result_value");

			if (dqTableValue != null) {
				// read according value from field
				psSelectDQField.setLong(1, objId);
				ResultSet rs2 = psSelectDQField.executeQuery();
				if (rs2.next()) {
					// just read it to check if null ! 
					double tmpDoubleForDebug = rs2.getDouble("rec_grade");
					if (rs2.wasNull()) {
						try {
							psUpdateDQField.setDouble(1, new Double(dqTableValue));
							psUpdateDQField.setLong(2, objId);
							psUpdateDQField.executeUpdate();
							numProcessed++;
							if (log.isDebugEnabled()) {
								log.debug("Transferred 'Datendefizit' value '" + dqTableValue +
									"' from DQ table to field (was empty), obj_id:" + objId);
							}
						} catch (Exception ex) {
							String msg = "Problems transferring 'Datendefizit' value '" + dqTableValue +
									"' from DQ table as DOUBLE to field, value is lost ! obj_id:" + objId;
							log.error(msg, ex);
							System.out.println(msg);
						}
					}
				}
				rs2.close();
			}
		}
		rs.close();
		st.close();
		psSelectDQField.close();
		psUpdateDQField.close();

		if (log.isInfoEnabled()) {
			log.info("Transferred " + numProcessed + " entries... done");
		}

		if (log.isInfoEnabled()) {
			log.info("Delete 'Datendefizit' values from DQ table (object_data_quality) ...");
		}
		sqlStr = "DELETE FROM object_data_quality where dq_element_id = 110";
		int numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Deleted " + numDeleted +	" entries.");
		}

		if (log.isInfoEnabled()) {
			log.info("Updating object_data_quality... done");
		}
	}

	private void updateProfile() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Update Profile in database...");
		}

        // read profile
		String profileXml = readGenericKey(KEY_PROFILE_XML);
		if (profileXml == null) {
			throw new Exception("igcProfile not set !");
		}
        profileMapper = new ProfileMapper();
		profileBean = profileMapper.mapStringToBean(profileXml);			

		moveRubricsAndControls(profileBean);
		
		addJavaScriptToControls(profileBean);

		// write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
		if (log.isDebugEnabled()) {
			log.debug("Resulting IGC Profile:" + profileXml);
		}
		setGenericKey(KEY_PROFILE_XML, profileXml);        	

		if (log.isInfoEnabled()) {
			log.info("Update Profile in database... done");
		}
	}

	/**
	 * Move rubric "Verschlagwortung" after rubric "Allgemeines".
	 * Move Control "INSPIRE-Themen" from "Allgemeines" to "Verschlagwortung".
	 * ...
	 */
	private void moveRubricsAndControls(ProfileBean profileBean) {
    	if (log.isInfoEnabled()) {
			log.info("Move rubric 'Verschlagwortung' after rubric 'Allgemeines'");
		}
		int index = MdekProfileUtils.findRubricIndex(profileBean, "general");
		Rubric rubric = MdekProfileUtils.removeRubric(profileBean, "thesaurus");
		MdekProfileUtils.addRubric(profileBean, rubric, index+1);

    	if (log.isInfoEnabled()) {
			log.info("Move control 'INSPIRE-Themen' from 'Allgemeines' to 'Verschlagwortung'");
		}
    	Controls control = MdekProfileUtils.removeControl(profileBean, "uiElement5064");
		rubric = MdekProfileUtils.findRubric(profileBean, "thesaurus");
		MdekProfileUtils.addControl(profileBean, control, rubric, 0);

		if (log.isInfoEnabled()) {
			log.info("Move control 'Datendefizit' from 'Fachbezug - Klasse 1' to 'Datenqualität'");
		}
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement3565");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1DQ");
		MdekProfileUtils.addControl(profileBean, control, rubric, 0);

		if (log.isInfoEnabled()) {
			log.info("Move control 'Höhengenauigkeit' from 'Fachbezug - Klasse 1' to 'Datenqualität'");
		}
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement5069");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1DQ");
		MdekProfileUtils.addControl(profileBean, control, rubric, 1);

		if (log.isInfoEnabled()) {
			log.info("Move control 'Lagegenauigkeit' from 'Fachbezug - Klasse 1' to 'Datenqualität'");
		}
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement3530");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1DQ");
		MdekProfileUtils.addControl(profileBean, control, rubric, 2);
		
		if (log.isInfoEnabled()) {
			log.info("Remove DQ table control 'Datendefizit' from 'Datenqualität'");
		}
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement7510");

    	if (log.isInfoEnabled()) {
			log.info("Move control 'Geo-Information/Karte - Sachdaten/Attributinformation' after 'Schlüsselkatalog'");
		}
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement5070");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1");
		index = MdekProfileUtils.findControlIndex(profileBean, rubric, "uiElement3535");
		MdekProfileUtils.addControl(profileBean, control, rubric, index+1);
	}

	private void addJavaScriptToControls(ProfileBean profileBean) {
		// tags for marking newly added javascript code (for later removal)
		String startTag = "\n//<3.2.0 update\n";
		String endTag = "//3.2.0>\n";

		//------------- 'Sprache der Ressource'
    	if (log.isInfoEnabled()) {
			log.info("'Sprache der Ressource'(uiElement5042): hide in 'Geodatendienst', make optional in classes 'Organisationenseinheit' + 'Vorhaben' + 'Informationssystem'");
		}
    	Controls control = MdekProfileUtils.findControl(profileBean, "uiElement5042");
		String jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"if (c.objClass === \"Class3\") {\n" +
"  // hide in 'Geodatendienst'\n" +
"  UtilUI.setHide(\"uiElement5042\");\n" +
"} else if (c.objClass === \"Class0\" || c.objClass === \"Class4\" || c.objClass === \"Class6\") {\n" +
"  // optional in classes 'Organisationenseinheit' + 'Vorhaben' + 'Informationssystem'\n" +
"  UtilUI.setOptional(\"uiElement5042\");\n" +
"} else {\n" +
"  UtilUI.setMandatory(\"uiElement5042\");\n" +
"}});\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);

		//------------- 'Zeichensatz des Datensatzes'
    	if (log.isInfoEnabled()) {
			log.info("'Zeichensatz des Datensatzes'(uiElement5043): only in 'Geo-Information/Karte', then optional");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement5043");
    	control.setIsMandatory(false);
    	control.setIsVisible("hide");
		jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"// only in 'Geo-Information/Karte', then optional\n" +
"if (c.objClass === \"Class1\") {\n" +
"  UtilUI.setOptional(\"uiElement5043\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"uiElement5043\");\n" +
"}});\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);

		//------------- 'ISO-Themenkategorie'
    	if (log.isInfoEnabled()) {
			log.info("'ISO-Themenkategorie'(uiElement5060): only in 'Geo-Information/Karte', then mandatory");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement5060");
    	control.setIsMandatory(false);
    	control.setIsVisible("hide");
		jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"// only in 'Geo-Information/Karte', then mandatory\n" +
"if (c.objClass === \"Class1\") {\n" +
"  UtilUI.setMandatory(\"uiElement5060\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"uiElement5060\");\n" +
"}});\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);
		
		//------------- 'INSPIRE-Themen'
    	if (log.isInfoEnabled()) {
			log.info("'INSPIRE-Themen'(uiElement5064): mandatory in 'Geo-Information/Karte', optional in classes 'Geodatendienst' + 'Informationssystem/Dienst/Anwendung' + 'Datensammlung/Datenbank'");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement5064");
    	control.setIsMandatory(false);
    	control.setIsVisible("hide");
		jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"if (c.objClass === \"Class3\" || c.objClass === \"Class5\" || c.objClass === \"Class6\") {\n" +
"  // optional in 'Geodatendienst' + 'Datensammlung/Datenbank' + 'Informationssystem/Dienst/Anwendung'\n" +
"  UtilUI.setOptional(\"uiElement5064\");\n" +
"} else if (c.objClass === \"Class1\") {\n" +
"  // mandatory in class 'Geo-Information/Karte'\n" +
"  UtilUI.setMandatory(\"uiElement5064\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"uiElement5064\");\n" +
"}});\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);
		
		//------------- 'INSPIRE-relevanter Datensatz'
    	if (log.isInfoEnabled()) {
			log.info("'INSPIRE-relevanter Datensatz'(uiElement6000): only in 'Geo-Information/Karte' + 'Geodatendienst' + 'Dienst/Anwendung/Informationssystem', then always show");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement6000");
    	control.setIsMandatory(false);
    	control.setIsVisible("hide");
		jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"// only in 'Geo-Information/Karte' + 'Geodatendienst' + 'Dienst/Anwendung/Informationssystem', then optional but always show\n" +
"if (c.objClass === \"Class1\" || c.objClass === \"Class3\" || c.objClass === \"Class6\") {\n" +
"  UtilUI.setShow(\"uiElement6000\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"uiElement6000\");\n" +
"}});\n" +
"// make 'INSPIRE-Themen' mandatory when selected\n" +
"function uiElement6000InputHandler() {\n" +
"  if (dijit.byId(\"isInspireRelevant\").checked) {\n" +
"    UtilUI.setMandatory(\"uiElement5064\");\n" +
"  } else {\n" +
"    if (\"Class1\" === UtilUdk.getObjectClass()) {\n" +
"      UtilUI.setMandatory(\"uiElement5064\");\n" +
"    } else {\n" +
"      UtilUI.setOptional(\"uiElement5064\");\n" +
"    }\n" +
"  }\n" +
"}\n" +
"dojo.connect(dijit.byId(\"isInspireRelevant\"), \"onChange\", function(val) {uiElement6000InputHandler();});\n" +
"dojo.connect(dijit.byId(\"isInspireRelevant\"), \"onClick\", function(obj, field) {uiElement6000InputHandler();});\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);

		//------------- show/hide Rubrik 'Datenqualität' via JS in first Control 'Datendefizit'
    	if (log.isInfoEnabled()) {
			log.info("show/hide Rubrik 'Datenqualität'(refClass1DQ) via JS in 'Datendefizit'(uiElement3565): only show rubric when 'Geo-Information/Karte'");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement3565");
		jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"// show Rubrik 'Datenqualität' only in 'Geo-Information/Karte'\n" +
"if (c.objClass === \"Class1\") {\n" +
"  UtilUI.setShow(\"refClass1DQ\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"refClass1DQ\");\n" +
"}});\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);

		//------------- 'Geo-Information/Karte - Sachdaten/Attributinformation' on input make 'Schlüsselkatalog' mandatory
    	if (log.isInfoEnabled()) {
			log.info("'Sachdaten/Attributinformation'(uiElement5070): on input make 'Schlüsselkatalog'(uiElement3535) mandatory");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement5070");
		jsCode = startTag +
"// make 'Schlüsselkatalog' mandatory on input 'Sachdaten/Attributinformation'\n" +
"function uiElement5070InputHandler() {\n" +
"  if (UtilGrid.getTableData(\"ref1Data\").length !== 0) {\n" +
"    UtilUI.setMandatory(\"uiElement3535\");\n" +
"  } else {\n" +
"    UtilUI.setOptional(\"uiElement3535\");\n" +
"  }\n" +
"}\n" +
"dojo.connect(UtilGrid.getTable(\"ref1Data\"), \"onDataChanged\", uiElement5070InputHandler);\n"
+ endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop column 'terms_of_use' from table 'object_use' ...");
		}
		jdbc.getDBLogic().dropColumn("terms_of_use", "object_use", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Drop columns 'specification', 'publication_date' from table 'object_conformity' ...");
		}
		jdbc.getDBLogic().dropColumn("specification", "object_conformity", jdbc);
		jdbc.getDBLogic().dropColumn("publication_date", "object_conformity", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
