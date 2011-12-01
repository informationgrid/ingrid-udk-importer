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
 *   <li>Move fields "Lagegenauigkeit" and "Höhengenauigkeit" to rubric "Datenqualität" (Profile), see INGRID32-48
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

		System.out.print("  Extending sys_list (new ones)...");
		extendSysList();
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

		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
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

	private void cleanUpSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list...");
		}
		
		int numDeleted;
		if (log.isInfoEnabled()) {
			log.info("Delete syslist 7110 (DQ_110_CompletenessOmission nameOfMeasure)...");
		}

		sqlStr = "DELETE FROM sys_list where lst_id = 7110";
		numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Deleted " + numDeleted +	" entries (all languages).");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list... done");
		}
	}

	protected void extendSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Extending sys_list...");
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

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 13);
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

		writeNewSyslist(lstId, newSyslistMap_de, newSyslistMap_en, 1);
// ---------------------------

		if (log.isInfoEnabled()) {
			log.info("Extending sys_list... done");
		}
	}

	/**
	 * @param defaultEntry pass key of default entry or < 0 if no default entry !
	 * @throws Exception
	 */
	private void writeNewSyslist(int listId,
			LinkedHashMap<Integer, String> syslistMap_de,
			LinkedHashMap<Integer, String> syslistMap_en,
			int defaultEntry) throws Exception {

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + listId;
		jdbc.executeUpdate(sqlStr);

		Iterator<Integer> itr = syslistMap_de.keySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			String isDefault = "N";
			if (key == defaultEntry) {
				isDefault = "Y";				
			}
			// german version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + listId + ", " + key + ", 'de', '" + syslistMap_de.get(key) + "', 0, '" + isDefault + "')");
			// english version
			jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) VALUES ("
					+ getNextId() + ", " + listId + ", " + key + ", 'en', '" + syslistMap_en.get(key) + "', 0, '" + isDefault + "')");
		}
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
		int indxRubricAllgemeines = MdekProfileUtils.findRubricIndex(profileBean, "general");
		Rubric rubric = MdekProfileUtils.removeRubric(profileBean, "thesaurus");
		MdekProfileUtils.addRubric(profileBean, rubric, indxRubricAllgemeines+1);

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
	}

	private void addJavaScriptToControls(ProfileBean profileBean) {
		// tags for marking newly added javascript code (for later removal)
		String startTag = "\n//<3.2.0 update\n";
		String endTag = "\n//3.2.0>\n";

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
"}});" + endTag;
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
"}});" + endTag;
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
"}});" + endTag;
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
"}});" + endTag;
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
"function isInspireRelevantHandler() {\n" +
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
"dojo.connect(dijit.byId(\"isInspireRelevant\"), \"onChange\", function(val) {isInspireRelevantHandler();});\n" +
"dojo.connect(dijit.byId(\"isInspireRelevant\"), \"onClick\", function(obj, field) {isInspireRelevantHandler();});\n" + endTag;
		MdekProfileUtils.updateScriptedProperties(control, jsCode);

		//------------- Rubrik 'Datenqualität' via JS in first Control 'Datendefizit'
    	if (log.isInfoEnabled()) {
			log.info("Rubrik 'Datenqualität'(refClass1DQ) via JS in 'Datendefizit'(uiElement3565): only show rubric when 'Geo-Information/Karte'");
		}
    	control = MdekProfileUtils.findControl(profileBean, "uiElement3565");
		jsCode = startTag +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"// show Rubrik 'Datenqualität' only in 'Geo-Information/Karte'\n" +
"if (c.objClass === \"Class1\") {\n" +
"  UtilUI.setShow(\"refClass1DQ\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"refClass1DQ\");\n" +
"}});" + endTag;
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
