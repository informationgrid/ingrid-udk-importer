/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.util.UtilsUdkCodelistsSerialized;
import de.ingrid.mdek.beans.ProfileBean;
import de.ingrid.mdek.beans.Rubric;
import de.ingrid.mdek.beans.controls.Controls;
import de.ingrid.mdek.profile.ProfileMapper;
import de.ingrid.mdek.util.MdekProfileUtils;
import de.ingrid.utils.udk.UtilsLanguageCodelist;

/**
 * <p>
 * Changes InGrid 3.2:<p>
 * <ul>
 *   <li> adding NEW syslists for "Spezifikation der Konformität" (6005) and "Nutzungsbedingungen" (6020), modify according tables (add _key/_value), see INGRID32-28
 * </ul>
 * Changes AK-IGE:<p>
 * <ul>
 *   <li>Add "publication_date" as metadata to syslist 6005, drop column from object_conformity, see INGRID32-47  
 *   <li>Profile: Move rubric "Verschlagwortung" after rubric "Allgemeines", move table "INSPIRE-Themen" from "Allgemeines" to "Verschlagwortung", see INGRID32-44  
 *   <li>Profile: Add Javascript for "ISO-Themenkategorie", "INSPIRE-Themen"/"INSPIRE-relevanter Datensatz" handling visibility and behaviour, see INGRID32-44, INGRID32-49
 *   <li>Profile: Add Javascript for "Sprache der Ressource" and "Zeichensatz des Datensatzes" handling visibility and behaviour, see INGRID32-43
 *   <li>Move field "Datendefizit" to rubric "Datenqualität" (Profile), migrate data from table "Datendefizit" to field, remove table/data/syslist 7110, see INGRID32-48
 *   <li>Move fields "Lagegenauigkeit" and "Höhengenauigkeit" to rubric "Datenqualität" (Profile), migrate data from table "Absoulte Positionsgenauigkeit", remove table/data/syslist 7117, see INGRID32-48
 *   <li>Profile: Add Javascript for "Datendefizit" handling visibility of rubric "Datenqualität", see INGRID32-48  
 *   <li>Profile: Move field "Geoinformation/Karte - Sachdaten/Attributinformation" next to "Schlüsselkatalog", on Input make "Schlüsselkatalog" mandatory, see INGRID32-50
 *   <li>New control "Objektartenkatalog" for "Datensammlung / Datenbank" (Profile), new db table "object_types_catalogue" replacing also old "t011_obj_geo table", migrate data ..., see INGRID32-50
 *   <li>Change Syslist 505 (Address Rollenbezeichner), also migrate data, then COMMENTED migration, see INGRID32-46
 *   <li>Profile: Remove Publishable JS call from "Nutzungsbedingungen", now textfield, not table anymore, see INGRID32-45
 *   <li>Change syslist.name + .description to TEXT, see INGRID32-45
 *   <li>Add t03_catalogue.cat_namespace, see INGRID32-30
 *   <li>Remove columns from t017_url_ref, remove syslist 2240 (url datatype), extend syslist 2000,  see INGRID32-27 (Rework dialog "Add/Edit Link")
 *   <li>Profile: Move table "Geodatendienst - Operationen" before "Erstellungsmaßstab", always visible; add JS onPublish, see INGRID32-26
 *   <li>Add new syslist 5180 for operation platform incl. "Altdatenuebernahme", see INGRID32-26
 *   <li>Remove default values from syslist 510 "Zeichensatz des Datensatzes", see INGRID32-43
 *   <li>Add t02_address.hide_address column, see INGRID32-37
 *   <li>Change sys_list.lang_id to VARCHAR(255) + update syslists in catalog from file to match repo, also writes NEW "languages" (iso, req_value), see INGRID32-24
 * </ul>
 */
public class IDCStrategy3_2_0 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_2_0.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_2_0;

	String profileXml = null;
    ProfileMapper profileMapper;
	ProfileBean profileBean = null;

	/** former "Datenverantwortung" becomes "Verwalter", this is the syslist entry key */
	int syslist505EntryKeyDatenverantwortung;
	/** former "Datenverantwortung" becomes "Verwalter", this is the new entry value (in language of catalog) */
	String syslist505EntryValueVerwalter;
	/** former "Auskunft" becomes "Ansprechpartner", this is the syslist entry key */
	int syslist505EntryKeyAuskunft;

	/** nameOfMeasureKey 'Mean value of positional uncertainties (1D)' == Lagegenauigkeit */
	int syslist7117EntryKeyLagegenauigkeit = 1;
	/** nameOfMeasureKey 'Mean value of positional uncertainties (2D)' == Höhengenauigkeit */
	int syslist7117EntryKeyHoehegenauigkeit = 2;

	/** ID of operation platform syslist */
	int SYSLIST_ID_OPERATION_PLATFORM = 5180;

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
		updateSysListsFromFile();
		System.out.println("done.");

		System.out.print("  Updating object_use...");
		updateObjectUse();
		System.out.println("done.");

		System.out.print("  Updating object_conformity...");
		updateObjectConformity();
		System.out.println("done.");

		System.out.print("  Updating object_data_quality...");
		updateDQDatendefizit();
		updateDQAbsPosGenauigkeit();
		System.out.println("done.");

		// DO NOT MIGRATE ADDRESS roles anymore, see INGRID32-46
/*
		System.out.print("  Updating t012_obj_adr...");
		updateT012ObjAdr();
		System.out.println("done.");
*/
		System.out.print("  Updating object_types_catalogue...");
		updateObjectTypesCatalogue();
		System.out.println("done.");

		System.out.print("  Updating t011_obj_serv_op_platform...");
		updateT011ObjServOpPlatform();
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
		log.info("\nExtending datastructure -> CAUSES COMMIT ! ...");

		log.info("Add columns 'terms_of_use_key/_value' to table 'object_use' ...");
		jdbc.getDBLogic().addColumn("terms_of_use_key", ColumnType.INTEGER, "object_use", false, null, jdbc);
		// we use TEXT_NO_CLOB because current free entries ARE > 255 chars !
		jdbc.getDBLogic().addColumn("terms_of_use_value", ColumnType.TEXT_NO_CLOB, "object_use", false, null, jdbc);

		log.info("Add columns 'specification_key/_value' to table 'object_conformity' ...");
		jdbc.getDBLogic().addColumn("specification_key", ColumnType.INTEGER, "object_conformity", false, null, jdbc);
		// we use TEXT_NO_CLOB because free entries may be > 255 !
		jdbc.getDBLogic().addColumn("specification_value", ColumnType.TEXT_NO_CLOB, "object_conformity", false, null, jdbc);

		log.info("Create table 'object_types_catalogue'...");
		jdbc.getDBLogic().createTableObjectTypesCatalogue(jdbc);

		log.info("Change column type sys_list.name + .description to TEXT ...");
		jdbc.getDBLogic().modifyColumn("name", ColumnType.TEXT_NO_CLOB, "sys_list", false, jdbc);
		jdbc.getDBLogic().modifyColumn("description", ColumnType.TEXT_NO_CLOB, "sys_list", false, jdbc);

		log.info("Change column type sys_list.lang_id to VARCHAR(255) ...");
		jdbc.getDBLogic().modifyColumn("lang_id", ColumnType.VARCHAR255, "sys_list", true, jdbc);

		log.info("Add column 'cat_namespace' to table 't03_catalogue' ...");
		jdbc.getDBLogic().addColumn("cat_namespace", ColumnType.VARCHAR1024, "t03_catalogue", false, null, jdbc);

		log.info("Add columns 'platform_key/_value' to table 't011_obj_serv_op_platform' ...");
		jdbc.getDBLogic().addColumn("platform_key", ColumnType.INTEGER, "t011_obj_serv_op_platform", false, null, jdbc);
		jdbc.getDBLogic().addColumn("platform_value", ColumnType.VARCHAR255, "t011_obj_serv_op_platform", false, null, jdbc);

		log.info("Add column 'hide_address' to table 't02_address' ...");
		jdbc.getDBLogic().addColumn("hide_address", ColumnType.VARCHAR1, "t02_address", false, "'N'", jdbc);

		log.info("Extending datastructure... done\n");
	}

	protected void updateSysList() throws Exception {
		log.info("\nUpdating sys_list...");

// ---------------------------
		int lstId = 6005;
		log.info("Inserting new syslist " + lstId +	" = \"Spezifikation der Konformität\"...");

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

		writeNewSyslist(lstId, true, newSyslistMap_de, newSyslistMap_en, 13, 13, null, null);
// ---------------------------
		lstId = 6020;
		log.info("Inserting new syslist " + lstId +	" = \"Nutzungsbedingungen\"...");

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Keine");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "No conditions apply");

		writeNewSyslist(lstId, true, newSyslistMap_de, newSyslistMap_en, -1, -1, null, null);
// ---------------------------
		lstId = 505;
		log.info("Update syslist " + lstId +	" = \"Address Rollenbezeichner\"...");

		// german syslist
		syslist505EntryKeyDatenverantwortung = 2;
		syslist505EntryKeyAuskunft = 7;
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "Ressourcenanbieter");
		newSyslistMap_de.put(syslist505EntryKeyDatenverantwortung, "Verwalter");
		newSyslistMap_de.put(3, "Eigentümer");
		newSyslistMap_de.put(4, "Nutzer");
		newSyslistMap_de.put(5, "Vertrieb");
		newSyslistMap_de.put(6, "Urheber");
		newSyslistMap_de.put(syslist505EntryKeyAuskunft, "Ansprechpartner");
		newSyslistMap_de.put(8, "Projektleitung");
		newSyslistMap_de.put(9, "Bearbeiter");
		newSyslistMap_de.put(10, "Herausgeber");
		newSyslistMap_de.put(11, "Autor");

		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "Resource Provider");
		newSyslistMap_en.put(syslist505EntryKeyDatenverantwortung, "Custodian");
		newSyslistMap_en.put(3, "Owner");
		newSyslistMap_en.put(4, "User");
		newSyslistMap_en.put(5, "Distributor");
		newSyslistMap_en.put(6, "Originator");
		newSyslistMap_en.put(syslist505EntryKeyAuskunft, "Point of Contact");
		newSyslistMap_en.put(8, "Principal Investigator");
		newSyslistMap_en.put(9, "Processor");
		newSyslistMap_en.put(10, "Publisher");
		newSyslistMap_en.put(11, "Author");

		// DESCRIPTION DE syslist (just for completeness)
		LinkedHashMap<Integer, String> newSyslistMap_description_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_description_de.put(1, "Anbieter der Ressource");
		newSyslistMap_description_de.put(syslist505EntryKeyDatenverantwortung, "Person/Stelle, welche die Zuständigkeit und Verantwortlichkeit für einen Datensatz " +
				"übernommen hat und seine sachgerechte Pflege und Wartung sichert");
		newSyslistMap_description_de.put(3, "Eigentümer der Ressource");
		newSyslistMap_description_de.put(4, "Nutzer der Ressource");
		newSyslistMap_description_de.put(5, "Person oder Stelle für den Vertrieb");
		newSyslistMap_description_de.put(6, "Erzeuger der Ressource");
		newSyslistMap_description_de.put(syslist505EntryKeyAuskunft, "Kontakt für Informationen zur Ressource oder deren Bezugsmöglichkeiten");
		newSyslistMap_description_de.put(8, "Person oder Stelle, die verantwortlich für die Erhebung der Daten und die Untersuchung ist");
		newSyslistMap_description_de.put(9, "Person oder Stelle, welche die Ressource modifiziert");
		newSyslistMap_description_de.put(10, "Person oder Stelle, welche die Ressource veröffentlicht");
		newSyslistMap_description_de.put(11, "Verfasser der Ressource");

		// DESCRIPTION EN syslist (just for completeness)
		LinkedHashMap<Integer, String> newSyslistMap_description_en = new LinkedHashMap<Integer, String>();
		newSyslistMap_description_en.put(1, "Party that supplies the resource");
		newSyslistMap_description_en.put(syslist505EntryKeyDatenverantwortung, "Party that accepts accountability and responsibility for the data and ensures " +
				"appropriate care and maintenance of the resource");
		newSyslistMap_description_en.put(3, "Party that owns the resource");
		newSyslistMap_description_en.put(4, "Party who uses the resource");
		newSyslistMap_description_en.put(5, "Party who distributes the resource");
		newSyslistMap_description_en.put(6, "Party who created the resource");
		newSyslistMap_description_en.put(syslist505EntryKeyAuskunft, "Party who can be contacted for acquiring knowledge about or acquisition of the resource");
		newSyslistMap_description_en.put(8, "Key party responsible for gathering information and conducting research");
		newSyslistMap_description_en.put(9, "Party who has processed the data in a manner such that the resource has been modified");
		newSyslistMap_description_en.put(10, "Party who published the resource");
		newSyslistMap_description_en.put(11, "Party who authored the resource");

		writeNewSyslist(lstId, true, newSyslistMap_de, newSyslistMap_en, -1, -1, newSyslistMap_description_de, newSyslistMap_description_en);

		// also fix data in objects (values dependent from catalog language) !

		Iterator<Entry<Integer,String>> entryIt;
		if ("de".equals(UtilsLanguageCodelist.getShortcutFromCode(readCatalogLanguageKey()))) {
			entryIt = newSyslistMap_de.entrySet().iterator();
		} else {
			entryIt = newSyslistMap_en.entrySet().iterator();
		}

		String psSql = "UPDATE t012_obj_adr SET special_name = ? " +
				"WHERE special_ref = 505 AND type = ?";
		PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		while (entryIt.hasNext()) {
			Entry<Integer,String> entry = entryIt.next();
			
			if (entry.getKey().equals(syslist505EntryKeyDatenverantwortung)) {
				syslist505EntryValueVerwalter = entry.getValue();
			}
			
			psUpdate.setString(1, entry.getValue());
			psUpdate.setInt(2, entry.getKey());
			int numUpdated = psUpdate.executeUpdate();

			log.debug("t012_obj_adr: updated " + numUpdated + " rows -> type(" + entry.getKey() + "), " +
					"new value(" +	entry.getValue() + ")");
		}
		psUpdate.close();

// ---------------------------
		log.info("Delete syslist 7110 (DQ_110_CompletenessOmission = nameOfMeasure for DQ Table 'Datendefizit')...");

		sqlStr = "DELETE FROM sys_list where lst_id = 7110";
		int numUpdated = jdbc.executeUpdate(sqlStr);
		log.debug("Deleted " + numUpdated +	" entries (all languages).");

// ---------------------------
		log.info("Delete syslist 7117 (DQ_117_AbsoluteExternalPositionalAccuracy = nameOfMeasure for DQ Table 'Absoulte Positionsgenauigkeit')...");

		sqlStr = "DELETE FROM sys_list where lst_id = 7117";
		numUpdated = jdbc.executeUpdate(sqlStr);
		log.debug("Deleted " + numUpdated +	" entries (all languages).");

// ---------------------------
		lstId = 2000;
		log.info("Insert new entries \"3109/Objektartenkatalog\", \"9990/Datendownload\", " +
				"\"9999/unspezifischer Verweis\" to syslist" + lstId +	" (link type) ...");

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(3109, "Objektartenkatalog");
		newSyslistMap_de.put(9990, "Datendownload");
		newSyslistMap_de.put(9999, "unspezifischer Verweis");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(3109, "Key Catalog");
		newSyslistMap_en.put(9990, "Download of data");
		newSyslistMap_en.put(9999, "unspecific Link");

		writeNewSyslist(lstId, false, newSyslistMap_de, newSyslistMap_en, -1, -1, null, null);

		log.info("Updating sys_list... done\n");

// ---------------------------
		log.info("Delete syslist 2240 (url datatype for t017_url_ref.datatype_key/.datatype_value...");

		sqlStr = "DELETE FROM sys_list where lst_id = 2240";
		numUpdated = jdbc.executeUpdate(sqlStr);
		log.debug("Deleted " + numUpdated +	" entries (all languages).");

// ---------------------------
		lstId = SYSLIST_ID_OPERATION_PLATFORM;
		log.info("Inserting new syslist " + lstId +	" = \"Operation - Unterstützte Platformen\"...");

		// german syslist
		newSyslistMap_de = new LinkedHashMap<Integer, String>();
		newSyslistMap_de.put(1, "XML");
		newSyslistMap_de.put(2, "CORBA");
		newSyslistMap_de.put(3, "JAVA");
		newSyslistMap_de.put(4, "COM");
		newSyslistMap_de.put(5, "SQL");
		newSyslistMap_de.put(6, "WebServices");
		newSyslistMap_de.put(7, "HTTPGet");
		newSyslistMap_de.put(8, "HTTPPost");
		newSyslistMap_de.put(9, "SOAP");
		// english syslist
		newSyslistMap_en = new LinkedHashMap<Integer, String>(); 
		newSyslistMap_en.put(1, "XML");
		newSyslistMap_en.put(2, "CORBA");
		newSyslistMap_en.put(3, "JAVA");
		newSyslistMap_en.put(4, "COM");
		newSyslistMap_en.put(5, "SQL");
		newSyslistMap_en.put(6, "WebServices");
		newSyslistMap_en.put(7, "HTTPGet");
		newSyslistMap_en.put(8, "HTTPPost");
		newSyslistMap_en.put(9, "SOAP");

		writeNewSyslist(lstId, true, newSyslistMap_de, newSyslistMap_en, -1, -1, null, null);

// ---------------------------
		log.info("Remove default values from syslist 510 (\"Zeichensatz des Datensatzes\")...");

		sqlStr = "UPDATE sys_list SET is_default = 'N' WHERE lst_id = 510";
		numUpdated = jdbc.executeUpdate(sqlStr);
		log.debug("Set " + numUpdated +	" entries to is_default = 'N' (all languages).");

// ---------------------------
		log.info("Updating sys_list... done\n");
	}

	/**
	 * Also drops all old values (if syslist already exists) !
	 * @param listId id of syslist
	 * @param deleteOldValues pass true if all old syslist values should be deleted before adding new ones !
	 * @param syslistMap_de german entries
	 * @param syslistMap_en english entries
	 * @param defaultEntry_de pass key of GERMAN default entry or -1 if no default entry !
	 * @param defaultEntry_en pass key of ENGLISH default entry or -1 if no default entry !
	 * @param syslistMap_descr_de pass null if no GERMAN description available
	 * @param syslistMap_descr_en pass null if no ENGLISH description available
	 * @throws Exception
	 */
	private void writeNewSyslist(int listId,
			boolean deleteOldValues,
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

		if (deleteOldValues) {
			// clean up, to guarantee no old values !
			sqlStr = "DELETE FROM sys_list where lst_id = " + listId;
			jdbc.executeUpdate(sqlStr);
		}

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

    /** Update syslists in IGC catalog from file to match repo. Also writes NEW "languages" (iso, req_value) */
    private void updateSysListsFromFile() throws Exception {
		log.info("\nUpdating sys_list from file to match REPO ! ...");

    	String psSql = "SELECT name FROM sys_list WHERE lst_id = ? AND entry_id = ? AND lang_id = ?";
    	PreparedStatement psSelect = jdbc.prepareStatement(psSql);

    	psSql = "UPDATE sys_list SET name = ? " +
    			"WHERE lst_id = ? AND entry_id = ? AND lang_id = ?";
    	PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		psSql = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name) " +
				"VALUES (?,?,?,?,?)";		
		PreparedStatement psInsert = jdbc.prepareStatement(psSql);

		Map<Long, String> langMap = new HashMap<Long, String>();
		langMap.put(150150150L, "iso");
		langMap.put(8150815L, "req_value");
		langMap.put(150L, "de");
		langMap.put(123L, "en");

		UtilsUdkCodelistsSerialized listsSerializedUtil = UtilsUdkCodelistsSerialized.getInstance("3_2_0_udk_codelists_serialized.xml");
		// remove the syslists not needed anymore (e.g. deleted above ...)
		listsSerializedUtil.removeUnwantedSyslists(new int[] { 7117, 7110 });

		// then get all remaining syslists (of file) 
    	Map<Long, List<de.ingrid.utils.udk.CodeListEntry>> allLists = listsSerializedUtil.getAllCodeLists();

        for (Iterator itListIds = allLists.keySet().iterator(); itListIds.hasNext();) {
        	Long listId = (Long) itListIds.next();
            List<de.ingrid.utils.udk.CodeListEntry> listEntries =  allLists.get(listId);

            for (de.ingrid.utils.udk.CodeListEntry entry : listEntries) {
            	String langString = langMap.get(entry.getLangId());
            	if (langString == null || langString.trim().length() == 0) {
            		log.error("Wrong language in read Syslist entry: listId/entryId/language = " + 
            			entry.getCodeListId() + "/" + entry.getDomainId() + "/" + entry.getLangId());
            		continue;
            	}
            	
            	// first check, whether entry exists
    			psSelect.setLong(1, entry.getCodeListId());
    			psSelect.setLong(2, entry.getDomainId());
    			psSelect.setString(3, langString);
    			ResultSet rs = psSelect.executeQuery();
    			int entryCount = 0;
    			while (rs.next()) {
    				entryCount++;
    				if (entryCount > 1) {
                		log.error("Multiple Entries for entry ! we only process first one !: listId/entryId/language = " + 
                    			entry.getCodeListId() + "/" + entry.getDomainId() + "/" + entry.getLangId());
                		break;
    				}
    				
    				String oldValue = rs.getString("name");
    				String newValue = entry.getValue();

    				if (!oldValue.equals(newValue)) {
                		log.error("WE UPDATE DIFFERENT VALUE in syslist entry ! entry -> '" + oldValue + "'/'" + newValue + "', "
                				+ entry.getCodeListId() + "/" + entry.getDomainId() + "/" + entry.getLangId() + " (oldValue/newValue, listId/entryId/language)");
    				}

    				// UPDATE
    				psUpdate.setString(1, newValue);
    				psUpdate.setLong(2, entry.getCodeListId());
    				psUpdate.setLong(3, entry.getDomainId());
    				psUpdate.setString(4, langString);
    				int numUpdated = psUpdate.executeUpdate();
    				if (numUpdated > 0) {
                		log.debug("UPDATED " + numUpdated + " entry -> '" + oldValue + "'/'" + newValue + "', "
                				+ entry.getCodeListId() + "/" + entry.getDomainId() + "/" + entry.getLangId() + " (oldValue/newValue, listId/entryId/language)");
    				} else {
                		log.error("PROBLEMS UPDATING " + numUpdated + " entry: " + entry.getCodeListId() + "/" +
                		entry.getDomainId() + "/" + entry.getLangId() + "/" + entry.getValue() + " (listId/entryId/language/value)");
    				}
    			}
    			rs.close();

    			if (entryCount == 0) {
    				// INSERT
    				psInsert.setLong(1, getNextId());
    				psInsert.setLong(2, entry.getCodeListId());
    				psInsert.setLong(3, entry.getDomainId());
    				psInsert.setString(4, langString);
    				psInsert.setString(5, entry.getValue());
    				int numInserted = psInsert.executeUpdate();
    				if (numInserted > 0) {
    					String msg = "ADDED " + numInserted + " NEW entry: " + entry.getCodeListId() + "/" +
    	                	entry.getDomainId() + "/" + entry.getLangId() + "/" + entry.getValue() + " (listId/entryId/language/value)";
    					if ("de".equals(langString) || "en".equals(langString)) {
                    		log.info("NEW SYSLIST ENTRY -> " + msg);
    					} else {
                    		log.debug("NEW LANG (iso, req_value) SYSLIST ENTRY -> " + msg);
    					}
    				} else {
                		log.error("PROBLEMS ADDING NEW entry: listId/entryId/language/value = " + entry.getCodeListId() + "/" +
                		entry.getDomainId() + "/" + entry.getLangId() + "/" + entry.getValue());
    				}
    			}    			
            }
        }
        psSelect.close();
        psUpdate.close();
        psInsert.close();

		log.info("Updating sys_list from file to match REPO ! ... done\n");
    }

	private void updateObjectUse() throws Exception {
		log.info("\nUpdating object_use...");

		log.info("Transfer old 'terms_of_use' as free entry to new 'terms_of_use_key/_value' ...");
		
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
//			log.debug("Object " + objUuid + " updated terms_of_use: '" + 
			log.debug("Updated terms_of_use: '" + termsOfUseText + "' --> '-1'/'" + termsOfUseVarchar + "'");
		}
		rs.close();
		st.close();
		psUpdate.close();

		log.info("Updated " + numProcessed + " entries... done");
		log.info("Updating object_use... done\n");
	}

	private void updateObjectConformity() throws Exception {
		log.info("\nUpdating object_conformity...");

		log.info("Transfer old 'specification' as free entry to new 'specification_key/_value' ...");

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
			log.debug("Updated specification: '" + specification + "' --> '-1'/'" + specification + "'");
		}
		rs.close();
		st.close();
		psUpdate.close();

		log.info("Updated " + numProcessed + " entries... done");
		log.info("Updating object_conformity... done\n");
	}

	private void updateT011ObjServOpPlatform() throws Exception {
		log.info("\nUpdating t011_obj_serv_op_platform...");

		log.info("Transfer old 'platform' to new 'platform_key/_value' via syslist " +
				SYSLIST_ID_OPERATION_PLATFORM + "...");

		// first read syslist values for mapping old free value to syslist value !
		Map<String, Integer> compareNameToKeyMap = new HashMap<String, Integer>();
		Map<Integer, String> platformKeyToNameyMap = new HashMap<Integer, String>();
		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=" + SYSLIST_ID_OPERATION_PLATFORM +
				" and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				String myValue = rs.getString("name");
				Integer myKey = rs.getInt("entry_id");
				platformKeyToNameyMap.put(myKey, myValue);
				// for comparison, we use lower case and remove blanks !
				compareNameToKeyMap.put(myValue.toLowerCase().replace(" ", ""), myKey);
			}
		}
		rs.close();
		st.close();
		
		// Then map former values to new syslist. We do NOT update search index (irgendwann reicht's ;)

		sql = "select id, platform from t011_obj_serv_op_platform";

		// use PreparedStatement to avoid problems when value String contains "'" !!!
		String psSql = "UPDATE t011_obj_serv_op_platform SET " +
				"platform_key = ?, " +
				"platform_value = ? " +
				"WHERE id = ?";		
		PreparedStatement psUpdate = jdbc.prepareStatement(psSql);

		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		int numProcessed = 0;
		int numDeleted = 0;
		while (rs.next()) {
			long id = rs.getLong("id");
			String platform = rs.getString("platform");
			Integer syslistKey = null;

			if (platform != null) {
				// map to new syslist value !
				String platformToCompare = platform.toLowerCase().replaceFirst(" ", "");
				syslistKey = compareNameToKeyMap.get(platformToCompare);
				if (syslistKey == null) {
					// if not found we check whether syslist value contains the old value
					for (Entry<String,Integer> entry : compareNameToKeyMap.entrySet()) {
						if (entry.getKey().contains(platformToCompare)) {
							syslistKey = entry.getValue();
							break;
						}
					}
				}
				if (syslistKey == null) {
					// if not found we check whether the old value contains syslist value
					for (Entry<String,Integer> entry : compareNameToKeyMap.entrySet()) {
						if (platformToCompare.contains(entry.getKey())) {
							syslistKey = entry.getValue();
							break;
						}
					}
				}
			}			
			
			// NOT FOUND, WE DELETE !
			if (syslistKey == null) {
				log.warn("!!! Could not map t011_obj_serv_op_platform.platform '" + platform +
					"' to new syslist " + SYSLIST_ID_OPERATION_PLATFORM +
					", WE DELETE THIS PLATFORM RECORD (no free entries possible) !");
				jdbc.executeUpdate("DELETE FROM t011_obj_serv_op_platform WHERE id=" + id);
				numDeleted++;
				continue;
			}

			// FOUND, we update !
			psUpdate.setInt(1, syslistKey);
			psUpdate.setString(2, platformKeyToNameyMap.get(syslistKey));
			psUpdate.setLong(3, id);
			psUpdate.executeUpdate();

			numProcessed++;
			log.info("Updated platform: '" + platform + "' --> '" + syslistKey + "'/'" + platformKeyToNameyMap.get(syslistKey) + "'");
		}
		rs.close();
		st.close();
		psUpdate.close();

		log.info("Updated " + numProcessed + " entries.");
		log.info("Deleted " + numDeleted + " entries.");
		log.info("Updating t011_obj_serv_op_platform... done\n");
	}

	private void updateDQDatendefizit() throws Exception {
		log.info("\nUpdating object_data_quality 'Datendefizit'...");

		log.info("Transfer 'Datendefizit' value from DQ table (object_data_quality) to DQ field (t011_obj_geo.rec_grade) if field is empty ...");

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
					double fieldValue = rs2.getDouble("rec_grade");
					boolean fieldValueWasNull = rs2.wasNull();

					log.debug("Object id=" + objId + " -> read DQ table value=" + dqTableValue +
						" / value in field Datendefizit=" + (fieldValueWasNull? null : fieldValue));


					if (fieldValueWasNull) {
						try {
							psUpdateDQField.setDouble(1, new Double(dqTableValue));
							psUpdateDQField.setLong(2, objId);
							psUpdateDQField.executeUpdate();
							numProcessed++;
							log.debug("Transferred 'Datendefizit' value '" + dqTableValue +
								"' from DQ table to field (was empty), obj_id:" + objId);
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

		log.info("Transferred " + numProcessed + " entries... done");

		log.info("Delete 'Datendefizit' values from DQ table (object_data_quality) ...");
		sqlStr = "DELETE FROM object_data_quality where dq_element_id = 110";
		int numDeleted = jdbc.executeUpdate(sqlStr);
		log.debug("Deleted " + numDeleted +	" entries.");

		log.info("Updating object_data_quality 'Datendefizit' ... done\n");
	}

	private void updateDQAbsPosGenauigkeit() throws Exception {
		log.info("\nUpdating object_data_quality 'Absolute Positionsgenauigkeit'...");

		log.info("Transfer 'Absolute Positionsgenauigkeit' values from DQ table (object_data_quality) to moved " +
			"fields 'Höhengenauigkeit' (T011_obj_geo.pos_accuracy_vertical) and 'Lagegenauigkeit (m)' (T011_obj_geo.rec_exact) " +
			"if fields are empty ...");

		// NOTICE: We do NOT update search index due to same values.

		// select all relevant entries in DQ Table
		String sqlSelectDQTable = "select obj_id, name_of_measure_key, result_value from object_data_quality where dq_element_id = 117";

		// select according values in DQ Field
		PreparedStatement psSelectDQFields = jdbc.prepareStatement(
				"SELECT pos_accuracy_vertical, rec_exact FROM t011_obj_geo WHERE obj_id = ?");

		// update according value in DQ Field
		PreparedStatement psUpdateDQFieldLage = jdbc.prepareStatement(
				"UPDATE t011_obj_geo SET " +
				"rec_exact = ? " +
				"WHERE obj_id = ?");
		PreparedStatement psUpdateDQFieldHoehe = jdbc.prepareStatement(
				"UPDATE t011_obj_geo SET " +
				"pos_accuracy_vertical = ? " +
				"WHERE obj_id = ?");

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlSelectDQTable, st);
		int numProcessed = 0;
		while (rs.next()) {
			long objId = rs.getLong("obj_id");
			int dqTableMeasureKey = rs.getInt("name_of_measure_key");
			String dqTableValue = rs.getString("result_value");

			if (dqTableValue != null) {
				// read according value from field
				psSelectDQFields.setLong(1, objId);
				ResultSet rs2 = psSelectDQFields.executeQuery();
				if (rs2.next()) {
					// read field value where to migrate to and check whether was null 
					double lageFieldValue = rs2.getDouble("rec_exact");
					boolean lageFieldValueWasNull = rs2.wasNull();
					double hoeheFieldValue = rs2.getDouble("pos_accuracy_vertical");
					boolean hoeheFieldValueWasNull = rs2.wasNull();

					log.debug("Object id=" + objId + " -> read DQ table value: measureKey=" + dqTableMeasureKey + ", value=" + dqTableValue +
						" / values in fields: Lagegenauigkeit=" + (lageFieldValueWasNull? null : lageFieldValue) +
						", Höhengenauigkeit=" + (hoeheFieldValueWasNull ? null : hoeheFieldValue));

					// transfer Lagegenauigkeit from table to field if field is null
					if (dqTableMeasureKey == syslist7117EntryKeyLagegenauigkeit && lageFieldValueWasNull) {
						try {
							psUpdateDQFieldLage.setDouble(1, new Double(dqTableValue));
							psUpdateDQFieldLage.setLong(2, objId);
							psUpdateDQFieldLage.executeUpdate();
							numProcessed++;
							log.debug("Transferred 'Lagegenauigkeit' value '" + dqTableValue +
								"' from DQ table to field (was empty), obj_id:" + objId);
						} catch (Exception ex) {
							String msg = "Problems transferring 'Lagegenauigkeit' value '" + dqTableValue +
									"' from DQ table as DOUBLE to field, value is lost ! obj_id:" + objId;
							log.error(msg, ex);
							System.out.println(msg);
						}
					}
					
					
					// transfer Höhengenauigkeit  from table to field if field is null
					if (dqTableMeasureKey == syslist7117EntryKeyHoehegenauigkeit && hoeheFieldValueWasNull) {
						try {
							psUpdateDQFieldHoehe.setDouble(1, new Double(dqTableValue));
							psUpdateDQFieldHoehe.setLong(2, objId);
							psUpdateDQFieldHoehe.executeUpdate();
							numProcessed++;
							log.debug("Transferred 'Höhengenauigkeit' value '" + dqTableValue +
								"' from DQ table to field (was empty), obj_id:" + objId);
						} catch (Exception ex) {
							String msg = "Problems transferring 'Höhengenauigkeit' value '" + dqTableValue +
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
		psSelectDQFields.close();
		psUpdateDQFieldLage.close();
		psUpdateDQFieldHoehe.close();

		log.info("Transferred " + numProcessed + " entries... done");

		log.info("Delete 'Absoulte Positionsgenauigkeit' values from DQ table (object_data_quality) ...");
		sqlStr = "DELETE FROM object_data_quality where dq_element_id = 117";
		int numDeleted = jdbc.executeUpdate(sqlStr);
		log.debug("Deleted " + numDeleted +	" entries.");

		log.info("Updating object_data_quality 'Absolute Positionsgenauigkeit' ... done\n");
	}
	// DO NOT MIGRATE ADDRESS roles anymore, see INGRID32-46
/*
	private void updateT012ObjAdr() throws Exception {
		log.info("\nUpdating t012_obj_adr...");

		log.info("Make former 'Auskunft' to new 'Verwalter' if no former 'Datenverantwortung' ...");

		// NOTICE: we also update object search index, so search in IGE works !

		// We read from node to determine working version to update search index ! 
		String sql = "select objNode.id as objNodeId, " +
				"objNode.obj_id as objIdWorking, " +
				"obj.id as objId, obj.obj_uuid, " +
				"objAdr.id as objAdrId, objAdr.type, objAdr.special_name " +
				"from object_node objNode, t01_object obj, t012_obj_adr objAdr " +
				"where objNode.obj_uuid = obj.obj_uuid " +
				"and obj.id = objAdr.obj_id " +
				"and objAdr.special_ref = 505 " +
				"order by objNodeId, objId, objAdrId";

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);

		// here is our current object to process, all data encapsulated in this helper class !
		ObjHelper currentObj = null;
		int numProcessed = 0;

		while (rs.next()) {
			long objNodeId = rs.getLong("objNodeId");
			long objIdWorking = rs.getLong("objIdWorking");
			long objId = rs.getLong("objId");
			String objUuid = rs.getString("obj_uuid");
			long objAdrId = rs.getLong("objAdrId");
			int type = rs.getInt("type");
			String typeValue = rs.getString("special_name");

			// check whether all data of an object is read, then do migration !
			boolean objChange = false;
			if (currentObj != null && currentObj.id != objId) {
				// object changed, process finished object
				objChange = true;
				numProcessed = numProcessed + processT012ObjAdr(currentObj);
			}

			// set up new object
			if (currentObj == null || objChange) {
				currentObj = new ObjHelper(objId, objUuid, objNodeId, objIdWorking);
			}

			// pass new stuff
			currentObj.objAdrs.add(new ObjAdrHelper(objAdrId, type, typeValue));
		}
		// also migrate last object ! not done in loop due to end of loop !
		if (currentObj != null) {
			numProcessed = numProcessed + processT012ObjAdr(currentObj);
		}

		rs.close();
		st.close();

		log.info("Changed " + numProcessed + " former 'Auskunft' relations to 'Verwalter' because no former 'Datenverantwortung' ... done");
		log.info("Updating t012_obj_adr... done\n");
	}
*/
	/** Helper class encapsulating all needed data of a processed object to process ! */
/*
	class ObjHelper {
		long id;
		long nodeId;
		boolean isWorkingObjectOfNode;
		String uuid;
		List<ObjAdrHelper> objAdrs;

		ObjHelper(long id, String uuid, long nodeId, long objIdWorkingVersion) {
			this.id = id;
			this.uuid = uuid;
			this.nodeId = nodeId;
			isWorkingObjectOfNode = (objIdWorkingVersion == id);
			objAdrs = new ArrayList<ObjAdrHelper>();
		}
	}
*/
	/** Helper class encapsulating all needed data of a object address relation ! */
/*
	class ObjAdrHelper {
		long id;
		int type;
		String typeValue;

		ObjAdrHelper(long id, int type, String typeValue) {
			this.id = id;
			this.type = type;
			this.typeValue = typeValue;
		}
	}
*/
	/** Migration: Ist keine Adresse mit der Rolle „Datenverantwortung" hinterlegt, so wird einer vorhandenen Adresse mit der Rolle „Auskunft" die neue Rolle
	 * „Verwalter" zugewiesen. Andernfalls wird die Adresse in dieser Rolle Auskunft beibehalten und gibt künftig die Auskunftsadresse zu den Daten an 
	 * (wurde bislang bei der Abgabe der Daten über die CSW-Schnittstelle als Auskunftsadresse für Metadaten verwendet).
	 * <br>NOTICE: name of address relations already updated to new values (Datenverantwortung -> Verwalter)!
	 * <b>But not search index, we also update Index !!!</b>
	 * @param obj the object containing the relations
	 * @return the number of updated obj adr relations
	 * @throws Exception
	 */
/*
	private int processT012ObjAdr(ObjHelper obj) throws Exception {
		int numUpdated = 0;
		
		// Search for former "Datenverantwortung" and "Auskunft"
		ObjAdrHelper objAdrDatenverantwortung = null;
		ObjAdrHelper objAdrAuskunft = null;
		for (ObjAdrHelper objAdr : obj.objAdrs) {
			if (objAdr.type == syslist505EntryKeyDatenverantwortung) {
				objAdrDatenverantwortung = objAdr;
			}
			if (objAdr.type == syslist505EntryKeyAuskunft) {
				objAdrAuskunft = objAdr;
			}
		}

		if (objAdrDatenverantwortung == null && objAdrAuskunft != null) {
			log.info("Object '" + obj.uuid + "': make former 'Auskunft' to new 'Verwalter' because no former 'Datenverantwortung'.");

			// first bring our 'Auskunft' helper object up to date, will also be written into search index !
			objAdrAuskunft.type = syslist505EntryKeyDatenverantwortung;
			objAdrAuskunft.typeValue = syslist505EntryValueVerwalter;

			// use PreparedStatement to avoid problems when value String contains "'" !!!
			String psSql = "UPDATE t012_obj_adr SET " +
					"type = ?, " +
					"special_name = ? " +
					"WHERE id = ?";		
			PreparedStatement psUpdate = jdbc.prepareStatement(psSql);
			
			psUpdate.setInt(1, objAdrAuskunft.type);
			psUpdate.setString(2, objAdrAuskunft.typeValue);
			psUpdate.setLong(3, objAdrAuskunft.id);
			numUpdated = psUpdate.executeUpdate();

			log.info("Updated " + numUpdated + " t012_obj_adr id:" + objAdrAuskunft.id + " to key/value -> " + objAdrAuskunft.type + "/" + objAdrAuskunft.typeValue);

			psUpdate.close();
		}

		// then update search index for IGE search. Values of syslist were changed.
		// Node may contain different object versions, index contains data of working version!
		if (obj.isWorkingObjectOfNode) {
			for (ObjAdrHelper objAdr : obj.objAdrs) {
				JDBCHelper.updateObjectIndex(obj.nodeId, objAdr.typeValue, jdbc);
			}
		}
		
		return numUpdated;
	}
*/
	private void updateObjectTypesCatalogue() throws Exception {
		log.info("\nUpdating object_types_catalogue...");

		migrateT011ObjGeoKeyc();
		migrateT011ObjDataPara();

		log.info("Updating object_types_catalogue... done\n");
	}
		
	private void migrateT011ObjGeoKeyc() throws Exception {
		log.info("\nMigrate data from 't011_obj_geo_keyc' to 'object_types_catalogue'...");

		// NOTICE: We do NOT update search index due to same values.

		// select all data from old tables
		String sqlSelectOldData = "SELECT obj.id, objGeoKeyc.line, objGeoKeyc.keyc_key, objGeoKeyc.keyc_value, " +
			"objGeoKeyc.key_date, objGeoKeyc.edition " +
			"FROM t01_object obj, t011_obj_geo objGeo, t011_obj_geo_keyc objGeoKeyc " +
			"WHERE obj.id = objGeo.obj_id " +
			"AND objGeo.id = objGeoKeyc.obj_geo_id " +
			"ORDER BY obj.id, objGeoKeyc.line";
		
		// insert into new table
		PreparedStatement psInsert = jdbc.prepareStatement(
				"INSERT INTO object_types_catalogue " +
				"(id, obj_id, line, title_key, title_value, type_date, type_version) " +
				"VALUES (?,?,?,?,?,?,?)");

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlSelectOldData, st);
		int numProcessed = 0;
		while (rs.next()) {

			long objId = rs.getLong("id");
			int line = rs.getInt("line");
			int titleKey = rs.getInt("keyc_key");
			String titleValue = rs.getString("keyc_value");
			String date = rs.getString("key_date");
			String version = rs.getString("edition");
			
					
			psInsert.setLong(1, getNextId());
			psInsert.setLong(2, objId);
			psInsert.setInt(3, line);
			psInsert.setInt(4, titleKey);
			psInsert.setString(5, titleValue);
			psInsert.setString(6, date);
			psInsert.setString(7, version);
			psInsert.executeUpdate();

			numProcessed++;
			log.debug("Transferred entry from 't011_obj_geo_keyc' to 'object_types_catalogue': " +
				"objId=" + objId + " -> " + line + "/" + titleKey + "/" + titleValue + "/" + date + "/" + version);
		}
		rs.close();
		st.close();
		psInsert.close();

		log.info("Transferred " + numProcessed + " entries... done");
		log.info("Migrate data from 't011_obj_geo_keyc' to 'object_types_catalogue' ... done\n");
	}

	private void migrateT011ObjDataPara() throws Exception {
		log.info("\nAdd default entry in 'object_types_catalogue' for data from 't011_obj_data_para'...");

		// NOTICE: We do NOT update search index !!!

		// select all data from old tables
		String sqlSelectOldData = "SELECT distinct obj.id " +
			"FROM t01_object obj, t011_obj_data_para objDataPara " +
			"WHERE obj.id = objDataPara.obj_id " +
			"ORDER BY obj.id";
		
		// insert into new table
		PreparedStatement psInsert = jdbc.prepareStatement(
				"INSERT INTO object_types_catalogue " +
				"(id, obj_id, line, title_key, title_value, type_date, type_version) " +
				"VALUES (?,?,?,?,?,?,?)");

		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sqlSelectOldData, st);
		int numProcessed = 0;
		while (rs.next()) {

			long objId = rs.getLong("id");
			int line = 1;
			int titleKey = -1;
			String titleValue = "unknown";
			String date = "20060501000000000";
			String version = null;
					
			psInsert.setLong(1, getNextId());
			psInsert.setLong(2, objId);
			psInsert.setInt(3, line);
			psInsert.setInt(4, titleKey);
			psInsert.setString(5, titleValue);
			psInsert.setString(6, date);
			psInsert.setString(7, version);
			psInsert.executeUpdate();

			numProcessed++;
			log.debug("Added default 'Objektartenkatalog' to 'object_types_catalogue': " +
				"objId=" + objId + " -> " + line + "/" + titleKey + "/" + titleValue + "/" + date + "/" + version);
		}
		rs.close();
		st.close();
		psInsert.close();

		log.info("Added " + numProcessed + " entries... done");
		log.info("Add default entry in 'object_types_catalogue' for data from 't011_obj_data_para' ... done\n");
	}

	private void updateProfile() throws Exception {
		log.info("\nUpdate Profile in database...");

        // read profile
		String profileXml = readGenericKey(KEY_PROFILE_XML);
		if (profileXml == null) {
			throw new Exception("igcProfile not set !");
		}
        profileMapper = new ProfileMapper();
		profileBean = profileMapper.mapStringToBean(profileXml);			

		updateRubricsAndControls(profileBean);

		updateJavaScript(profileBean);

		// write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
		log.debug("Resulting IGC Profile:" + profileXml);
		setGenericKey(KEY_PROFILE_XML, profileXml);        	

		log.info("Update Profile in database... done\n");
	}

	/** Manipulate structure of rubrics / controls, NO Manipulation of JS.
	 * Also removes/adds controls
	 */
	private void updateRubricsAndControls(ProfileBean profileBean) {
		log.info("Move rubric 'Verschlagwortung' after rubric 'Allgemeines'");
		int index = MdekProfileUtils.findRubricIndex(profileBean, "general");
		Rubric rubric = MdekProfileUtils.removeRubric(profileBean, "thesaurus");
		MdekProfileUtils.addRubric(profileBean, rubric, index+1);

		log.info("Move control 'INSPIRE-Themen' from 'Allgemeines' to 'Verschlagwortung'");
    	Controls control = MdekProfileUtils.removeControl(profileBean, "uiElement5064");
		rubric = MdekProfileUtils.findRubric(profileBean, "thesaurus");
		MdekProfileUtils.addControl(profileBean, control, rubric, 0);

		log.info("Move control 'Datendefizit' from 'Fachbezug - Klasse 1' to 'Datenqualität'");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement3565");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1DQ");
		MdekProfileUtils.addControl(profileBean, control, rubric, 0);

		log.info("Move control 'Höhengenauigkeit' from 'Fachbezug - Klasse 1' to 'Datenqualität'");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement5069");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1DQ");
		MdekProfileUtils.addControl(profileBean, control, rubric, 1);

		log.info("Move control 'Lagegenauigkeit' from 'Fachbezug - Klasse 1' to 'Datenqualität'");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement3530");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1DQ");
		MdekProfileUtils.addControl(profileBean, control, rubric, 2);
		
		log.info("Remove DQ table control 'Datendefizit' from 'Datenqualität'");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement7510");

		log.info("Remove DQ table control 'Absoulte Positionsgenauigkeit' from 'Datenqualität'");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement7517");

		log.info("Move control 'Geo-Information/Karte - Sachdaten/Attributinformation' after 'Schlüsselkatalog'");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElement5070");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass1");
		index = MdekProfileUtils.findControlIndex(profileBean, rubric, "uiElement3535");
		MdekProfileUtils.addControl(profileBean, control, rubric, index+1);

		log.info("Add new LEGACY control 'Datensammlung/Datenbank - Fachbezug - Objektartenkatalog' before 'Inhalte der Datensammlung/Datenbank'");
    	control = new Controls();
        control.setIsLegacy(true);
        control.setId("uiElement3109");
        control.setIsMandatory(false);
        control.setIsVisible("optional");
    	rubric = MdekProfileUtils.findRubric(profileBean, "refClass5");
    	// add before 'Inhalte der Datensammlung/Datenbank'
		index = MdekProfileUtils.findControlIndex(profileBean, rubric, "uiElement3110");
		MdekProfileUtils.addControl(profileBean, control, rubric, index);

		log.info("Move control 'Geodatendienst - Operationen' before 'Erstellungsmaßstab', always show");
    	control = MdekProfileUtils.removeControl(profileBean, "uiElementN004");
        control.setIsVisible("show");
		rubric = MdekProfileUtils.findRubric(profileBean, "refClass3");
		index = MdekProfileUtils.findControlIndex(profileBean, rubric, "uiElementN023");
		MdekProfileUtils.addControl(profileBean, control, rubric, index);
	}

	/** Manipulate JS in Controls */
	private void updateJavaScript(ProfileBean profileBean) {
		// tags for marking newly added javascript code (for later removal)
		String startTag = "\n// START 3.2.0 update\n";
		String endTag = "// 3.2.0 END\n";

		//------------- 'Sprache der Ressource'
		log.info("'Sprache der Ressource'(uiElement5042): hide in 'Geodatendienst', make optional in classes 'Organisationenseinheit' + 'Vorhaben' + 'Informationssystem'");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

		//------------- 'Zeichensatz des Datensatzes'
		log.info("'Zeichensatz des Datensatzes'(uiElement5043): only in 'Geo-Information/Karte', then optional");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

		//------------- 'ISO-Themenkategorie'
		log.info("'ISO-Themenkategorie'(uiElement5060): only in 'Geo-Information/Karte', then mandatory");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);
		
		//------------- 'INSPIRE-Themen'
		log.info("'INSPIRE-Themen'(uiElement5064): mandatory in 'Geo-Information/Karte', optional in classes 'Geodatendienst' + 'Informationssystem/Dienst/Anwendung' + 'Datensammlung/Datenbank'");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);
		
		//------------- 'INSPIRE-relevanter Datensatz'
		log.info("'INSPIRE-relevanter Datensatz'(uiElement6000): only in 'Geo-Information/Karte' + 'Geodatendienst' + 'Dienst/Anwendung/Informationssystem', then always show");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

		//------------- show/hide Rubrik 'Datenqualität' via JS in first Control 'Datendefizit'
		log.info("show/hide Rubrik 'Datenqualität'(refClass1DQ) via JS in 'Datendefizit'(uiElement3565): only show rubric when 'Geo-Information/Karte'");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

		//------------- 'Geo-Information/Karte - Sachdaten/Attributinformation' on input make 'Schlüsselkatalog' mandatory
		log.info("'Sachdaten/Attributinformation'(uiElement5070): on input make 'Schlüsselkatalog'(uiElement3535) mandatory");
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
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

		//------------- 'Datensammlung/Datenbank - Inhalte der Datensammlung/Datenbank' on input make 'Objektartenkatalog' mandatory
		log.info("'Inhalte der Datensammlung/Datenbank'(uiElement3110): on input make 'Objektartenkatalog'(uiElement3109) mandatory");
    	control = MdekProfileUtils.findControl(profileBean, "uiElement3110");
		jsCode = startTag +
"// make 'Objektartenkatalog' mandatory on input 'Inhalte der Datensammlung/Datenbank'\n" +
"function uiElement3110InputHandler() {\n" +
"  if (UtilGrid.getTableData(\"ref5dbContent\").length !== 0) {\n" +
"    UtilUI.setMandatory(\"uiElement3109\");\n" +
"  } else {\n" +
"    UtilUI.setOptional(\"uiElement3109\");\n" +
"  }\n" +
"}\n" +
"dojo.connect(UtilGrid.getTable(\"ref5dbContent\"), \"onDataChanged\", uiElement3110InputHandler);\n"
+ endTag;
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

		//------------- 'Nutzungsbedingungen' - remove Publishable JS call (changed from table to textfield)
		log.info("'Nutzungsbedingungen' (uiElementN026): remove availabilityUsePublishable JS");
    	control = MdekProfileUtils.findControl(profileBean, "uiElementN026");
		MdekProfileUtils.removeAllScriptedProperties(control);

		//------------- 'Geodatendienst - Operationen' check table before publish
		log.info("'Geodatendienst - Operationen'(uiElementN004): check table before publish");
    	control = MdekProfileUtils.findControl(profileBean, "uiElementN004");
		jsCode = startTag +
"// check the content of the operation table before publish\n" +
"dojo.subscribe(\"/onBeforeObjectPublish\", function(/*Array*/notPublishableIDs) {\n" +
"    ref3OperationPublishable(notPublishableIDs);\n" +
"});\n"
+ endTag;
		MdekProfileUtils.addToScriptedProperties(control, jsCode);

	}

	private void cleanUpDataStructure() throws Exception {
		log.info("\nCleaning up datastructure -> CAUSES COMMIT ! ...");

		log.info("Drop column 'terms_of_use' from table 'object_use' ...");
		jdbc.getDBLogic().dropColumn("terms_of_use", "object_use", jdbc);

		log.info("Drop columns 'specification', 'publication_date' from table 'object_conformity' ...");
		jdbc.getDBLogic().dropColumn("specification", "object_conformity", jdbc);
		jdbc.getDBLogic().dropColumn("publication_date", "object_conformity", jdbc);

		log.info("Drop table 't011_obj_geo_keyc' ...");
		jdbc.getDBLogic().dropTable("t011_obj_geo_keyc", jdbc);

		log.info("Drop columns 'datatype_key', 'datatype_value, 'volume', 'icon', 'icon_text' from table 't017_url_ref' ...");
		jdbc.getDBLogic().dropColumn("datatype_key", "t017_url_ref", jdbc);
		jdbc.getDBLogic().dropColumn("datatype_value", "t017_url_ref", jdbc);
		jdbc.getDBLogic().dropColumn("volume", "t017_url_ref", jdbc);
		jdbc.getDBLogic().dropColumn("icon", "t017_url_ref", jdbc);
		jdbc.getDBLogic().dropColumn("icon_text", "t017_url_ref", jdbc);

		log.info("Drop column 'platform' from table 't011_obj_serv_op_platform' ...");
		jdbc.getDBLogic().dropColumn("platform", "t011_obj_serv_op_platform", jdbc);

		log.info("Cleaning up datastructure... done\n");
	}

	private int readCatalogLanguageKey() throws Exception {
		int langKey = -1;
		String sql = "SELECT language_key FROM t03_catalogue";
		try {
			Statement st = jdbc.createStatement();
			ResultSet rs = jdbc.executeQuery(sql, st);
			// has to be there !!!
			rs.next();

			langKey = rs.getInt(1);
			
			rs.close();
			st.close();

		} catch (SQLException e) {
			log.error("Error executing SQL: " + sql, e);
			throw e;
		}

		return langKey;
	}
}
