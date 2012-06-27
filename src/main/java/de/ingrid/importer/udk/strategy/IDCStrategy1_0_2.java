/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.provider.Row;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_2 extends IDCStrategyDefault1_0_2 {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_2.class);
	
	private static final String MY_VERSION = VALUE_IDC_VERSION_102;

	private Integer ADDRESS_TYPE_PERSON = 2;
	private Integer AUSKUNFT_ADDRESS_TYPE = 7;
	private Integer AUSKUNFT_ADDRESS_SPECIAL_REF = 505;
	private String IDX_NAME_THESAURUS = "thesaurus";
	private String IDX_NAME_GEOTHESAURUS = "geothesaurus";

	private ArrayList<String> duplicateEntries;

	// global prepared statements, created once !
	private PreparedStatement psInsertSpatialReference = null;
	private PreparedStatement psInsertSpatialRefValue = null;

	// possible syslist entries for free spatial references
	private List<String> freeSpatialReferenceEntryKeys = null;
	private List<String> freeSpatialReferenceEntryNames = null;
	private List<String> freeSpatialReferenceEntryNamesLowerCase = null;

	// cache for remembering already stored FREE spatial_ref_values ! should be stored only once PER OBJECT !
	private HashMap<String, Long> storedFreeSpatialReferences = new HashMap<String, Long>();
	

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write IDC structure version !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		System.out.print("  Pre processing...");
		preProcess_generic();
		System.out.println("done.");

		System.out.print("  Importing sys_list...");
		// must be processed first because other methods depend on that data
		processSysList();
		System.out.println("done.");
		System.out.print("  Importing t03_catalogue...");
		processT03Catalogue();
		System.out.println("done.");
		System.out.print("  Importing t01_object...");
		processT01Object();
		System.out.println("done.");
		System.out.print("  Importing t02_address...");
		processT02Address();
		System.out.println("done.");
		System.out.print("  Importing t022_adr_adr...");
		processT022AdrAdr();
		System.out.println("done.");
		System.out.print("  Importing t021_communication...");
		processT021Communication();
		System.out.println("done.");
		System.out.print("  Importing t012_obj_obj...");
		processT012ObjObj();
		System.out.println("done.");
		System.out.print("  Importing t012_obj_adr...");
		processT012ObjAdr();
		System.out.println("done.");
		System.out.print("  Importing t04_search...");
		processT04Search();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_project...");
		processT011ObjProject();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_literatur...");
		processT011ObjLiteratur();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_data...");
		processT011ObjData();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_data_param...");
		processT011ObjDataParam();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo...");
		processT011ObjGeo();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_vector...");
		processT011ObjGeoVector();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_symc...");
		processT011ObjGeoSymc();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_topic_cat...");
		processT011ObjGeoTopicCat();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_suppl_info...");
		processT011ObjGeoSupplInfo();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_spatial_ref...");
		processT011ObjGeoSpatialRep();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_scale...");
		processT011ObjGeoScale();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_geo_keyc...");
		processT011ObjGeoKeyc();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv...");
		processT011ObjServ();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv_version...");
		processT011ObjServVersion();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv_operation...");
		processT011ObjServOperation();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv_op_platform...");
		processT011ObjServOpPlatform();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv_op_para...");
		processT011ObjServOpPara();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv_op_depends...");
		processT011ObjServOpDepends();
		System.out.println("done.");
		System.out.print("  Importing t011_obj_serv_op_connpoint...");
		processT011ObjServOpConnpoint();
		System.out.println("done.");
		System.out.print("  Importing t015_legist...");
		processT015Legist();
		System.out.println("done.");
		System.out.print("  Importing t0113_dataset_references...");
		processT0113DatasetReference();
		System.out.println("done.");
		System.out.print("  Importing t0110_avail_format...");
		processT0110AvailFormat();
		System.out.println("done.");
		System.out.print("  Importing t0112_media_operation...");
		processT0112MediaOption();
		System.out.println("done.");
		System.out.print("  Importing t017_url_ref...");
		processT017UrlRef();
		System.out.println("done.");
		System.out.print("  Importing t011_township...");
		processT011Township();
		System.out.println("done.");
		System.out.print("  Importing t019_coordinates...");
		processT019Coordinates();
		System.out.println("done.");
		System.out.print("  Importing t08_attrtyp...");
		processT08AttrTyp();
		System.out.println("done.");
		System.out.print("  Importing t08_attrlist...");
		processT08AttrList();
		System.out.println("done.");
		System.out.print("  Importing t08_attr...");
		processT08Attr();
		System.out.println("done.");
		System.out.print("  Importing t014_info_impart...");
		processT014InfoImpart();
		System.out.println("done.");
		System.out.print("  Importing default address/permission for admin...");
		importDefaultUserdata();
		System.out.println("done.");

		System.out.print("  Post processing...");
		postProcess_generic();
		postProcess_spatialRefCatalogue();
		System.out.println("done.");

		System.out.print("  Set HI/LO table...");
		setHiLoGenerator();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Import finished successfully.");
	}

	protected void preProcess_generic() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Pre processing ...");
		}

		// clean up non existing object relations and orphaned objects !!!!

		boolean checkObjectRelations = true;
		int numCycles = 0;
		while (checkObjectRelations) {
			checkObjectRelations = false;
			numCycles++;
			
			// load object relations and verify ! remove relations between non existing objects !		
			String entityName = "t012_obj_obj";
			int numReadEntities = 0;
			int numRemovedEntities = 0;
			for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
				numReadEntities++;
				Row row = i.next();
				// from uuid exists ?
				if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("object_from_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: object_from_id ('"
							+ row.get("object_from_id") + "') not found in imported data of t01_object. Skip record (" +
							"object_to_id ('" + row.get("object_to_id") + "')).");
					}
					i.remove();
					numRemovedEntities++;
					continue;
				}
				
				// to uuid exists ?
				if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("object_to_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: object_to_id ('" + row.get("object_to_id")
							+ "') not found in imported data of t01_object. Skip record (" +
							"object_from_id ('"	+ row.get("object_from_id") + "')).");
					}
					i.remove();
					numRemovedEntities++;
					continue;
				}
			}

			if (log.isInfoEnabled()) {
				log.info("PREPROCESSING t012_obj_obj: " +
					"cycle " + numCycles + ", read " + numReadEntities + ", removed " + numRemovedEntities);
			}

			// after clear up of object relations verify objects ! remove orphans !
			entityName = "t01_object";
			numReadEntities = 0;
			numRemovedEntities = 0;
			for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
				numReadEntities++;
				Row row = i.next();
				boolean rootNotSet = false;
				if (row.get("root") == null || row.get("root").trim().length() == 0) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in " + entityName + " found: root('" + row.get("root") + 
							"') NOT SET ! obj_id ('" + row.get("obj_id")	+ "'). We check relations and set accordingly !");
					}
					rootNotSet = true;
				}
				// check whether object is referenced as "to object"
				if (rootNotSet || row.get("root").equals("0")) {
					// non root object OR root not set
					int toIdSize = IDCStrategyHelper.getEntityFieldValue(dataProvider, "t012_obj_obj", "object_to_id",
							row.get("obj_id"), "object_to_id").length();
					if (toIdSize == 0) {
						// NOT REFERENCED AS TO OBJECT !
						if (rootNotSet) {
							// we mark object as root !
							if (log.isInfoEnabled()) {
								log.info("Invalid entry: object ('" + row.get("obj_id")	+ "') NOT FOUND in t012_obj_obj.object_to_id. We set as new root object.");
							}
							row.put("root", "1");
						} else {
							if (log.isInfoEnabled()) {
								log.info("Invalid entry (outside the hierarchy) in " + entityName + " found: obj_id ('"
									+ row.get("obj_id")	+ "') not found in t012_obj_obj.object_to_id and root == 0. Skip record.");
							}
							i.remove();
							numRemovedEntities++;
							checkObjectRelations = true;
						}
					} else {
						// REFERENCED AS TO OBJECT !
						if (rootNotSet) {
							// we mark object as NOT root !
							if (log.isInfoEnabled()) {
								log.info("Invalid entry: object ('" + row.get("obj_id")	+ "') FOUND in t012_obj_obj.object_to_id. We DON'T set as root object.");
							}
							row.put("root", "0");							
						}
					}
				}
			}

			if (log.isInfoEnabled()) {
				log.info("PREPROCESSING t01_object: " +
					"cycle " + numCycles + ", read " + numReadEntities + ", removed " + numRemovedEntities);
			}
		}

		if (log.isInfoEnabled()) {
			log.info("Pre processing ... done.");
		}
	}

	protected void processT03Catalogue() throws Exception {

		String entityName = "t03_catalogue";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, partner_name , provider_name, country_code,"
				+ "workflow_control, expiry_duration, create_time, mod_uuid, mod_time, language_code) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t03_catalogue";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("mod_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: mod_id ('" + row.get("mod_id")
								+ "') not found in t02_address. Trying to use create_id instead.");
					}
					if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("create_id")) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: create_id ('" + row.get("create_id")
									+ "') not found in imported data of t02_address.");
						}
					}

				}
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setString(cnt++, row.get("cat_id")); // cat_uuid
				p.setString(cnt++, row.get("catalogue")); // cat_name
				p.setString(cnt++, getImportDescriptor().getIdcPartnerName()); // partner_name
				p.setString(cnt++, getImportDescriptor().getIdcProviderName()); // provider_name
				p.setString(cnt++, IDCStrategyHelper.transCountryCode(row.get("country"))); // country_code
				p.setString(cnt++, "N"); // workflow_control
				p.setNull(cnt++, Types.INTEGER); // expiry_duration
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("create_time"))); // create_time
				String modId = row.get("mod_id");
				if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", modId) == 0) {
					modId = row.get("create_id");
				}
				p.setString(cnt++, modId); // mod_uuid,
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("mod_time"))); // mod_time
				p.setString(cnt++, getCatalogLanguage()); // language_code
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isInfoEnabled()) {
					log.info("Skip record of t03_catalogue (cat_id='" + row.get("cat_id") + "'; mod_type='"
							+ row.get("mod_type") + "')");
				}
				row.clear();
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT01Object() throws Exception {

		duplicateEntries = new ArrayList<String>();

		String entityName = "t01_object";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t01_object (id, obj_uuid, obj_name, org_obj_id, obj_class, "
				+ "obj_descr, cat_id, info_note, avail_access_note, loc_descr, time_from, time_to, "
				+ "time_descr, time_period, time_interval, time_status, time_alle, time_type, "
				+ "publish_id, dataset_alternate_name, dataset_character_set, dataset_usage, "
				+ "data_language_code, metadata_character_set, metadata_standard_name, "
				+ "metadata_standard_version, metadata_language_code, vertical_extent_minimum, "
				+ "vertical_extent_maximum, vertical_extent_unit, vertical_extent_vdatum, fees, "
				+ "ordering_instructions, lastexport_time, expiry_time, work_state, work_version, "
				+ "mark_deleted, create_time, mod_time, mod_uuid, responsible_uuid) "
				+ "VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t01_object";
		jdbc.executeUpdate(sqlStr);
		
		sqlStr = "DELETE FROM full_index_obj";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {

				String duplicateKey = row.get("obj_id");

				// Skip Record ?
				if (row.get("root").equals("0")) {
					int toIdSize = IDCStrategyHelper.getEntityFieldValue(dataProvider, "t012_obj_obj", "object_to_id",
							row.get("obj_id"), "object_to_id").length();
					if (toIdSize == 0) {
						if (log.isInfoEnabled()) {
							log.info("Invalid entry (outside the hierarchy) in " + entityName + " found: obj_id ('"
									+ row.get("obj_id")
									+ "') not found in t012_obj_obj.object_to_id and root == 0. Skip record.");
						}
						row.clear();						
					}
				} else if (duplicateEntries.contains(duplicateKey)) {
					if (log.isInfoEnabled()) {
						log.info("Duplicate entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
								+ "'). Skip record.");
					}
					row.clear();
				} else if (IDCStrategyHelper.getPK(dataProvider, "t03_catalogue", "cat_id", row.get("cat_id")) == 0) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in " + entityName + " found: cat_id ('" + row.get("cat_id")
								+ "') not found in imported data of t03_catalogue. Skip record");
					}
					row.clear();
				}
				
				// if row not cleared process row !
				if (row.get("obj_id") != null) {
					int cnt = 1;
					p.setInt(cnt++, row.getInteger("primary_key")); // id
					p.setString(cnt++, row.get("obj_id")); // obj_uuid
					p.setString(cnt++, row.get("obj_name")); // obj_name
					p.setString(cnt++, row.get("org_id")); // org_obj_id
					JDBCHelper.addInteger(p, cnt++, row.getInteger("obj_class")); // class_id

					String objDescr;
					if (row.get("obj_descr") != null) {
						// check for max length of the underlying text field,
						// take the multi byte characterset into account.
						byte[] bArray = row.get("obj_descr").getBytes("UTF-8");
						if (bArray.length > 65535) {
							objDescr = new String(bArray, 0, 65535, "UTF-8");
						} else {
							objDescr = row.get("obj_descr");
						}
						p.setString(cnt++, objDescr); // obj_descr
					} else {
						p.setString(cnt++, null); // obj_descr
					}
					p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t03_catalogue", "cat_id", row
									.get("cat_id"))); // cat_id
					p.setString(cnt++, row.get("info_note")); // info_note
					p.setString(cnt++, row.get("avail_access_note")); // avail_access_note
					p.setString(cnt++, row.get("loc_descr")); // loc_descr

					HashMap<String, String> timeMap = new HashMap<String, String>();
					timeMap.put("time_from", row.get("time_from"));
					timeMap.put("time_to", row.get("time_to"));
					timeMap.put("time_type", row.get("time_type"));
					IDCStrategyHelper.processObjectTimeReference(row.get("obj_id"), timeMap);

					p.setString(cnt++, (String) timeMap.get("time_from")); // time_from
					p.setString(cnt++, (String) timeMap.get("time_to")); // time_to
					p.setString(cnt++, row.get("time_descr")); // time_descr
					JDBCHelper.addInteger(p, cnt++, row.getInteger("time_period")); // time_period
					p.setString(cnt++, row.get("time_interval")); // time_interval
					JDBCHelper.addInteger(p, cnt++, row.getInteger("time_status")); // time_status
					p.setString(cnt++, row.get("time_alle")); // time_alle
					p.setString(cnt++, (String) timeMap.get("time_type")); // time_type
					Integer publishId = row.getInteger("publish_id");
					if (publishId == null) {
						publishId = new Integer(3);
					} else if (publishId.intValue() == 4) {
						publishId = new Integer(3);
					}
					JDBCHelper.addInteger(p, cnt++, publishId); // publish_id
					p.setString(cnt++, row.get("dataset_alternate_name")); // dataset_alternate_name
					JDBCHelper.addInteger(p, cnt++, row.getInteger("dataset_character_set")); // dataset_character_set
					p.setString(cnt++, row.get("dataset_usage")); // dataset_usage
					p.setString(cnt++, IDCStrategyHelper.transLanguageCode(row.get("data_language"))); // data_language_code
					JDBCHelper.addInteger(p, cnt++, row.getInteger("metadata_character_set")); // metadata_character_set
					p.setString(cnt++, row.get("metadata_standard_name")); // metadata_standard_name
					p.setString(cnt++, row.get("metadata_standard_version")); // metadata_standard_version
					p.setString(cnt++, IDCStrategyHelper.transLanguageCode(row.get("metadata_language"))); // metadata_language_code
					JDBCHelper.addDouble(p, cnt++, row.getDouble("vertical_extent_minimum")); // vertical_extent_minimum
					JDBCHelper.addDouble(p, cnt++, row.getDouble("vertical_extent_maximum")); // vertical_extent_maximum

					JDBCHelper.addInteger(p, cnt++, row.getInteger("vertical_extent_unit")); // vertical_extent_unit
					
					// map old codelist to new codelist, log inconsistencies
					Integer oldKeyList101 = row.getInteger("vertical_extent_vdatum");
					Integer newKeyList101 = oldKeyList101;
					if (oldKeyList101 != null) {
						newKeyList101 = mapOldKeyToNewKeyList101.get(oldKeyList101);
						if (newKeyList101 == null) {
							if (log.isDebugEnabled()) {
								log.debug("Invalid entry in " + entityName + " found: vertical_extent_vdatum ('" + oldKeyList101
										+ "') not found in syslist 101, is set to null. obj_uuid=" + row.get("obj_id"));
							}
						}
					}
					
					JDBCHelper.addInteger(p, cnt++, newKeyList101); // vertical_extent_vdatum
					p.setString(cnt++, row.get("fees")); // fees,
					p.setString(cnt++, row.get("ordering_instructions")); // ordering_instructions
					p.setString(cnt++, ""); // lastexport_time
					p.setString(cnt++, ""); // expiry_time
					p.setString(cnt++, "V"); // work_state
					p.setInt(cnt++, 0); // work_version
					p.setString(cnt++, "N"); // mark_deleted,
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("create_time"))); // create_time,
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("mod_time"))); // mod_time,
					String modId = row.get("mod_id");
					// log invalid address references (user addresses)
					if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", modId) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: mod_id ('" + modId
									+ "') not found in imported data of t02_address. Trying to use create_id instead.");
						}

						// we use create_id instead
						modId = row.get("create_id");
						
						// but log if also not valid (will be fixed in post processing)
						if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("create_id")) == 0) {
							if (log.isDebugEnabled()) {
								log.debug("Invalid entry in " + entityName + " found: create_id ('" + row.get("create_id")
										+ "') not found in imported data of t02_address.");
							}
						}
					}
					p.setString(cnt++, modId); // mod_uuid,
					p.setString(cnt++, modId); // responsible_uuid
					try {
						p.executeUpdate();
						duplicateEntries.add(duplicateKey);
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}
					
					// create and update index data
					dataProvider.setId(dataProvider.getId() + 1);					
					JDBCHelper.createObjectIndex(dataProvider.getId(), row.getInteger("primary_key"), "full", jdbc);
					dataProvider.setId(dataProvider.getId() + 1);					
					JDBCHelper.createObjectIndex(dataProvider.getId(), row.getInteger("primary_key"), IDX_NAME_THESAURUS, jdbc);
					dataProvider.setId(dataProvider.getId() + 1);					
					JDBCHelper.createObjectIndex(dataProvider.getId(), row.getInteger("primary_key"), IDX_NAME_GEOTHESAURUS, jdbc);

					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("obj_id"), jdbc); // T01Object.objUuid
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("obj_name"), jdbc); // T01Object.objName
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("org_id"), jdbc); // T01Object.orgObjId
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("objDescr"), jdbc); // T01Object.objDescr
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("info_note"), jdbc); // T01Object.infoNote
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("avail_access_note"), jdbc); // T01Object.availAccessNote
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("loc_descr"), jdbc); // T01Object.locDescr
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("time_descr"), jdbc); // T01Object.timeDescr
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("dataset_alternate_name"), jdbc); // T01Object.datasetAlternateName
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("dataset_usage"), jdbc); // T01Object.datasetUsage
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("metadata_standard_name"), jdbc); // T01Object.metadataStandardName
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("metadata_standard_version"), jdbc); // T01Object.metadataStandardVersion
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("fees"), jdbc); // T01Object.fees
					JDBCHelper.updateObjectIndex(row.getInteger("primary_key"), row.get("ordering_instructions"), jdbc); // T01Object.orderingInstructions
					
				}

			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isDebugEnabled()) {
					log.debug("Skip record of t01_object (obj_id='" + row.get("obj_id") + "'; mod_type='"
							+ row.get("mod_type") + "')");
				}
				row.clear();
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT02Address() throws Exception {

		duplicateEntries = new ArrayList<String>();
		String entityName = "t02_address";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t02_address (id, adr_uuid, org_adr_id, "
				+ "adr_type, institution, lastname, firstname, address_value, address_key, title_value, title_key, "
				+ "street, postcode, postbox, postbox_pc, city, country_code, job, "
				+ "descr, lastexport_time, expiry_time, work_state, work_version, "
				+ "mark_deleted, create_time, mod_time, mod_uuid, responsible_uuid) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t02_address";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "DELETE FROM full_index_addr";
		jdbc.executeUpdate(sqlStr);
		
		final List<String> allowedSpecialRefTitleEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefTitleEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefTitleEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=4305 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefTitleEntryNames.add(rs.getString("name"));
				allowedSpecialRefTitleEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefTitleEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		final List<String> allowedSpecialRefAddressEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefAddressEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefAddressEntryNamesLowerCase = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=4300 and lang_id='" + getCatalogLanguage() + "'";
		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefAddressEntryNames.add(rs.getString("name"));
				allowedSpecialRefAddressEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefAddressEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String duplicateKey = row.get("adr_id");
				if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("mod_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: mod_id ('" + row.get("mod_id")
								+ "') not found in imported data of t02_address. Trying to use create_id instead.");
					}
					if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("create_id")) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: create_id ('" + row.get("create_id")
									+ "') not found in imported data of t02_address.");
						}
					}
				}
				if (row.get("root").equals("0")
						&& IDCStrategyHelper.getEntityFieldValue(dataProvider, "t022_adr_adr", "adr_to_id",
								row.get("adr_id"), "adr_to_id").length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: adr_id ('" + row.get("adr_id")
								+ "') not found in t022_adr_adr and root == 0. Skip record.");
					}
					row.clear();
				} else if (duplicateEntries.contains(duplicateKey)) {
					if (log.isInfoEnabled()) {
						log.info("Duplicate entry in " + entityName + " found: adr_id ('" + row.get("adr_id")
								+ "'). Skip record.");
					}
					row.clear();
				} else if (IDCStrategyHelper.getPK(dataProvider, "t03_catalogue", "cat_id", row.get("cat_id")) == 0) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in " + entityName + " found: cat_id ('" + row.get("cat_id")
								+ "') not found in imported data of t03_catalogue. Skip record.");
					}
					row.clear();
				} else {
					int cnt = 1;
					p.setInt(cnt++, row.getInteger("primary_key")); // id
					p.setString(cnt++, row.get("adr_id")); // adr_uuid
					p.setString(cnt++, row.get("org_adr_id")); // org_adr_id
					Integer addrType = row.getInteger("typ");
					JDBCHelper.addInteger(p, cnt++, addrType); // adr_type
					// institution only if not person ! see http://jira.media-style.com/browse/INGRIDII-146
					String institution = row.get("institution");
					if (ADDRESS_TYPE_PERSON.equals(addrType)) {
						institution = null;
					}
					p.setString(cnt++, institution); // institution						
					p.setString(cnt++, row.get("lastname")); // lastname
					p.setString(cnt++, row.get("firstname")); // firstname

					// try to find entry in syslist
					int entryIndex = -1;
					if (row.get("address") != null) {
						entryIndex = allowedSpecialRefAddressEntryNamesLowerCase.indexOf(row.get("address").toLowerCase());
					}
					String addressValueWritten = row.get("address");
					if (entryIndex != -1) {
						// we set also value from entry !!! necessary for mapping !
						addressValueWritten = allowedSpecialRefAddressEntryNames.get(entryIndex);
						p.setString(cnt++, addressValueWritten); // address_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialRefAddressEntries.get(entryIndex))); // address_key
					} else {
						p.setString(cnt++, addressValueWritten); // address_value
						p.setInt(cnt++, -1); // address_key
					}

					// try to find entry in syslist
					entryIndex = -1;
					if (row.get("title") != null) {
						entryIndex = allowedSpecialRefTitleEntryNamesLowerCase.indexOf(row.get("title").toLowerCase());
					}
					String titleValueWritten = row.get("title");
					if (entryIndex != -1) {
						// we set also value from entry !!! necessary for mapping !
						titleValueWritten = allowedSpecialRefTitleEntryNames.get(entryIndex);
						p.setString(cnt++, titleValueWritten); // title_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialRefTitleEntries.get(entryIndex))); // title_key
					} else {
						p.setString(cnt++, titleValueWritten); // title_value
						p.setInt(cnt++, -1); // title_key
					}

					p.setString(cnt++, row.get("street")); // street
					p.setString(cnt++, row.get("postcode")); // postcode
					p.setString(cnt++, row.get("postbox")); // postbox
					p.setString(cnt++, row.get("postbox_pc")); // postbox_pc
					p.setString(cnt++, row.get("city")); // city
					p.setString(cnt++, IDCStrategyHelper.transCountryCode(row.get("state_id"))); // country_code
					p.setString(cnt++, row.get("job")); // job
					p.setString(cnt++, row.get("descr")); // descr
					p.setString(cnt++, ""); // lastexport_time
					p.setString(cnt++, ""); // expiry_time
					p.setString(cnt++, "V"); // work_state
					p.setInt(cnt++, 0); // work_version
					p.setString(cnt++, "N"); // mark_deleted
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("create_time"))); // create_time
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("mod_time"))); // mod_time
					String modId = row.get("mod_id");
					if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", modId) == 0) {
						modId = row.get("create_id");
					}
					p.setString(cnt++, modId); // mod_uuid,
					p.setString(cnt++, modId); // responsible_uuid

					try {
						p.executeUpdate();
						duplicateEntries.add(duplicateKey);
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}

					// create and update full index
					dataProvider.setId(dataProvider.getId() + 1);
					JDBCHelper.createAddressIndex(dataProvider.getId(), row.getInteger("primary_key"), "full", jdbc);
					dataProvider.setId(dataProvider.getId() + 1);
					JDBCHelper.createAddressIndex(dataProvider.getId(), row.getInteger("primary_key"), "partial", jdbc);

					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("adr_id"), jdbc); // T02Address.adrUuid
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("org_adr_id"), jdbc); // T02Address.orgAdrId
					if (institution != null) {
						JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), institution, jdbc); // T02Address.institution
						JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), institution, "partial", jdbc); // T02Address.institution in partial idx						
					}
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("lastname"), jdbc); // T02Address.lastname
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("lastname"), "partial", jdbc); // T02Address.lastname in partial idx
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("firstname"), jdbc); // T02Address.firstname
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("firstname"), "partial", jdbc); // T02Address.firstname in partial idx 
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), addressValueWritten, jdbc); // T02Address.addressValue
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), titleValueWritten, jdbc); // T02Address.titleValue
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("street"), jdbc); // T02Address.street
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("postcode"), jdbc); // T02Address.postcode
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("postbox"), jdbc); // T02Address.postbox
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("postbox_pc"), jdbc); // T02Address.postboxPc
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("city"), jdbc); // T02Address.city
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("job"), jdbc); // T02Address.job
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("descr"), jdbc); // T02Address.descr
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("descr"), "partial", jdbc); // T02Address.descr in partial idx
					
				}
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isDebugEnabled()) {
					log.debug("Skip record of " + entityName + " (adr_id='" + row.get("adr_id") + "'; mod_type='"
							+ row.get("mod_type") + "')");
				}
				row.clear();
			}

		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT022AdrAdr() throws Exception {

		String entityName = "t022_adr_adr";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO address_node (id, addr_uuid, addr_id, addr_id_published, fk_addr_uuid) VALUES (?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM address_node";
		jdbc.executeUpdate(sqlStr);

		ArrayList<String> storedEntries = new ArrayList<String>();

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_from_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: adr_from_id ('" + row.get("adr_from_id")
							+ "') not found in imported data of t02_address.");
				}
			} else if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_to_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: adr_to_id ('" + row.get("adr_to_id")
							+ "') not found in imported data of t02_address.");
				}
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (storedEntries.contains(row.get("adr_to_id"))) {
					if (log.isDebugEnabled()) {
						log.debug("Duplicate entry for adr_to_id in " + entityName + " ('" + row.get("adr_to_id")
								+ "', mod_type='" + row.get("mod_type") + "'). Skip import.");
					}
				} else {
					int cnt = 1;
					p.setInt(cnt++, row.getInteger("primary_key")); // id
					p.setString(cnt++, row.get("adr_to_id")); // addr_uuid
					p.setInt(cnt++, IDCStrategyHelper
							.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_to_id"))); // addr_id
					p.setInt(cnt++, IDCStrategyHelper
							.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_to_id"))); // addr_id_published
					p.setString(cnt++, row.get("adr_from_id")); // fk_addr_uuid
					try {
						p.executeUpdate();
						storedEntries.add(row.get("adr_to_id"));
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}

				}
			}
		}

		// insert root objects into address_node
		for (Iterator<Row> i = dataProvider.getRowIterator("t02_address"); i.hasNext();) {
			Row row = i.next();
			if (row.getInteger("root") != null && row.getInteger("root") != 0 && row.get("mod_type") != null
					&& !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long id = dataProvider.getId();
				id++;
				p.setLong(cnt++, id); // id
				dataProvider.setId(id);
				p.setString(cnt++, row.get("adr_id")); // addr_uuid
				p.setInt(cnt++, row.getInteger("primary_key")); // addr_id
				p.setInt(cnt++, row.getInteger("primary_key")); // addr_id_published
				p.setString(cnt++, null); // fk_addr_uuid
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}

		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT021Communication() throws Exception {

		String entityName = "t021_communication";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t021_communication (id, adr_id, line, commtype_value, commtype_key, comm_value, descr) VALUES (?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t021_communication";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=4430 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: adr_id ('" + row.get("adr_id")
							+ "') not found in imported data of t02_address. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_id"))); // adr_id
				p.setInt(cnt++, row.getInteger("line")); // line
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("comm_type") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("comm_type").toLowerCase());
				}
				String valueWritten = row.get("comm_type");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // commtype_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // commtype_key
				} else {
					p.setString(cnt++, valueWritten); // commtype_value
					p.setInt(cnt++, -1); // commtype_key
				}
				p.setString(cnt++, row.get("comm_value")); // comm_value
				p.setString(cnt++, row.get("descr")); // descr
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long addrId = IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("obj_id"));
				JDBCHelper.updateAddressIndex(addrId, row.get("comm_value"), jdbc); // T021Communication.commValue
				JDBCHelper.updateAddressIndex(addrId, row.get("descr"), jdbc); // T021Communication.descr
				
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT012ObjObj() throws Exception {

		String entityName = "t012_obj_obj";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrObjectNode = "INSERT INTO object_node (id, obj_uuid, obj_id, obj_id_published, fk_obj_uuid) VALUES "
				+ "(?, ?, ?, ?, ?)";
		String pSqlStrObjectReference = "INSERT INTO object_reference (id, obj_from_id, obj_to_uuid, line, special_ref, special_name, descr) VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement pSqlObjectNode = jdbc.prepareStatement(pSqlStrObjectNode);
		PreparedStatement pSqlObjectReference = jdbc.prepareStatement(pSqlStrObjectReference);

		sqlStr = "DELETE FROM object_node";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM object_reference";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();
		final List<String> importedObjectNodes = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=2000 " +
				"AND entry_id IN (3100, 3210, 3345, 3515, 3520, 3535, 3555, 3570, 5066) " +
				"and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();

		boolean skipRecord = false;

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("object_from_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: object_from_id ('"
							+ row.get("object_from_id") + "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("object_to_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: object_to_id ('" + row.get("object_to_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				if (row.getInteger("typ") != null && row.getInteger("typ") == 0) {
					if (importedObjectNodes.contains(row.get("object_to_id"))) {
						if (log.isDebugEnabled()) {
							log.debug("ObjectNode for obj_id='" + row.get("object_to_id") + "' already imported!");
						}
					} else {
						// structure
						pSqlObjectNode.setInt(cnt++, row.getInteger("primary_key")); // id
						pSqlObjectNode.setString(cnt++, row.get("object_to_id")); // object_uuid
						pSqlObjectNode.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
								.get("object_to_id"))); // object_id
						pSqlObjectNode.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
								.get("object_to_id"))); // object_id_published
						pSqlObjectNode.setString(cnt++, row.get("object_from_id")); // fk_obj_uuid
						
						importedObjectNodes.add(row.get("object_to_id"));
						
						try {
							pSqlObjectNode.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSqlObjectNode.toString(), e);
							throw e;
						}
					}
				} else if (row.getInteger("typ") != null && row.getInteger("typ") == 1) {
					skipRecord = false;
					pSqlObjectReference.setInt(cnt++, row.getInteger("primary_key")); // id
					pSqlObjectReference.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
							.get("object_from_id"))); // object_from_uuid
					pSqlObjectReference.setString(cnt++, row.get("object_to_id")); // object_to_uuid
					pSqlObjectReference.setInt(cnt++, row.getInteger("line")); // line

					// try to find entry in sys_list
					int entryIndex = -1;

					// first analyze special_ref
					boolean hasSpecialRef = false;
					if (row.get("special_ref") != null && row.getInteger("special_ref") != 0) {
						hasSpecialRef = true;
						entryIndex = allowedSpecialRefEntries.indexOf(row.get("special_ref"));
					}

					// then analyze special_name if necessary
					boolean hasSpecialName = false;
					if (row.get("special_name") != null && row.get("special_name").trim().length() > 0) {
						hasSpecialName = true;
						
						// analyze special name if entry not found yet
						if (entryIndex == -1) {
							entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("special_name").toLowerCase());

							if (entryIndex != -1) {
								// fits object class to syslist entry ? if not then do free entry !
								
								// get according key (special_ref)
								int specialRef = Integer.parseInt(allowedSpecialRefEntries.get(entryIndex));
								// get object class
								Integer objClass = IDCStrategyHelper.getEntityFieldValueAsInteger(dataProvider, "t01_object", "obj_id", row.get("obj_id"), "obj_class");
								if (objClass == null) {
									entryIndex = -1;
								} else if (specialRef == 3210 && objClass == 2) {
									// "Basisdaten" of wrong object class, we set to "Basisdaten" of correct class
									entryIndex = allowedSpecialRefEntries.indexOf("3345");
								} else if (specialRef == 3345 && objClass == 3) {
									// "Basisdaten" of wrong object class, we set to "Basisdaten" of correct class
									entryIndex = allowedSpecialRefEntries.indexOf("3210");
								} else if (specialRef == 3210 && objClass == 3) {
									// correct index
								} else if (specialRef == 3345 && objClass == 2) {
									// correct index
								} else if (specialRef == 3100 && objClass == 5) {
									// correct index
								} else if ((specialRef == 3515 || specialRef == 3520 || specialRef == 3535 || specialRef == 3555 || specialRef == 3570 || specialRef == 5066) && objClass == 1) {
									// correct index
								} else {
									entryIndex = -1;
								}

								// log whether entry key was determined via name !
								if (entryIndex != -1 && hasSpecialRef) {
									int newSpecialRef = Integer.parseInt(allowedSpecialRefEntries.get(entryIndex));
									log.info("Invalid special_ref '" + row.get("special_ref")
										+ "' found !!! We set new special_ref '" + newSpecialRef + "' detected via special_name='"
										+ row.get("special_name") + "'.");							
								}
							}
						}
					}

					// write special_ref, special_name
					String specialNameWritten = row.get("special_name");
					if (entryIndex != -1) {
						JDBCHelper.addInteger(pSqlObjectReference, cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // special_ref
						// we set also value from entry !!! necessary for mapping !
						specialNameWritten = allowedSpecialRefEntryNames.get(entryIndex);
						pSqlObjectReference.setString(cnt++, specialNameWritten); // special_name
					} else if (hasSpecialName) {
						if (hasSpecialRef) {
							log.info("Invalid special_ref '" + row.get("special_ref")
								+ "' found. Reference will be imported as free entry with special_name='"
								+ row.get("special_name") + "'.");							
						}

						pSqlObjectReference.setInt(cnt++, -1); // special_ref
						pSqlObjectReference.setString(cnt++, specialNameWritten); // special_name
					} else {
						log.error("Invalid special_ref='" + row.get("special_ref") + "' with special_name='"
								+ row.get("special_name") + "' found. We skip record !");
						skipRecord = true;
					}
					
					pSqlObjectReference.setString(cnt++, row.get("descr")); // descr
					if (!skipRecord) {
						try {
							pSqlObjectReference.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSqlObjectReference.toString(), e);
							throw e;
						}
					}
				}
			}
		}
		// insert root objects into object_node
		duplicateEntries = new ArrayList<String>();
		for (Iterator<Row> i = dataProvider.getRowIterator("t01_object"); i.hasNext();) {
			Row row = i.next();
			int cnt = 1;
			if (row.getInteger("root") != null && row.getInteger("root") != 0 && row.get("mod_type") != null
					&& !invalidModTypes.contains(row.get("mod_type"))) {
				long id = dataProvider.getId();
				id++;
				pSqlObjectNode.setLong(cnt++, id); // id
				dataProvider.setId(id);
				pSqlObjectNode.setString(cnt++, row.get("obj_id")); // object_uuid
				pSqlObjectNode.setInt(cnt++, row.getInteger("primary_key")); // object_id
				pSqlObjectNode.setInt(cnt++, row.getInteger("primary_key")); // object_id_published
				pSqlObjectNode.setString(cnt++, null); // fk_obj_uuid
				try {
					pSqlObjectNode.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + pSqlObjectNode.toString(), e);
					throw e;
				}
			}
		}
		pSqlObjectNode.close();
		pSqlObjectReference.close();

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT012ObjAdr() throws Exception {

		String entityName = "t012_obj_adr";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t012_obj_adr (id, obj_id, adr_uuid, type, line, "
				+ "special_ref, special_name, mod_time) VALUES " + "( ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t012_obj_adr";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries505 = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames505 = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=505 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames505.add(rs.getString("name"));
				allowedSpecialRefEntries505.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();

		final List<String> allowedSpecialRefEntries2010 = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames2010 = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase2010 = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=2010 " +
				"AND entry_id IN (3360, 3400, 3410) " +
				"and lang_id='" + getCatalogLanguage() + "'";
		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames2010.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase2010.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries2010.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();

		// for tracking duplicate entries in udk !
		HashMap<String, String> writtenUniqueKeys = new HashMap<String, String>();
		long maxId = 0;

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (row.get("obj_id") == null || row.get("obj_id").length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: obj_id('" + row.get("obj_id") +
								"') not set. Skip record.");
					}
					row.clear();
				} else if (row.get("adr_id") == null || row.get("adr_id").length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: adr_id('" + row.get("adr_id") +
								"') not set. Skip record. obj_id ('" + row.get("obj_id") + "').");
					}
					row.clear();
				} else if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of t01_object. Skip record.");
					}
					row.clear();
				} else if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: adr_id ('" + row.get("adr_id")
								+ "') not found in imported data of t02_address. Skip record. obj_id ('" + row.get("obj_id")	+ "').");
					}
					row.clear();
				} else if (!IDCStrategyHelper.isValidUdkAddressType(row.getInteger("typ"), row.get("special_name")) && row.get("typ") != null && row.getInteger("typ").intValue() != 0) {
					log.info("Invalid entry in " + entityName + " found: typ ('" + row.get("typ")
							+ "') does not correspond with special_name ('" + row.get("special_name")
							+ "'). Skip record. obj_id ('" + row.get("obj_id")	+ "'), adr_id ('" + row.get("adr_id") + "').");
					row.clear();
				} else {
					int currId = row.getInteger("primary_key");
					if (currId > maxId) {
						maxId = currId;
					}
					int cnt = 1;
					p.setInt(cnt++, currId); // id
					int objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
					p.setInt(cnt++, objId); // obj_id
					String adrUuid = row.get("adr_id");
					p.setString(cnt++, adrUuid); // adr_uuid
					
					Integer type = null;
					Integer specialRef = null;
					String specialName = null;
					
					if (row.getInteger("typ") == null || row.getInteger("typ").intValue() == 999 || row.getInteger("typ").intValue() == -1) {
						type = new Integer(-1);
					// if type is valid
					} else if (row.getInteger("typ").intValue() >= 0 && row.getInteger("typ").intValue() <= 9) {
						type = IDCStrategyHelper.transAddressTypeUdk2Idc(row.getInteger("typ"));
						specialRef = new Integer(505);
						specialName = allowedSpecialRefEntryNames505.get(allowedSpecialRefEntries505.indexOf(type.toString()));
					// if typ is invalid
					} else  {
						log.info("Invalid udk address type detected (type='" + row.getInteger("typ")
								+ "', special_name='" + row.get("special_name")
								+ "'). The record will be imported as free entry.");
						type = new Integer(-1);
					}

					// handle 2010 syslist if no entry yet
					if (specialRef == null) {
						// default
						// special ref null -> free entry
						specialName = row.get("special_name");

						if (row.get("special_ref") != null) {
							int entryIndex = allowedSpecialRefEntries2010.indexOf(row.get("special_ref"));
							if (entryIndex != -1) {
								// if special_ref is valid
								type = row.getInteger("special_ref");
								specialRef = new Integer(2010);
								specialName = allowedSpecialRefEntryNames2010.get(entryIndex);
							}
						} else if (row.get("special_name") != null) {
							int entryIndex = allowedSpecialRefEntryNamesLowerCase2010.indexOf(row.get("special_name").toLowerCase());
							if (entryIndex != -1) {
								//	if special_name is in lookup list, check against object classes for valid ids
								int specialReferenceTypeId = Integer.getInteger(allowedSpecialRefEntries2010.get(entryIndex));
								Integer objClass = IDCStrategyHelper.getEntityFieldValueAsInteger(dataProvider, "t01_object", "obj_id", row.get("obj_id"), "obj_class");
								if (objClass != null) {
									if (specialReferenceTypeId == 3360 && objClass.intValue() == 2) {
										type = specialReferenceTypeId;
										specialRef = new Integer(2010);
										specialName = allowedSpecialRefEntryNames2010.get(entryIndex);
									} else if ((specialReferenceTypeId == 3400 || specialReferenceTypeId == 3410)&& objClass.intValue() == 4) {
										type = specialReferenceTypeId;
										specialRef = new Integer(2010);
										specialName = allowedSpecialRefEntryNames2010.get(entryIndex);
									}
								}
							}
						} else {
							log.info("Invalid entry in " + entityName + " found: type('" + row.getInteger("typ")
									+ "'), special_name('" + row.get("special_name")
									+ "'), special_ref('" + row.get("special_ref") + "'). BUT WE WRITE RECORD with these values !");
							specialName = row.get("special_name");
						}
					}

					JDBCHelper.addInteger(p, cnt++, type ); // type
					JDBCHelper.addInteger(p, cnt++, row.getInteger("line") ); // line
					JDBCHelper.addInteger(p, cnt++, specialRef ); // special_ref
					JDBCHelper.addString(p, cnt++, specialName ); // special_name

					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("mod_time"))); // mod_time
					
					// check for duplicate entries
					String key = "" + objId + adrUuid + type;
					if (writtenUniqueKeys.containsKey(key)) {
						log.info("Invalid entry in " + entityName + " found: duplicate entry, objId('" + objId +
								"'), adrUuid('" + adrUuid + "'), type ('" + type + "') found. Skip record.");
						row.clear();						
					} else {
						try {
							p.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + p.toString(), e);
							throw e;
						}
						writtenUniqueKeys.put(key, key);
					}
				}
			}
		}

		// check whether there are objects without auskunft addresses !
		// then set catadmin as auskunft !

		// get "consts"
		String auskunftSpecialName = allowedSpecialRefEntryNames505.get(allowedSpecialRefEntries505.indexOf(AUSKUNFT_ADDRESS_TYPE.toString()));

		// fetch with outer join, so we can process address references of EVERY object !
		sql = "select distinct obj.id, oa.type, oa.special_ref, oa.line " +
			"from t01_object obj left outer join t012_obj_adr oa on obj.id =oa.obj_id " +
			"ORDER BY obj.id, oa.special_ref, oa.type ASC";
		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);

		// process, if an object has no auskunft add it !
		boolean hasAuskunft = true;
		long lastObjId = -1;
		int line = 0;
		while (rs.next()) {
			long currObjId = rs.getLong("id");
			if (currObjId != lastObjId) {
				if (!hasAuskunft) {
					log.info("Invalid entry in " + entityName + " found: no auskunft address !!! objId('" + lastObjId +
						"'). We add catAdmin address as auskunft.");

					// write catadmin as auskunft !
					int cnt = 1;
					p.setLong(cnt++, ++maxId); // id
					p.setLong(cnt++, lastObjId); // obj_id
					p.setString(cnt++, getCatalogAdminUuid()); // adr_uuid
					JDBCHelper.addInteger(p, cnt++, AUSKUNFT_ADDRESS_TYPE ); // type
					JDBCHelper.addInteger(p, cnt++, line ); // line
					JDBCHelper.addInteger(p, cnt++, AUSKUNFT_ADDRESS_SPECIAL_REF ); // special_ref
					JDBCHelper.addString(p, cnt++, auskunftSpecialName ); // special_name
					p.setString(cnt++, null); // mod_time

					try {
						p.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}
				}

				hasAuskunft = false;
				line = 0;
			}

			lastObjId = currObjId;
			Integer currLine = rs.getInt("line");
			if (currLine != null && line < currLine) {
				line = currLine;
			}
			if (!hasAuskunft) {
				boolean typeOk = AUSKUNFT_ADDRESS_TYPE.equals(rs.getInt("type"));
				boolean specialRefOk = AUSKUNFT_ADDRESS_SPECIAL_REF.equals(rs.getInt("special_ref"));
				if (typeOk && specialRefOk) {
					hasAuskunft = true;
				}
			}
		}
		rs.close();
		st.close();
		p.close();

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT04Search() throws Exception {

		String entityName = "t04_search";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSearchtermObj = "INSERT INTO searchterm_obj (id, obj_id, line, searchterm_id) VALUES ( ?, ?, ?, ?)";
		String pSqlStrSearchtermAdr = "INSERT INTO searchterm_adr (id, adr_id, line, searchterm_id) VALUES ( ?, ?, ?, ?)";
		String pSqlStrSearchtermValue = "INSERT INTO searchterm_value (id, type, term, searchterm_sns_id) VALUES ( ?, ?, ?, ?)";
		String pSqlStrSearchtermSns = "INSERT INTO searchterm_sns (id, sns_id, expired_at) VALUES ( ?, ?, ?)";

		PreparedStatement pSearchtermObj = jdbc.prepareStatement(pSqlStrSearchtermObj);
		PreparedStatement pSearchtermAdr = jdbc.prepareStatement(pSqlStrSearchtermAdr);
		PreparedStatement pSearchtermValue = jdbc.prepareStatement(pSqlStrSearchtermValue);
		PreparedStatement pSearchtermSns = jdbc.prepareStatement(pSqlStrSearchtermSns);

		HashMap<String, Long> searchTermValues = new HashMap<String, Long>();
		HashMap<String, Long> searchTermSnsValues = new HashMap<String, Long>();
		ArrayList<String> alreadyImported = new ArrayList<String>();

		sqlStr = "DELETE FROM searchterm_sns";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM searchterm_value";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM searchterm_adr";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM searchterm_obj";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();

			// check for null searchterms !!! (yes this is possible in exported udk data !)
			String searchterm = row.get("searchterm");
			if (searchterm == null || searchterm.trim().length() == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: invalid searchterm('" + searchterm + "') ! " +
							"obj_id ('" + row.get("obj_id") + "'). Skip record.");
				}
				row.clear();
				continue;
			}

			String key = row.get("obj_id") + "_" + row.get("type") + "_" + row.get("searchterm");
			
			if (!alreadyImported.contains(key) && row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				alreadyImported.add(key);
				int cnt = 1;
				// free searchterm object
				if (row.getInteger("type") != null && row.getInteger("type") == 1) {
					// check for invalid record
					if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
									+ "') not found in imported data of t01_object. Skip record.");
						}
						row.clear();
					} else if (row.get("searchterm") == null) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: searchterm is null. Skip record.");
						}
						row.clear();
					} else {
						long pSearchtermValueId;
						// if the term has been stored already, refere to the
						// already stored id
						if (searchTermValues.containsKey(row.get("searchterm").concat("_F"))) {
							pSearchtermValueId = ((Long) searchTermValues.get(row.get("searchterm").concat("_F")))
									.longValue();
						} else {
							// store the search term
							dataProvider.setId(dataProvider.getId() + 1);
							pSearchtermValue.setLong(cnt++, dataProvider.getId()); // id
							pSearchtermValue.setString(cnt++, "F"); // 1 = F, 2
							// = T, 3 =
							// F, 4 = T
							pSearchtermValue.setString(cnt++, row.get("searchterm")); // term
							pSearchtermValue.setNull(cnt++, Types.INTEGER); // searchterm_sns_id
							try {
								pSearchtermValue.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSearchtermValue.toString(), e);
								throw e;
							}
							searchTermValues.put(row.get("searchterm").concat("_F"), new Long(dataProvider.getId()));
							pSearchtermValueId = dataProvider.getId();
						}

						// store the object -> searchterm relation
						cnt = 1;
						dataProvider.setId(dataProvider.getId() + 1);
						pSearchtermObj.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermObj.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
								.get("obj_id"))); // obj_id
						pSearchtermObj.setString(cnt++, row.get("line")); // term
						pSearchtermObj.setLong(cnt++, pSearchtermValueId); // searchterm_id
						try {
							pSearchtermObj.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSearchtermObj.toString(), e);
							throw e;
						}
						
						// update full text index
						JDBCHelper.updateObjectIndex(IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
								.get("obj_id")), row.get("searchterm"), jdbc); // SearchtermValue.term
					}
					// thesaurus searchterm object
				} else if (row.getInteger("type") != null && row.getInteger("type") == 2) {
					// check for invalid record
					if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
									+ "') not found in imported data of t01_object. Skip record.");
						}
						row.clear();
					} else if (IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid", "th_desc_no",
							row.get("th_desc_no"), "th_orig_desc_no").length() == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: th_desc_no ('"
									+ row.get("th_desc_no") + "') not found in thesorigid. Skip record.");
						}
						row.clear();
					} else {
						long pSearchtermValueId;
						long pSearchtermSnsId;
						String snsTopicId = IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid",
								"th_desc_no", row.get("th_desc_no"), "th_orig_desc_no");
						String snsSearchTermCheckIdent = row.get("searchterm").concat("_").concat(snsTopicId);
						// if the term has been stored already, refere to the
						// already stored id
						if (searchTermValues.containsKey(snsSearchTermCheckIdent)) {
							pSearchtermValueId = searchTermValues.get(snsSearchTermCheckIdent).longValue();
						} else {
							if (searchTermSnsValues.containsKey(snsTopicId)) {
								pSearchtermSnsId = searchTermSnsValues.get(snsTopicId).longValue();
							} else {
								// store the new sns topic id in table
								// searchterm_sns
								cnt = 1;
								dataProvider.setId(dataProvider.getId() + 1);
								pSearchtermSns.setLong(cnt++, dataProvider.getId()); // id
								pSearchtermSns.setString(cnt++, "uba_thes_".concat(snsTopicId)); // sns_id
								pSearchtermSns.setNull(cnt++, java.sql.Types.VARCHAR); // expired_at
								try {
									pSearchtermSns.executeUpdate();
								} catch (Exception e) {
									log.error("Error executing SQL: " + pSearchtermSns.toString(), e);
									throw e;
								}
								pSearchtermSnsId = dataProvider.getId();
								searchTermSnsValues.put(snsTopicId, new Long(pSearchtermSnsId));
							}

							// store the search term
							cnt = 1;
							dataProvider.setId(dataProvider.getId() + 1);
							pSearchtermValue.setLong(cnt++, dataProvider.getId()); // id
							pSearchtermValue.setString(cnt++, "T"); // 1 = F, 2
							// = T, 3 =
							// F, 4 = T
							pSearchtermValue.setString(cnt++, row.get("searchterm")); // term
							pSearchtermValue.setLong(cnt++, pSearchtermSnsId); // searchterm_sns_id
							try {
								pSearchtermValue.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSearchtermValue.toString(), e);
								throw e;
							}
							searchTermValues.put(snsSearchTermCheckIdent, new Long(dataProvider.getId()));
							pSearchtermValueId = dataProvider.getId();
						}

						// store the object -> searchterm relation
						cnt = 1;
						dataProvider.setId(dataProvider.getId() + 1);
						pSearchtermObj.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermObj.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
								.get("obj_id"))); // obj_id
						pSearchtermObj.setString(cnt++, row.get("line")); // term
						pSearchtermObj.setLong(cnt++, pSearchtermValueId); // searchterm_id
						try {
							pSearchtermObj.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSearchtermObj.toString(), e);
							throw e;
						}

						// update full text index
						int objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
						JDBCHelper.updateObjectIndex(objId, row.get("searchterm"), jdbc); // SearchtermValue.term in full index
						String snsId = "uba_thes_".concat(snsTopicId);
						JDBCHelper.updateObjectIndex(objId, snsId, jdbc); // SearchtermSns.snsId in full index
						JDBCHelper.updateObjectIndex(objId, snsId, IDX_NAME_THESAURUS, jdbc); // SearchtermSns.snsId in thesaurus index
						
					}
				} else if (row.getInteger("type") != null && row.getInteger("type") == 3) {
					// check for invalid record
					if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("obj_id")) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
									+ "') not found in imported data of t02_address. Skip record.");
						}
						row.clear();
					} else {
						long pSearchtermValueId;
						// if the term has been stored already, refere to the
						// already stored id
						if (searchTermValues.containsKey(row.get("searchterm").concat("_F"))) {
							pSearchtermValueId = ((Long) searchTermValues.get(row.get("searchterm").concat("_F")))
									.longValue();
						} else {
							// store the search term
							dataProvider.setId(dataProvider.getId() + 1);
							pSearchtermValue.setLong(cnt++, dataProvider.getId()); // id
							pSearchtermValue.setString(cnt++, "F"); // 1 = F, 2
							// = T, 3 =
							// F, 4 = T
							pSearchtermValue.setString(cnt++, row.get("searchterm")); // term
							pSearchtermValue.setNull(cnt++, Types.INTEGER); // searchterm_sns_id
							try {
								pSearchtermValue.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSearchtermValue.toString(), e);
								throw e;
							}
							searchTermValues.put(row.get("searchterm").concat("_F"), new Long(dataProvider.getId()));
							pSearchtermValueId = dataProvider.getId();
						}

						// store the address -> searchterm relation
						cnt = 1;
						long addrId = IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("obj_id"));
						dataProvider.setId(dataProvider.getId() + 1);
						pSearchtermAdr.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermAdr.setLong(cnt++, addrId); // obj_id
						pSearchtermAdr.setString(cnt++, row.get("line")); // term
						pSearchtermAdr.setLong(cnt++, pSearchtermValueId); // searchterm_id
						try {
							pSearchtermAdr.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSearchtermAdr.toString(), e);
							throw e;
						}
						// update full text index
						JDBCHelper.updateAddressIndex(addrId, row.get("searchterm"), jdbc); // SearchtermValue.term
						JDBCHelper.updateAddressIndex(addrId, row.get("searchterm"), "partial", jdbc); // SearchtermValue.term in partial idx

					}
				} else if (row.getInteger("type") != null && row.getInteger("type") == 4) {
					// check for invalid record
					if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("obj_id")) == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
									+ "') not found in imported data of t02_address. Skip record.");
						}
						row.clear();
					} else if (IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid", "th_desc_no",
							row.get("th_desc_no"), "th_orig_desc_no").length() == 0) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: th_desc_no ('"
									+ row.get("th_desc_no") + "') not found in thesorigid. Skip record.");
						}
						row.clear();
					} else {
						long pSearchtermValueId;
						long pSearchtermSnsId;
						String snsTopicId = IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid",
								"th_desc_no", row.get("th_desc_no"), "th_orig_desc_no");
						String snsSearchTermCheckIdent = row.get("searchterm").concat("_").concat(snsTopicId);
						// if the term has been stored already, refere to the
						// already stored id
						if (searchTermValues.containsKey(snsSearchTermCheckIdent)) {
							pSearchtermValueId = searchTermValues.get(snsSearchTermCheckIdent).longValue();
						} else {
							if (searchTermSnsValues.containsKey(snsTopicId)) {
								pSearchtermSnsId = searchTermSnsValues.get(snsTopicId).longValue();
							} else {
								// store the new sns topic id in table
								// searchterm_sns
								cnt = 1;
								dataProvider.setId(dataProvider.getId() + 1);
								pSearchtermSns.setLong(cnt++, dataProvider.getId()); // id
								pSearchtermSns.setString(cnt++, "uba_thes_".concat(snsTopicId)); // sns_id
								pSearchtermSns.setNull(cnt++, java.sql.Types.VARCHAR); // expired_at
								try {
									pSearchtermSns.executeUpdate();
								} catch (Exception e) {
									log.error("Error executing SQL: " + pSearchtermSns.toString(), e);
									throw e;
								}
								pSearchtermSnsId = dataProvider.getId();
								searchTermSnsValues.put(snsTopicId, new Long(pSearchtermSnsId));
							}

							// store the search term
							cnt = 1;
							dataProvider.setId(dataProvider.getId() + 1);
							pSearchtermValue.setLong(cnt++, dataProvider.getId()); // id
							pSearchtermValue.setString(cnt++, "T"); // 1 = F, 2
							// = T, 3 =
							// F, 4 = T
							pSearchtermValue.setString(cnt++, row.get("searchterm")); // term
							pSearchtermValue.setLong(cnt++, pSearchtermSnsId); // searchterm_sns_id
							try {
								pSearchtermValue.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSearchtermValue.toString(), e);
								throw e;
							}
							searchTermValues.put(snsSearchTermCheckIdent, new Long(dataProvider.getId()));
							pSearchtermValueId = dataProvider.getId();
						}

						// store the object -> searchterm relation
						cnt = 1;
						long addrId = IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("obj_id"));
						dataProvider.setId(dataProvider.getId() + 1);
						pSearchtermAdr.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermAdr.setLong(cnt++, addrId); // obj_id
						pSearchtermAdr.setString(cnt++, row.get("line")); // term
						pSearchtermAdr.setLong(cnt++, pSearchtermValueId); // searchterm_id
						try {
							pSearchtermAdr.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSearchtermAdr.toString(), e);
							throw e;
						}
						// update full text index
						JDBCHelper.updateAddressIndex(addrId, row.get("searchterm"), jdbc); // SearchtermValue.term
						JDBCHelper.updateAddressIndex(addrId, row.get("searchterm"), "partial", jdbc); // SearchtermValue.term in partial idx
						JDBCHelper.updateAddressIndex(addrId, "uba_thes_".concat(snsTopicId), jdbc); // SearchtermSns.snsId
						JDBCHelper.updateAddressIndex(addrId, "uba_thes_".concat(snsTopicId), "partial", jdbc); // SearchtermSns.snsId in partial idx

					}
				}
			}
		}
		pSearchtermObj.close();
		pSearchtermAdr.close();
		pSearchtermValue.close();
		pSearchtermSns.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjProject() throws Exception {

		String entityName = "t011_obj_project";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_project (id, obj_id, leader, member, description) VALUES ( ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_project";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setString(cnt++, row.get("leader")); // leader
				p.setString(cnt++, row.get("member")); // member
				p.setString(cnt++, row.get("description")); // description
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("leader"), jdbc); // T011ObjProject.leader
				JDBCHelper.updateObjectIndex(objId, row.get("member"), jdbc); // T011ObjProject.member
				JDBCHelper.updateObjectIndex(objId, row.get("description"), jdbc); // T011ObjProject.description
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjLiteratur() throws Exception {

		String entityName = "t011_obj_literatur";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_literature (id, obj_id, author, publisher, type_value, type_key, publish_in, "
				+ "volume, sides, publish_year, publish_loc, loc, doc_info, base, isbn, publishing, "
				+ "description) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_literature";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=3385 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")); 
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setString(cnt++, row.get("autor")); // author
				p.setString(cnt++, row.get("publisher")); // publisher
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("typ") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("typ").toLowerCase());
				}
				String valueWritten = row.get("typ");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // type_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // type_key
				} else {
					p.setString(cnt++, valueWritten); // type_value
					p.setInt(cnt++, -1); // type_key
				}
				p.setString(cnt++, row.get("publish_in")); // publish_in
				p.setString(cnt++, row.get("volume")); // volume
				p.setString(cnt++, row.get("sides")); // sides
				p.setString(cnt++, row.get("publish_year")); // publish_year
				p.setString(cnt++, row.get("publish_loc")); // publish_loc
				p.setString(cnt++, row.get("loc")); // loc
				p.setString(cnt++, row.get("doc_info")); // doc_info
				p.setString(cnt++, row.get("base")); // base
				p.setString(cnt++, row.get("isbn")); // isbn
				p.setString(cnt++, row.get("publishing")); // publishing
				p.setString(cnt++, row.get("description")); // description
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("autor"), jdbc); // T011ObjLiterature.author
				JDBCHelper.updateObjectIndex(objId, row.get("publisher"), jdbc); // T011ObjLiterature.publisher
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T011ObjLiterature.typeValue
				JDBCHelper.updateObjectIndex(objId, row.get("publish_in"), jdbc); // T011ObjLiterature.publishIn
				JDBCHelper.updateObjectIndex(objId, row.get("volume"), jdbc); // T011ObjLiterature.volume
				JDBCHelper.updateObjectIndex(objId, row.get("sides"), jdbc); // T011ObjLiterature.sides
				JDBCHelper.updateObjectIndex(objId, row.get("publish_year"), jdbc); // T011ObjLiterature.publishYear
				JDBCHelper.updateObjectIndex(objId, row.get("publish_loc"), jdbc); // T011ObjLiterature.publishLoc
				JDBCHelper.updateObjectIndex(objId, row.get("loc"), jdbc); // T011ObjLiterature.loc
				JDBCHelper.updateObjectIndex(objId, row.get("doc_info"), jdbc); // T011ObjLiterature.docInfo
				JDBCHelper.updateObjectIndex(objId, row.get("base"), jdbc); // T011ObjLiterature.base
				JDBCHelper.updateObjectIndex(objId, row.get("isbn"), jdbc); // T011ObjLiterature.isbn
				JDBCHelper.updateObjectIndex(objId, row.get("publishing"), jdbc); // T011ObjLiterature.publishing
				JDBCHelper.updateObjectIndex(objId, row.get("description"), jdbc); // T011ObjLiterature.description
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjData() throws Exception {

		String entityName = "t011_obj_data";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_data (id, obj_id, base, description) VALUES ( ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_data";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setString(cnt++, row.get("base")); // base
				p.setString(cnt++, row.get("description")); // description
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("base"), jdbc); // T011ObjData.base
				JDBCHelper.updateObjectIndex(objId, row.get("description"), jdbc); // T011ObjData.description

			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjDataParam() throws Exception {

		String entityName = "t011_obj_data_para";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_data_para (id, obj_id, line, parameter, unit) VALUES ( ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_data_para";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")); 
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				p.setString(cnt++, row.get("parameter")); // parameter
				p.setString(cnt++, row.get("unit")); // unit
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("parameter"), jdbc); // T011ObjDataPara.parameter
				JDBCHelper.updateObjectIndex(objId, row.get("unit"), jdbc); // T011ObjDataPara.unit
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeo() throws Exception {

		String entityName = "t011_obj_geo";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo (id, obj_id, special_base, data_base, method, referencesystem_value, rec_exact, rec_grade, hierarchy_level, "
				+ "vector_topology_level, referencesystem_key, pos_accuracy_vertical, keyc_incl_w_dataset) "
				+ "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				// COORD IS NULL AND REFERENCESYSTEM_ID=-1
				if ((row.get("coord") == null || row.get("coord").length() == 0)
						&& row.getInteger("referencesystem_id") != null
						&& row.getInteger("referencesystem_id").intValue() == -1) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in " + entityName
								+ " found: coord=NULL OR empty AND referencesystem_id=-1 !! Record will be imported!");
					}
				}
				// REFERENCESYSTEM_ID NOT IN (SELECT DISTINCT (DOMAIN_ID)
				// FROM SYS_CODELIST_DOMAIN WHERE CODELIST_ID=100) AND
				// REFERENCESYSTEM_ID!=-1"
				if (row.getInteger("referencesystem_id") != null
						&& row.getInteger("referencesystem_id").intValue() != -1
						&& dataProvider.findRow("sys_codelist_domain", new String[] { "codelist_id", "domain_id" },
								new String[] { "100", row.get("referencesystem_id") }) == null) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in " + entityName + " found: referencesystem_id="
								+ row.get("referencesystem_id")
								+ " not found in sys_codelist_domain with codelist_id=100 !! Record will be imported!");
					}
				}
				String coord = row.get("coord");
				if (coord != null && (coord.indexOf("\n") > -1 || coord.indexOf("\r") > -1)) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in "
										+ entityName
										+ " found: coord contains one or more newlines. newlines will be replaced with ';' ! Record will be imported!");
					}
					coord = coord.replaceAll("/\r\n/g", ";");
					coord = coord.replaceAll("/\n\r/g", ";");
					coord = coord.replaceAll("/\n/g", ";");
					coord = coord.replaceAll("/\r/g", ";");
				}
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setString(cnt++, row.get("special_base")); // special_base
				p.setString(cnt++, row.get("data_base")); // data_base
				p.setString(cnt++, row.get("method")); // method
				p.setString(cnt++, coord); // referencesystem_value
				JDBCHelper.addDouble(p, cnt++, row.getDouble("rec_exact")); // rec_exact
				JDBCHelper.addDouble(p, cnt++, row.getDouble("rec_grade")); // rec_grade
				JDBCHelper.addInteger(p, cnt++, row.getInteger("hierarchy_level")); // hierarchy_level
				JDBCHelper.addInteger(p, cnt++, row.getInteger("vector_topology_level")); // vector_topology_level

				// map old codelist to new codelist, log inconsistencies
				Integer oldKeyList100 = row.getInteger("referencesystem_id");
				Integer newKeyList100 = oldKeyList100;
				if (oldKeyList100 != null && oldKeyList100 != -1) {
					newKeyList100 = mapOldKeyToNewKeyList100.get(oldKeyList100);
					if (newKeyList100 == null) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid entry in " + entityName + " found: referencesystem_key ('" + oldKeyList100
									+ "') not found in syslist 100, is set to null. obj_id=" + objId);
						}
					}
				}
				
				JDBCHelper.addInteger(p, cnt++, newKeyList100); // referencesystem_key
				JDBCHelper.addDouble(p, cnt++, row.getDouble("pos_accuracy_vertical")); // pos_accuracy_vertical
				JDBCHelper.addInteger(p, cnt++, row.getInteger("keyc_incl_w_dataset")); // keyc_incl_w_dataset
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("special_base"), jdbc); // T011ObjGeo.specialBase
				JDBCHelper.updateObjectIndex(objId, row.get("data_base"), jdbc); // T011ObjGeo.dataBase
				JDBCHelper.updateObjectIndex(objId, row.get("method"), jdbc); // T011ObjGeo.method
				JDBCHelper.updateObjectIndex(objId, row.get("special_base"), jdbc); // T011ObjGeo.specialBase
				JDBCHelper.updateObjectIndex(objId, row.get("coord"), jdbc); // T011ObjGeo.referencesystemValue
				
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isDebugEnabled()) {
					log.debug("Skip record of " + entityName + " (obj_id='" + row.get("obj_id") + "'; mod_type='"
							+ row.get("mod_type") + "')");
				}
				row.clear();
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoVector() throws Exception {

		String entityName = "t011_obj_geo_vector";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_vector (id, obj_geo_id, line, geometric_object_type, geometric_object_count) VALUES ( ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_vector";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_geo. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				JDBCHelper.addInteger(p, cnt++, row.getInteger("geometric_object_type")); // geometric_object_type
				JDBCHelper.addInteger(p, cnt++, row.getInteger("geometric_object_count")); // geometric_object_count
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoSymc() throws Exception {

		String entityName = "t011_obj_geo_symc";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_symc (id, obj_geo_id, line, symbol_cat_value, symbol_cat_key, symbol_date, edition) VALUES ( ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_symc";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=3555 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_geo. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("symbol_cat") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("symbol_cat").toLowerCase());
				}
				String valueWritten = row.get("symbol_cat");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // symbol_cat_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // symbol_cat_key
				} else {
					p.setString(cnt++, valueWritten); // symbol_cat_value
					p.setInt(cnt++, -1); // symbol_cat_key
				}
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("symbol_date"))); // symbol_date
				p.setString(cnt++, row.get("edition")); // edition
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T011ObjGeoSymc.symbolCatValue
				JDBCHelper.updateObjectIndex(objId, IDCStrategyHelper.transDateTime(row.get("symbol_date")), jdbc); // T011ObjGeoSymc.symbolDate
				JDBCHelper.updateObjectIndex(objId, row.get("edition"), jdbc); // T011ObjGeoSymc.edition
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoTopicCat() throws Exception {

		String entityName = "t011_obj_geo_topic_cat";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_topic_cat (id, obj_id, line, topic_category) VALUES ( ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_topic_cat";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				JDBCHelper.addInteger(p, cnt++, row.getInteger("topic_category")); // topic_category
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
		
		// map another udk table (T0111_env_class) to same idc table (t011_obj_topic_cat)
		processT0111EnvClass();
	}

	private void processT0111EnvClass() throws Exception {

		String entityName = "t0111_env_class";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		// set up mapping of old value to new syslist 527 value via map<oldValue, newValue>
		HashMap<String, String[]> mapOldValueToNewValuesList527 = new HashMap<String, String[]>();
		mapOldValueToNewValuesList527.put("ab", new String[] {"umwelt"});
		mapOldValueToNewValuesList527.put("ua", new String[] {"umwelt"});
		mapOldValueToNewValuesList527.put("bo", new String[] {"umwelt"});
		mapOldValueToNewValuesList527.put("le", new String[] {"umwelt"});
		mapOldValueToNewValuesList527.put("lu", new String[] {"umwelt", "klima und atmosphäre"});
		mapOldValueToNewValuesList527.put("nl", new String[] {"umwelt"});
		mapOldValueToNewValuesList527.put("sr", new String[] {"umwelt", "gesundheit", "klima und atmosphäre"});
		mapOldValueToNewValuesList527.put("lf", new String[] {"umwelt", "landwirtschaft"});
		mapOldValueToNewValuesList527.put("gt", new String[] {"umwelt", "gesundheit"});
		mapOldValueToNewValuesList527.put("en", new String[] {"umwelt", "ökonomie", "energie und kommunikation"});
		mapOldValueToNewValuesList527.put("ch", new String[] {"umwelt", "gesundheit"});
		mapOldValueToNewValuesList527.put("uw", new String[] {"umwelt", "ökonomie"});
		mapOldValueToNewValuesList527.put("ur", new String[] {"umwelt", "gesellschaft"});
		mapOldValueToNewValuesList527.put("wa", new String[] {"umwelt"});

		// load syslist 527 ids, values needed for mapping
		final List<Integer> allowedSpecialRefEntries = new ArrayList<Integer>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=527 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getInt("entry_id"));
			}
		}
		rs.close();
		st.close();

		// remember key for "Umwelt", is used in postprocessing !
		int indxDefault = allowedSpecialRefEntryNamesLowerCase.indexOf("umwelt");
		if (indxDefault != -1) {
			defaultThemenkategorieEntryId = allowedSpecialRefEntries.get(indxDefault);
		}

		pSqlStr = "INSERT INTO t011_obj_topic_cat (id, obj_id, line, topic_category) VALUES ( ?, ?, ?, ?)";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			int objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
			if (objId == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();

			} else if (row.get("name") == null) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: name IS NULL !!! obj_id ('" + row.get("obj_id")
							+ "'). Skip record.");
				}
				row.clear();

			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {

				// map old value to new keys !
				String oldValue = row.get("name");
				String[] newValues = mapOldValueToNewValuesList527.get(oldValue.toLowerCase());
				ArrayList<Integer> newKeys = new ArrayList<Integer>();
				if (newValues != null) {
					for (String newValue : newValues) {
						int entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(newValue);
						if (entryIndex != -1) {
							int newKey = allowedSpecialRefEntries.get(entryIndex);
							if (!newKeys.contains(newKey)) {
								newKeys.add(newKey);
							}
						}
					}
				}
				if (newKeys.size() > 0) {
					// we have new "themenkategorien" to add, add only the ones not added yet

					// fetch the ones already added !
					sql = "SELECT topic_category, line FROM t011_obj_topic_cat WHERE obj_id=" + objId + "";
					st = jdbc.createStatement();
					rs = jdbc.executeQuery(sql, st);
					int lastLine = 0;
					List<Integer> existingKeys = new ArrayList<Integer>();
					while (rs.next()) {
						existingKeys.add(rs.getInt("topic_category"));
						int line = rs.getInt("line");
						lastLine = line > lastLine ? line : lastLine; 
					}
					rs.close();
					st.close();
					
					// add new ones
					for (Integer newKey : newKeys) {
						if (!existingKeys.contains(newKey)) {
							int cnt = 1;
							dataProvider.setId(dataProvider.getId() + 1);
							p.setLong(cnt++, dataProvider.getId()); // id
							p.setLong(cnt++, objId); // obj_id
							p.setInt(cnt++, ++lastLine); // line
							JDBCHelper.addInteger(p, cnt++, newKey); // topic_category
							try {
								p.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + p.toString(), e);
								throw e;
							}
							existingKeys.add(newKey);
						}
					}
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoSupplInfo() throws Exception {

		String entityName = "t011_obj_geo_supplinfo";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_supplinfo (id, obj_geo_id, line, feature_type) VALUES ( ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_supplinfo";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_geo. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				p.setString(cnt++, row.get("feature_type")); // feature_type
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoSpatialRep() throws Exception {

		String entityName = "t011_obj_geo_spatial_rep";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_spatial_rep (id, obj_geo_id, line, type) VALUES ( ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_spatial_rep";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_geo. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				JDBCHelper.addInteger(p, cnt++, row.getInteger("type")); // type
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoScale() throws Exception {

		String entityName = "t011_obj_geo_scale";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_scale (id, obj_geo_id, line, scale, resolution_ground, resolution_scan) VALUES ( ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_scale";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_geo. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				JDBCHelper.addInteger(p, cnt++, row.getInteger("scale")); // scale
				JDBCHelper.addDouble(p, cnt++, row.getDouble("resolution_ground")); // resolution_ground
				JDBCHelper.addDouble(p, cnt++, row.getDouble("resolution_scan")); // resolution_scan
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoKeyc() throws Exception {

		String entityName = "t011_obj_geo_keyc";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_keyc (id, obj_geo_id, line, keyc_value, keyc_key, key_date, edition) VALUES ( ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_keyc";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=3535 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_geo. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("subject_cat") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("subject_cat").toLowerCase());
				}
				String valueWritten = row.get("subject_cat");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // keyc_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // keyc_key
				} else {
					p.setString(cnt++, valueWritten); // keyc_value
					p.setInt(cnt++, -1); // keyc_key
				}
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("key_date"))); // subject_cat
				p.setString(cnt++, row.get("edition")); // subject_cat
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T011ObjGeoKeyc.keycValue
				JDBCHelper.updateObjectIndex(objId, IDCStrategyHelper.transDateTime(row.get("key_date")), jdbc); // T011ObjGeoKeyc.keyDate
				JDBCHelper.updateObjectIndex(objId, row.get("edition"), jdbc); // T011ObjGeoKeyc.edition
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServ() throws Exception {

		String entityName = "t011_obj_serv";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv (id, obj_id, type_value, type_key, history, environment, base, description) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=5100 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("type") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("type").toLowerCase());
				}
				String valueWritten = row.get("type");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // type_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // type_key
				} else {
					p.setString(cnt++, valueWritten); // type_value
					p.setInt(cnt++, -1); // type_key
				}
				p.setString(cnt++, row.get("history")); // history
				p.setString(cnt++, row.get("environment")); // environment
				p.setString(cnt++, row.get("base")); // base
				p.setString(cnt++, row.get("description")); // description
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T011ObjServ.typeValue
				JDBCHelper.updateObjectIndex(objId, row.get("history"), jdbc); // T011ObjServ.history
				JDBCHelper.updateObjectIndex(objId, row.get("environment"), jdbc); // T011ObjServ.environment
				JDBCHelper.updateObjectIndex(objId, row.get("base"), jdbc); // T011ObjServ.base
				JDBCHelper.updateObjectIndex(objId, row.get("description"), jdbc); // T011ObjServ.description
				
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isDebugEnabled()) {
					log.debug("Skip record of " + entityName + " (obj_id='" + row.get("obj_id") + "'; mod_type='"
							+ row.get("mod_type") + "')");
				}
				row.clear();
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServVersion() throws Exception {

		String entityName = "t011_obj_serv_version";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_version (id, obj_serv_id, line, serv_version) VALUES ( ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_version";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_serv. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				p.setString(cnt++, row.get("version")); // serv_version
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, row.get("version"), jdbc); // T011ObjServVersion.servVersion

			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOperation() throws Exception {

		String entityName = "t011_obj_serv_operation";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_operation (id, obj_serv_id, line, name_value, name_key, descr, invocation_name) VALUES ( ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_operation";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialWMSRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialWMSRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialWMSRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=5110 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialWMSRefEntryNames.add(rs.getString("name"));
				allowedSpecialWMSRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialWMSRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		final List<String> allowedSpecialWFSRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialWFSRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialWFSRefEntryNamesLowerCase = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=5120 and lang_id='" + getCatalogLanguage() + "'";
		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialWFSRefEntryNames.add(rs.getString("name"));
				allowedSpecialWFSRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialWFSRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t011_obj_serv. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				String serviceType = IDCStrategyHelper.getEntityFieldValue(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id"), "type");
				String nameWritten = row.get("name");
				if (serviceType != null && serviceType.equalsIgnoreCase("WMS")) {
					// try to find entry in syslist
					int entryIndex = -1;
					if (row.get("name") != null) {
						entryIndex = allowedSpecialWMSRefEntryNamesLowerCase.indexOf(row.get("name").toLowerCase());
					}
					if (entryIndex != -1) {
						// we set also value from entry !!! necessary for mapping !
						nameWritten = allowedSpecialWMSRefEntryNames.get(entryIndex);
						p.setString(cnt++, nameWritten); // name_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialWMSRefEntries.get(entryIndex))); // name_key
					} else {
						p.setString(cnt++, nameWritten); // name_value
						p.setInt(cnt++, -1); // name_key
					}
				} else if (serviceType != null && serviceType.equalsIgnoreCase("WFS")) {
					// try to find entry in syslist
					int entryIndex = -1;
					if (row.get("name") != null) {
						entryIndex = allowedSpecialWFSRefEntryNamesLowerCase.indexOf(row.get("name").toLowerCase());
					}
					if (entryIndex != -1) {
						// we set also value from entry !!! necessary for mapping !
						nameWritten = allowedSpecialWFSRefEntryNames.get(entryIndex);
						p.setString(cnt++, nameWritten); // name_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialWFSRefEntries.get(entryIndex))); // name_key
					} else {
						p.setString(cnt++, nameWritten); // name_value
						p.setInt(cnt++, -1); // name_key
					}
				} else {
					p.setString(cnt++, nameWritten); // name_value
					p.setInt(cnt++, -1); // name_key
				}
				p.setString(cnt++, row.get("descr")); // descr
				p.setString(cnt++, row.get("invocation_name")); // invocation_name
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, nameWritten, jdbc); // T011ObjServOperation.nameValue
				JDBCHelper.updateObjectIndex(objId, row.get("descr"), jdbc); // T011ObjServOperation.descr
				JDBCHelper.updateObjectIndex(objId, row.get("invocation_name"), jdbc); // T011ObjServOperation.invocationName
			
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isDebugEnabled()) {
					log.debug("Skip record of " + entityName + " (obj_id='" + row.get("obj_id") + "'; mod_type='"
							+ row.get("mod_type") + "')");
				}
				row.clear();
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpPlatform() throws Exception {

		String entityName = "t011_obj_serv_op_platform";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_platform (id, obj_serv_op_id, line, platform) VALUES (?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_op_platform";
		jdbc.executeUpdate(sqlStr);

		String[] rowNames = new String[] { "obj_id", "line" };
		String[] rowValues = new String[2];

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			rowValues[0] = row.get("obj_id");
			rowValues[1] = row.get("line");

			int fk = IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv_operation", rowNames, rowValues);
			if (fk == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "'), line ('" + row.get("line")
							+ "') not found in imported data of t011_obj_serv_operation. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				JDBCHelper.addInteger(p, cnt++, row.getInteger("dcp_line")); // line
				p.setString(cnt++, row.get("platform")); // platform
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, row.get("platform"), jdbc); // T011ObjServOpPlatform.platform
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpPara() throws Exception {

		String entityName = "t011_obj_serv_op_para";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_para (id, obj_serv_op_id, line, name, direction, descr, optional, repeatability) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_op_para";
		jdbc.executeUpdate(sqlStr);

		String[] rowNames = new String[] { "obj_id", "line" };
		String[] rowValues = new String[2];

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			rowValues[0] = row.get("obj_id");
			rowValues[1] = row.get("line");

			int fk = IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv_operation", rowNames, rowValues);
			if (fk == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "'), line ('" + row.get("line")
							+ "') not found in imported data of t011_obj_serv_operation. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				p.setInt(cnt++, row.getInteger("para_line")); // line
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, row.get("direction")); // direction
				p.setString(cnt++, row.get("descr")); // descr
				JDBCHelper.addInteger(p, cnt++, row.getInteger("optional")); // optional
				JDBCHelper.addInteger(p, cnt++, row.getInteger("repeatability")); // repeatability
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, row.get("name"), jdbc); // T011ObjServOpPara.name
				JDBCHelper.updateObjectIndex(objId, row.get("direction"), jdbc); // T011ObjServOpPara.direction
				JDBCHelper.updateObjectIndex(objId, row.get("descr"), jdbc); // T011ObjServOpPara.descr
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpDepends() throws Exception {

		String entityName = "t011_obj_serv_op_depends";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_depends (id, obj_serv_op_id, line, depends_on) VALUES (?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_op_depends";
		jdbc.executeUpdate(sqlStr);

		String[] rowNames = new String[] { "obj_id", "line" };
		String[] rowValues = new String[2];

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			rowValues[0] = row.get("obj_id");
			rowValues[1] = row.get("line");

			int fk = IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv_operation", rowNames, rowValues);
			if (fk == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "'), line ('" + row.get("line")
							+ "') not found in imported data of t011_obj_serv_operation. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				JDBCHelper.addInteger(p, cnt++, row.getInteger("dep_line")); // line
				p.setString(cnt++, row.get("depends_on")); // depends_on
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, row.get("depends_on"), jdbc); // T011ObjServOpDepends.dependsOn
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpConnpoint() throws Exception {

		String entityName = "t011_obj_serv_op_connpoint";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_connpoint (id, obj_serv_op_id, line, connect_point) VALUES (?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_op_connpoint";
		jdbc.executeUpdate(sqlStr);

		String[] rowNames = new String[] { "obj_id", "line" };
		String[] rowValues = new String[2];

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			rowValues[0] = row.get("obj_id");
			rowValues[1] = row.get("line");

			int fk = IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv_operation", rowNames, rowValues);
			if (fk == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "'), line ('" + row.get("line")
							+ "') not found in imported data of t011_obj_serv_operation. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				JDBCHelper.addInteger(p, cnt++, row.getInteger("conn_line")); // line
				p.setString(cnt++, row.get("connect_point")); // connect_point
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, row.get("connect_point"), jdbc); // T011ObjServOpConnpoint.connectPoint
				
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT015Legist() throws Exception {

		String entityName = "t015_legist";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t015_legist (id, obj_id, line, legist_value, legist_key) VALUES (?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t015_legist";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1350 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();

		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of t01_object. Skip record.");
					}
					row.clear();
				} else {
					int cnt = 1;
					long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
					p.setInt(cnt++, row.getInteger("primary_key")); // id
					p.setLong(cnt++, objId); // obj_id
					p.setInt(cnt++, row.getInteger("line")); // line
					// try to find entry in syslist
					int entryIndex = -1;
					if (row.get("name") != null) {
						entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("name").toLowerCase());
					}
					String valueWritten = row.get("name");
					if (entryIndex != -1) {
						// we set also value from entry !!! necessary for mapping !
						valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
						p.setString(cnt++, valueWritten); // legist_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // legist_key
					} else {
						p.setString(cnt++, valueWritten); // legist_value
						p.setInt(cnt++, -1); // legist_key
					}
					try {
						p.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}
					
					// update full text index
					JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T015Legist.legistValue
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT0113DatasetReference() throws Exception {

		String entityName = "t0113_dataset_reference";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t0113_dataset_reference (id, obj_id, line, reference_date, type) VALUES (?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t0113_dataset_reference";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("reference_date"))); // reference_date
				JDBCHelper.addInteger(p, cnt++, row.getInteger("type")); // type
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT0110AvailFormat() throws Exception {

		String entityName = "t0110_avail_format";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t0110_avail_format (id, obj_id, line, format_value, format_key, ver, file_decompression_technique, specification) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t0110_avail_format";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1320 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")); 
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("name") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("name").toLowerCase());
				}
				String valueWritten = row.get("name");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // format_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // format_key
				} else {
					p.setString(cnt++, valueWritten); // format_value
					p.setInt(cnt++, -1); // format_key
				}
				p.setString(cnt++, row.get("version")); // ver
				p.setString(cnt++, row.get("file_decompression_technique")); // file_decompression_technique
				p.setString(cnt++, row.get("specification")); // specification
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T0110AvailFormat.formatValue
				JDBCHelper.updateObjectIndex(objId, row.get("version"), jdbc); // T0110AvailFormat.ver
				JDBCHelper.updateObjectIndex(objId, row.get("file_decompression_technique"), jdbc); // T0110AvailFormat.fileDecompressionTechnique
				JDBCHelper.updateObjectIndex(objId, row.get("specification"), jdbc); // T0110AvailFormat.specification
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT0112MediaOption() throws Exception {

		String entityName = "t0112_media_option";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t0112_media_option (id, obj_id, line, medium_note, medium_name, transfer_size) VALUES (?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t0112_media_option";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				p.setString(cnt++, row.get("medium_note")); // medium_note
				p.setString(cnt++, row.get("medium_name")); // medium_name
				JDBCHelper.addDouble(p, cnt++, row.getDouble("transfer_size")); // transfer_size
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("medium_note"), jdbc); // T0112MediaOption.mediumNote
				JDBCHelper.updateObjectIndex(objId, row.get("medium_name"), jdbc); // T0112MediaOption.mediumName
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT017UrlRef() throws Exception {

		String entityName = "t017_url_ref";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t017_url_ref (id, obj_id, line, url_link, special_ref, special_name, content, datatype_value, datatype_key, volume, icon, icon_text, descr, url_type) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t017_url_ref";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedDatatypeValues = new ArrayList<String>();
		final List<String> allowedDatatypeValuesLowerCase = new ArrayList<String>();
		final List<String> allowedDatatypeKeys = new ArrayList<String>();

		String sql = "SELECT name, entry_id FROM sys_list WHERE LST_ID=2240 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedDatatypeValues.add(rs.getString("name"));
				allowedDatatypeValuesLowerCase.add(rs.getString("name").toLowerCase());
				allowedDatatypeKeys.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=2000 " +
				"AND entry_id IN (3100, 3210, 3345, 3515, 3520, 3535, 3555, 3570, 5066) " +
				"and lang_id='" + getCatalogLanguage() + "'";
		st = jdbc.createStatement();
		rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, objId); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				p.setString(cnt++, row.get("url_link")); // url_link

				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("special_ref") != null) {
					entryIndex = allowedSpecialRefEntries.indexOf(row.get("special_ref").toLowerCase());
				}
				if (entryIndex == -1 && row.get("special_name") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("special_name").toLowerCase());
				}
				String specialNameWritten = row.get("special_name");
				if (entryIndex != -1) {
					JDBCHelper.addInteger(p, cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // special_ref
					// we set also value from entry !!! necessary for mapping !
					specialNameWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, specialNameWritten); // special_name
				} else {
					p.setInt(cnt++, -1); // special_ref
					p.setString(cnt++, specialNameWritten); // special_name
				}
			
				p.setString(cnt++, row.get("content")); // content
				
				// try to find entry in syslist
				entryIndex = -1;
				if (row.get("datatype") != null) {
					entryIndex = allowedDatatypeValuesLowerCase.indexOf(row.get("datatype").toLowerCase());
				}
				String valueWritten = row.get("datatype");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedDatatypeValues.get(entryIndex);
					p.setString(cnt++, valueWritten); // datatype_value
					p.setInt(cnt++, Integer.parseInt(allowedDatatypeKeys.get(entryIndex))); // datatype_key
				} else {
					p.setString(cnt++, valueWritten); // datatype_value
					p.setInt(cnt++, -1); // datatype_key
				}
				p.setString(cnt++, row.get("volume")); // volume
				p.setString(cnt++, row.get("icon")); // icon
				p.setString(cnt++, row.get("icon_text")); // icon_text
				p.setString(cnt++, row.get("descr")); // descr
				JDBCHelper.addInteger(p, cnt++, row.getInteger("url_type")); // url_type
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}

				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("url_link"), jdbc); // T017UrlRef.urlLink
				JDBCHelper.updateObjectIndex(objId, specialNameWritten, jdbc); // T017UrlRef.specialName
				JDBCHelper.updateObjectIndex(objId, row.get("content"), jdbc); // T017UrlRef.content
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T017UrlRef.datatypeValue
				JDBCHelper.updateObjectIndex(objId, row.get("volume"), jdbc); // T017UrlRef.volume
				JDBCHelper.updateObjectIndex(objId, row.get("icon_text"), jdbc); // T017UrlRef.iconText
				JDBCHelper.updateObjectIndex(objId, row.get("descr"), jdbc); // T017UrlRef.descr
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011Township() throws Exception {

		String entityName = "t011_township";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSpatialRefSns = "INSERT INTO spatial_ref_sns (id, sns_id, expired_at) " + "VALUES (?, ?, ?)";
		PreparedStatement psInsertSpatialRefSns = jdbc.prepareStatement(pSqlStrSpatialRefSns);

		// NOTICE: executed before importing free spatialReferences, so we CLEAR ALL REFERENCES !
		sqlStr = "DELETE FROM spatial_reference";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM spatial_ref_value";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM spatial_ref_sns";
		jdbc.executeUpdate(sqlStr);

		// cache for remembering already stored thesaurus spatial_ref_values, should be stored only ONCE !
		HashMap<String, Long> storedNativeAGSKeys = new HashMap<String, Long>();
		// cache for remembering already stored spatial references, meaning thesaurus spatial ref values connected to an object !
		HashMap<String, Long> storedObjectSpatialReferences = new HashMap<String, Long>();

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			
			// skip deleted rows
			if (row.get("mod_type") == null || invalidModTypes.contains(row.get("mod_type"))) {
				continue;
			}

			// skip rows with invalid object references
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
				continue;
			}

			// ags Key set ? skip rows where not set !
			String nativeAGSKey = row.get("township_no");
			if (nativeAGSKey == null || nativeAGSKey.trim().length() == 0) {
				log.debug("Invalid entry in " + entityName + " found: township_no('" + nativeAGSKey +
						"'), obj_id ('" + row.get("obj_id")	+ "'). Skip record.");
				row.clear();
				continue;
			}

			// valid AGS ? extract location name. skip rows where not valid.
			String locName = IDCStrategyHelper.getLocationNameFromNativeAGS(nativeAGSKey, dataProvider);
			if (locName == null) {
				log.debug("Invalid entry in " + entityName + " found: township_no ('" + nativeAGSKey
						+ "') not valid AGS key ! obj_id ('" + row.get("obj_id")	+ "'). Skip record.");
				row.clear();
				continue;
			}

			// OK, IS VALID !

			// extract object data to be connected with
			long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
			Integer line = row.getInteger("line");

			// check whether it is a district -> no sns topics for districts !
			// if district, the state is written as thesaurus spatial reference and district as free spatial reference !
			if (nativeAGSKey.length() != 3) {
				// NO DISTRICT ! write as thesaurus spatial reference !
				// NOTICE: we already found location and it is no district, so snsTopicId should exist !
				String snsTopicId = IDCStrategyHelper.transformNativeKey2TopicId(nativeAGSKey);

				writeThesaurusSpatialReference (storedNativeAGSKeys,
						nativeAGSKey,
						snsTopicId,
						locName,
						objId,
						line,
						psInsertSpatialRefSns,
						storedObjectSpatialReferences);

			} else {
				// IS DISTRICT !

				log.debug("District entry in " + entityName + " found: township_no ('" + nativeAGSKey + "'), obj_id ('" + row.get("obj_id")+ "'). " +
						"Write state as thesaurus and district as free spatial reference !");

				// NOTICE: we already found district, so state must exist !
				String nativeAGSKeyState = nativeAGSKey.substring(0, 2);
				String locNameState = IDCStrategyHelper.getLocationNameFromNativeAGS(nativeAGSKeyState, dataProvider);
				String snsTopicIdState = IDCStrategyHelper.transformNativeKey2TopicId(nativeAGSKeyState);
				
				// write state as thesaurus spatial ref
				boolean wasWritten = writeThesaurusSpatialReference (storedNativeAGSKeys,
						nativeAGSKeyState,
						snsTopicIdState,
						locNameState,
						objId,
						line,
						psInsertSpatialRefSns,
						storedObjectSpatialReferences);

				if (wasWritten) {
					log.debug("Wrote state " + snsTopicIdState + ", '" + locNameState + "' as thesaurus spatial reference of object !");
				} else {
					log.debug("State " + snsTopicIdState + ", '" + locNameState + "' is already thesaurus spatial reference of object ! Not added !");
				}

				// write district as free spatial ref
				// NOTICE: we use line 0, so this one is at start !

				// extract bounding box values
				double x1 = IDCStrategyHelper.getEntityFieldValueAsDouble(
						dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "x1");
				double x2 = IDCStrategyHelper.getEntityFieldValueAsDouble(
						dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "x2");
				double y1 = IDCStrategyHelper.getEntityFieldValueAsDouble(
						dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "y1");
				double y2 = IDCStrategyHelper.getEntityFieldValueAsDouble(
						dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "y2");
				wasWritten = writeFreeSpatialReference(locName,
						x1, x2, y1, y2,
						objId,
						0,
						nativeAGSKey);

				if (wasWritten) {
					log.debug("Wrote district '" + locName + "' as free spatial reference of object !");
				} else {
					log.debug("District '" + locName + "' is already free spatial reference of object ! Not added !");
				}
			}
		}
		psInsertSpatialRefSns.close();
		psInsertSpatialReference.close();
		psInsertSpatialRefValue.close();
		psInsertSpatialReference = null;
		psInsertSpatialRefValue = null;
		
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	private boolean writeFreeSpatialReference (String bezug,
			Double x1,
			Double x2,
			Double y1,
			Double y2,
			long objId,
			int line,
			String nativeAGSKey
			) throws Exception {

		// create Prepared Statements for insert if not created yet
		if (psInsertSpatialReference == null) {
			String pSqlStrSpatialReference = "INSERT INTO spatial_reference (id, obj_id, line, spatial_ref_id) "
				+ "VALUES (?, ?, ?, ?)";
			psInsertSpatialReference = jdbc.prepareStatement(pSqlStrSpatialReference);

			String pSqlStrSpatialRefValue = "INSERT INTO spatial_ref_value (id, type, spatial_ref_sns_id, name_value, name_key, nativekey, x1, x2, y1, y2) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			psInsertSpatialRefValue = jdbc.prepareStatement(pSqlStrSpatialRefValue);
		}
		
		// load syslist entries for free spatial references if not loaded yet !
		if (freeSpatialReferenceEntryKeys == null) {
			freeSpatialReferenceEntryKeys = new ArrayList<String>();
			freeSpatialReferenceEntryNames = new ArrayList<String>();
			freeSpatialReferenceEntryNamesLowerCase = new ArrayList<String>();

			String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1100 and lang_id='" + getCatalogLanguage() + "'";
			Statement st = jdbc.createStatement();
			ResultSet rs = jdbc.executeQuery(sql, st);
			while (rs.next()) {
				if (rs.getString("name") != null) {
					freeSpatialReferenceEntryNames.add(rs.getString("name"));
					freeSpatialReferenceEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
					freeSpatialReferenceEntryKeys.add(rs.getString("entry_id"));
				}
			}
			rs.close();
			st.close();
		}

		int cnt = 1;
		long pSpatialRefValueId;

		// create key for this spatial reference to check whether it was stored already
		// NOTICE: Free spatial references should exist PER OBJECT ! name can be changed interactively in frontend !
		String freeSpatialRefKey = objId + bezug.toLowerCase();

		if (!storedFreeSpatialReferences.containsKey(freeSpatialRefKey)) {
			// no free spatial ref with same name for this object ! create it !
			
			// try to find entry in syslist
			int bezugKey = -1;
			int entryIndex = freeSpatialReferenceEntryNamesLowerCase.indexOf(bezug.toLowerCase());
			if (entryIndex != -1) {
				// we set also value from syslist entry !!! necessary for mapping !
				bezug = freeSpatialReferenceEntryNames.get(entryIndex);
				bezugKey = Integer.parseInt(freeSpatialReferenceEntryKeys.get(entryIndex));
			}

			// was a native AGS key passed, meaning this one was written from t011_township (district).
			// we write the full ags key.
			String nativeKey = "";
			if (nativeAGSKey != null) {
				nativeKey = IDCStrategyHelper.transformNativeKey2FullAgs(nativeAGSKey);
			}

			// create the free spatial ref value
			dataProvider.setId(dataProvider.getId() + 1);
			pSpatialRefValueId = dataProvider.getId();
			psInsertSpatialRefValue.setLong(cnt++, pSpatialRefValueId); // id
			psInsertSpatialRefValue.setString(cnt++, "F"); // type
			psInsertSpatialRefValue.setNull(cnt++, Types.INTEGER); // spatial_ref_sns_id
			psInsertSpatialRefValue.setString(cnt++, bezug); // name_value
			psInsertSpatialRefValue.setInt(cnt++, bezugKey); // name_key
			psInsertSpatialRefValue.setString(cnt++, nativeKey); // nativekey
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, x1); // x1
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, x2); // x2
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, y1); // y1
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, y2); // y2
			try {
				psInsertSpatialRefValue.executeUpdate();
				storedFreeSpatialReferences.put(freeSpatialRefKey, new Long(pSpatialRefValueId));
			} catch (Exception e) {
				log.error("Error executing SQL: " + psInsertSpatialRefValue.toString(), e);
				throw e;
			}

			// and connect it to object
			cnt = 1;
			dataProvider.setId(dataProvider.getId() + 1);
			psInsertSpatialReference.setLong(cnt++, dataProvider.getId()); // id
			psInsertSpatialReference.setLong(cnt++, objId); // obj_id
			psInsertSpatialReference.setInt(cnt++, line); // line
			psInsertSpatialReference.setLong(cnt++, pSpatialRefValueId); // spatial_ref_id
			try {
				psInsertSpatialReference.executeUpdate();
			} catch (Exception e) {
				log.error("Error executing SQL: " + psInsertSpatialReference.toString(), e);
				throw e;
			}

			// update full text index
			JDBCHelper.updateObjectIndex(objId, bezug, jdbc);
			
			return true;
		} else {
			return false;			
		}
	}

	private boolean writeThesaurusSpatialReference (HashMap<String, Long> storedNativeAGSKeys,
			String nativeAGSKey,
			String snsTopicId,
			String locName,
			long objId,
			int line,
			PreparedStatement psInsertSpatialRefSns,
			HashMap<String, Long> storedObjectSpatialReferences
			) throws Exception {

		// create Prepared Statements for insert if not created yet
		if (psInsertSpatialReference == null) {
			String pSqlStrSpatialReference = "INSERT INTO spatial_reference (id, obj_id, line, spatial_ref_id) "
				+ "VALUES (?, ?, ?, ?)";
			psInsertSpatialReference = jdbc.prepareStatement(pSqlStrSpatialReference);

			String pSqlStrSpatialRefValue = "INSERT INTO spatial_ref_value (id, type, spatial_ref_sns_id, name_value, name_key, nativekey, x1, x2, y1, y2) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			psInsertSpatialRefValue = jdbc.prepareStatement(pSqlStrSpatialRefValue);
		}
		
		long pSpatialRefValueId = 0;
		int cnt = 1;

		String fullAGSKey = IDCStrategyHelper.transformNativeKey2FullAgs(nativeAGSKey);

		// if the spatial ref has been stored already, refer to the already stored id
		// NOTICE: Thesaurus spatial ref exist only ONCE and can be connected to multiple objects !
		if (storedNativeAGSKeys.containsKey(nativeAGSKey)) {
			pSpatialRefValueId = ((Long) storedNativeAGSKeys.get(nativeAGSKey)).longValue();
			
		} else {
			// store the spatial ref sns values
			dataProvider.setId(dataProvider.getId() + 1);
			long spatialRefSnsId = dataProvider.getId();
			psInsertSpatialRefSns.setLong(cnt++, spatialRefSnsId); // id
			psInsertSpatialRefSns.setString(cnt++, snsTopicId); // sns_id
			psInsertSpatialRefSns.setString(cnt++, null); // expired_at
			try {
				psInsertSpatialRefSns.executeUpdate();
			} catch (Exception e) {
				log.error("Error executing SQL: " + psInsertSpatialRefSns.toString(), e);
				throw e;
			}

			// extract bounding box values
			double x1 = IDCStrategyHelper.getEntityFieldValueAsDouble(
					dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "x1");
			double x2 = IDCStrategyHelper.getEntityFieldValueAsDouble(
					dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "x2");
			double y1 = IDCStrategyHelper.getEntityFieldValueAsDouble(
					dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "y1");
			double y2 = IDCStrategyHelper.getEntityFieldValueAsDouble(
					dataProvider, "t01_st_bbox", "loc_town_no", nativeAGSKey, "y2");

			// store the spatial ref value refering to sns
			cnt = 1;
			dataProvider.setId(dataProvider.getId() + 1);
			pSpatialRefValueId = dataProvider.getId();

			psInsertSpatialRefValue.setLong(cnt++, pSpatialRefValueId); // id
			psInsertSpatialRefValue.setString(cnt++, "G"); // type
			psInsertSpatialRefValue.setLong(cnt++, spatialRefSnsId); // spatial_ref_sns_id
			// do NOT map via sys_list, this is a geothesaurus entry (SNS) !
			psInsertSpatialRefValue.setString(cnt++, locName); // name_value
			psInsertSpatialRefValue.setInt(cnt++, -1); // name_key
			psInsertSpatialRefValue.setString(cnt++, fullAGSKey); // nativekey
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, x1); // x1
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, x2); // x2
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, y1); // y1
			JDBCHelper.addDouble(psInsertSpatialRefValue, cnt++, y2); // y2
			try {
				psInsertSpatialRefValue.executeUpdate();
				storedNativeAGSKeys.put(nativeAGSKey, new Long(pSpatialRefValueId));
			} catch (Exception e) {
				log.error("Error executing SQL: " + psInsertSpatialRefValue.toString(), e);
				throw e;
			}
		}

		// check whether we already have this thesaurus ref in our object !
		String objSpatialRefKey = "" + objId + "/" + pSpatialRefValueId; 
		if (!storedObjectSpatialReferences.containsKey(objSpatialRefKey)) {
			// store the spatial reference to object
			cnt = 1;
			dataProvider.setId(dataProvider.getId() + 1);
			long spatialRefId = dataProvider.getId();
			psInsertSpatialReference.setLong(cnt++, spatialRefId); // id
			psInsertSpatialReference.setLong(cnt++, objId); // obj_id
			psInsertSpatialReference.setInt(cnt++, line); // line
			psInsertSpatialReference.setLong(cnt++, pSpatialRefValueId); // spatial_ref_id
			try {
				psInsertSpatialReference.executeUpdate();
				storedObjectSpatialReferences.put(objSpatialRefKey, spatialRefId);
			} catch (Exception e) {
				log.error("Error executing SQL: " + psInsertSpatialReference.toString(), e);
				throw e;
			}
			
			// update full text index
			JDBCHelper.updateObjectIndex(objId, locName, jdbc);
			JDBCHelper.updateObjectIndex(objId, snsTopicId, jdbc);
			JDBCHelper.updateObjectIndex(objId, fullAGSKey, jdbc);
			// update geothesaurus index
			if (snsTopicId != null) {
				JDBCHelper.updateObjectIndex(objId, snsTopicId, IDX_NAME_GEOTHESAURUS, jdbc); // SpatialRefSns.snsId
			}
			
			return true;
		} else {
			return false;
		}
	}

	protected void processT019Coordinates() throws Exception {

		String entityName = "t019_coordinates";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();

			// skip deleted rows
			if (row.get("mod_type") == null || invalidModTypes.contains(row.get("mod_type"))) {
				continue;
			}

			// skip rows with invalid object references
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
				continue;
			}

			/*
			 * if (row.get("geo_x1") == null) { if
			 * (log.isDebugEnabled()) { log.debug("Invalid entry in " +
			 * entityName + " found: geo_x1 is null. Skip record."); }
			 * row.clear(); } else if (row.get("geo_x2") == null) { if
			 * (log.isDebugEnabled()) { log.debug("Invalid entry in " +
			 * entityName + " found: geo_x2 is null. Skip record."); }
			 * row.clear(); } else if (row.get("geo_y1") == null) { if
			 * (log.isDebugEnabled()) { log.debug("Invalid entry in " +
			 * entityName + " found: geo_y1 is null. Skip record."); }
			 * row.clear(); } else if (row.get("geo_y2") == null) { if
			 * (log.isDebugEnabled()) { log.debug("Invalid entry in " +
			 * entityName + " found: geo_y2 is null. Skip record."); }
			 * row.clear();
			 */

			// bezug set ? skip rows where not set !
			String bezug = row.get("bezug");
			if (bezug == null || bezug.trim().length() == 0) {
				log.debug("Invalid entry in " + entityName + " found: bezug('" + bezug +
						"'), obj_id ('" + row.get("obj_id")	+ "'). Skip record.");
				row.clear();
				continue;
			}

			// OK, IS VALID ! write it ! 
			writeFreeSpatialReference(
					bezug,
					row.getDouble("geo_x1"),
					row.getDouble("geo_x2"),
					row.getDouble("geo_y1"),
					row.getDouble("geo_y2"),
					IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")),
					row.getInteger("line"),
					null);
		}
		psInsertSpatialReference.close();
		psInsertSpatialRefValue.close();
		psInsertSpatialReference = null;
		psInsertSpatialRefValue = null;

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT08AttrTyp() throws Exception {

		String entityName = "t08_attrtyp";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}
		pSqlStr = "INSERT INTO t08_attr_type (id, name, length, type) " + "VALUES (?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t08_attr_type";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t03_catalogue", "cat_id", row.get("cat_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: cat_id ('" + row.get("cat_id")
							+ "') not found in imported data of t03_catalogue. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setString(cnt++, row.get("attr_name")); // attr_id
				JDBCHelper.addInteger(p, cnt++, row.getInteger("length")); // length
				p.setString(cnt++, row.get("typ")); // type
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT08AttrList() throws Exception {

		String entityName = "t08_attrlist";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t08_attr_list (id, attr_type_id, type, listitem_line, listitem_value, lang_code) "
				+ "VALUES (?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t08_attr_list";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t08_attrtyp", "attr_id", row.get("attr_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: attr_id ('" + row.get("attr_id")
							+ "') not found in imported data of t08_attrtyp. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t08_attrtyp", "attr_id", row.get("attr_id"))); // attr_type_id
				p.setString(cnt++, "Z"); // type
				p.setInt(cnt++, row.getInteger("counter")); // listitem_line
				p.setString(cnt++, row.get("data")); // listitem_value
				p.setString(cnt++, getCatalogLanguage()); // lang_code
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT08Attr() throws Exception {

		String entityName = "t08_attr";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t08_attr (id, attr_type_id, obj_id, data) " + "VALUES (?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t08_attr";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (IDCStrategyHelper.getPK(dataProvider, "t08_attrtyp", "attr_id", row.get("attr_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: attr_id ('" + row.get("attr_id")
							+ "') not found in imported data of t08_attrtyp. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t08_attrtyp", "attr_id", row.get("attr_id"))); // attr_id
				p.setLong(cnt++, objId); // obj_id
				p.setString(cnt++, row.get("data")); // data
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				// update full text index
				JDBCHelper.updateObjectIndex(objId, row.get("data"), jdbc); // T08Attr.data
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT014InfoImpart() throws Exception {

		String entityName = "t014_info_impart";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t014_info_impart (id, obj_id, line, impart_value, impart_key) " + "VALUES (?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t014_info_impart";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNamesLowerCase = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1370 and lang_id='" + getCatalogLanguage() + "'";
		Statement st = jdbc.createStatement();
		ResultSet rs = jdbc.executeQuery(sql, st);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name"));
				allowedSpecialRefEntryNamesLowerCase.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		st.close();
		
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInteger("line")); // line
				// try to find entry in syslist
				int entryIndex = -1;
				if (row.get("name") != null) {
					entryIndex = allowedSpecialRefEntryNamesLowerCase.indexOf(row.get("name").toLowerCase());
				}
				String valueWritten = row.get("name");
				if (entryIndex != -1) {
					// we set also value from entry !!! necessary for mapping !
					valueWritten = allowedSpecialRefEntryNames.get(entryIndex);
					p.setString(cnt++, valueWritten); // impart_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(entryIndex))); // impart_key
				} else {
					p.setString(cnt++, valueWritten); // impart_value
					p.setInt(cnt++, -1); // impart_key
				}
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
				
				// update full text index
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				JDBCHelper.updateObjectIndex(objId, valueWritten, jdbc); // T014InfoImpart.impartValue
			}
		}
		p.close();
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void postProcess_spatialRefCatalogue() throws Exception {

		// set spatial ref id for the catalog
		// ----------------------------------
		if (log.isInfoEnabled()) {
			log.info("update spatial ref id for the catalog ...");
		}
		boolean spatialRefWritten = false;
		String fullAGS = null;
		for (Iterator<Row> i = dataProvider.getRowIterator("t03_catalogue"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String[] t071_stateFieldsWhere = new String[] { "country_id", "state_id" };
				String[] t071_stateValuesWhere = new String[] { row.get("country"), row.get("state") };
				String locTownNo = IDCStrategyHelper.getEntityFieldValue(dataProvider, "t071_state", 
						t071_stateFieldsWhere, t071_stateValuesWhere, "loc_town_no");
				fullAGS = IDCStrategyHelper.transformNativeKey2FullAgs(locTownNo);
				if (fullAGS == null || fullAGS.length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Problems mapping catalog state('" + row.get("state") + "') -> t071_state.loc_town_no('" + 
								locTownNo +	"') to full AGS Key.");						
					}

				} else {
					String sql = "SELECT id FROM spatial_ref_value WHERE nativekey='" + fullAGS + "'";
					Statement st = jdbc.createStatement();
					ResultSet rs = jdbc.executeQuery(sql, st);
					if (rs.next()) {
						Long id = rs.getLong("id");
						if (id != null && id.longValue() > 0) {
							jdbc.executeUpdate("UPDATE t03_catalogue SET spatial_ref_id = " + id + " WHERE id="
									+ row.getInteger("primary_key") + "");
							spatialRefWritten = true;
						}
						rs.close();
					}
					st.close();
				}
			}
		}
		if (!spatialRefWritten) {
			if (log.isInfoEnabled()) {
				log.info("Problems mapping computed catalog fullAGS '" + fullAGS + "' to spatial reference. No catalog spatial reference written.");						
			}
			
		}
	}
}
