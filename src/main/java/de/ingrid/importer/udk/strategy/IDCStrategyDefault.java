/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.Row;
import de.ingrid.importer.udk.util.UuidGenerator;

/**
 * @author Administrator
 * 
 */
public abstract class IDCStrategyDefault implements IDCStrategy {

	protected DataProvider dataProvider = null;

	protected ImportDescriptor importDescriptor = null;

	protected JDBCConnectionProxy jdbc = null;

	String sqlStr = null;

	String pSqlStr = null;

	private static Log log = LogFactory.getLog(IDCStrategyDefault.class);

	protected static ArrayList<String> invalidModTypes;

	private ArrayList<String> duplicateEntries;

	static String IDX_SEPARATOR = "|";  
	static String IDX_NAME_THESAURUS = "thesaurus";
	static String IDX_NAME_GEOTHESAURUS = "geothesaurus";

	public void setDataProvider(DataProvider data) {
		dataProvider = data;
	}

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc) {
		this.jdbc = jdbc;
	}

	public void setImportDescriptor(ImportDescriptor descriptor) {
		importDescriptor = descriptor;
	}

	public IDCStrategyDefault() {
		super();
		invalidModTypes = new ArrayList<String>();
		invalidModTypes.add("D");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public abstract void execute();

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
						&& IDCStrategyHelper.getEntityFieldValue(dataProvider, "t012_obj_obj", "object_to_id",
								row.get("obj_id"), "object_to_id").length() == 0) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry (outside the hierarchy) in " + entityName + " found: obj_id ('"
								+ row.get("obj_id")
								+ "') not found in t012_obj_obj.object_to_id and root == 0. Skip record.");
					}
					row.clear();
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
				} else {
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
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("time_from"))); // time_from
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("time_to"))); // time_to
					p.setString(cnt++, row.get("time_descr")); // time_descr
					JDBCHelper.addInteger(p, cnt++, row.getInteger("time_period")); // time_period
					p.setString(cnt++, row.get("time_interval")); // time_interval
					JDBCHelper.addInteger(p, cnt++, row.getInteger("time_status")); // time_status
					p.setString(cnt++, row.get("time_alle")); // time_alle
					p.setString(cnt++, row.get("time_type")); // time_type
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
					JDBCHelper.addInteger(p, cnt++, row.getInteger("vertical_extent_vdatum")); // vertical_extent_vdatum
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
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t02_address";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "DELETE FROM full_index_addr";
		jdbc.executeUpdate(sqlStr);
		
		final List<String> allowedSpecialRefTitleEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefTitleEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=4305;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefTitleEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefTitleEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		final List<String> allowedSpecialRefAddressEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefAddressEntryNames = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=4300;";
		rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefAddressEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefAddressEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
					JDBCHelper.addInteger(p, cnt++, row.getInteger("typ")); // typ
					p.setString(cnt++, row.get("institution")); // institution
					p.setString(cnt++, row.get("lastname")); // lastname
					p.setString(cnt++, row.get("firstname")); // firstname
					if (row.get("address") != null && allowedSpecialRefAddressEntryNames.contains(row.get("address").toLowerCase())) {
						p.setNull(cnt++, Types.VARCHAR); // address_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialRefAddressEntries.get(allowedSpecialRefAddressEntryNames.indexOf(row.get("address").toLowerCase())))); // address_key
					} else {
						p.setString(cnt++, row.get("address")); // address_value
						p.setInt(cnt++, -1); // address_key
					}
					if (row.get("title") != null && allowedSpecialRefTitleEntryNames.contains(row.get("title").toLowerCase())) {
						p.setNull(cnt++, Types.VARCHAR); // title_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialRefTitleEntries.get(allowedSpecialRefTitleEntryNames.indexOf(row.get("title").toLowerCase())))); // title_key
					} else {
						p.setString(cnt++, row.get("title")); // title_value
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
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("institution"), jdbc); // T02Address.institution
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("institution"), "partial", jdbc); // T02Address.institution in partial idx
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("lastname"), jdbc); // T02Address.lastname
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("lastname"), "partial", jdbc); // T02Address.lastname in partial idx
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("firstname"), jdbc); // T02Address.firstname
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("firstname"), "partial", jdbc); // T02Address.firstname in partial idx 
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("address"), jdbc); // T02Address.addressValue
					JDBCHelper.updateAddressIndex(row.getInteger("primary_key"), row.get("title"), jdbc); // T02Address.titleValue
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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT03Catalogue() throws Exception {

		String entityName = "t03_catalogue";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, country_code,"
				+ "workflow_control, expiry_duration, create_time, mod_uuid, mod_time, language_code) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

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
				p.setString(cnt++, "de"); // language_code
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				if (log.isDebugEnabled()) {
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

	protected void postProcess() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Post processing ...");
		}

		// get spatial ref id for the catalog
		for (Iterator<Row> i = dataProvider.getRowIterator("t03_catalogue"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String locTownNo = IDCStrategyHelper.getEntityFieldValue(dataProvider, "t071_state", "state_id", row
						.get("state"), "loc_town_no");
				String sql = "SELECT id FROM spatial_ref_value WHERE nativekey='"
						+ IDCStrategyHelper.transformNativeKey2FullAgs(locTownNo) + "'";
				ResultSet rs = jdbc.executeQuery(sql);
				if (rs.next()) {
					Long id = rs.getLong("id");
					if (id != null && id.longValue() > 0) {
						jdbc.executeUpdate("UPDATE t03_catalogue SET spatial_ref_id = " + id + " WHERE id="
								+ row.getInteger("primary_key") + ";");
					}
					rs.close();
				}
			}
		}
		
		// set the correct obj_node_id to the object index table
		// this is necessary, because the node_id is not yet known, when the index is created
		for (Iterator<Row> i = dataProvider.getRowIterator("t01_object"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String sql = "SELECT id FROM object_node WHERE obj_id="
					+ row.getInteger("primary_key");
				ResultSet rs = jdbc.executeQuery(sql);
				if (rs.next()) {
					jdbc.executeUpdate("UPDATE full_index_obj SET obj_node_id = " + rs.getLong("id") + " WHERE obj_node_id="
							+ row.getInteger("primary_key") + ";");
				}
				rs.close();
			}
		}
		
		// set the correct addr_node_id to the address index table
		// this is necessary, because the node_id is not yet known, when the index is created
		for (Iterator<Row> i = dataProvider.getRowIterator("t02_address"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				String sql = "SELECT id FROM address_node WHERE addr_id="
					+ row.getInteger("primary_key");
				ResultSet rs = jdbc.executeQuery(sql);
				if (rs.next()) {
					jdbc.executeUpdate("UPDATE full_index_addr SET addr_node_id = " + rs.getLong("id") + " WHERE addr_node_id="
							+ row.getInteger("primary_key") + ";");
				}
				rs.close();
			}
		}		
			
		// final closing separator in object index and address index
		jdbc.executeUpdate("UPDATE full_index_obj SET idx_value = concat(idx_value, '" + IDX_SEPARATOR + "');");
		jdbc.executeUpdate("UPDATE full_index_addr SET idx_value = concat(idx_value, '" + IDX_SEPARATOR + "');");

		if (log.isInfoEnabled()) {
			log.info("Post processing ... done.");
		}
	}

	protected void processT012ObjObj() throws Exception {

		String entityName = "t012_obj_obj";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrObjectNode = "INSERT INTO object_node (id, obj_uuid, obj_id, obj_id_published, fk_obj_uuid) VALUES "
				+ "(?, ?, ?, ?, ?);";
		String pSqlStrObjectReference = "INSERT INTO object_reference (id, obj_from_id, obj_to_uuid, line, special_ref, special_name, descr) VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement pSqlObjectNode = jdbc.prepareStatement(pSqlStrObjectNode);
		PreparedStatement pSqlObjectReference = jdbc.prepareStatement(pSqlStrObjectReference);

		sqlStr = "DELETE FROM object_node";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM object_reference";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		final List<String> importedObjectNodes = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=2000 AND entry_id IN (3100, 3210, 3345, 3515, 3520, 3535, 3555, 3570, 5066);";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();

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
					if (allowedSpecialRefEntries.contains(row.get("special_ref"))) {
						JDBCHelper.addInteger(pSqlObjectReference, cnt++, row.getInteger("special_ref")); // special_ref
						pSqlObjectReference.setString(cnt++, null); // special_name
					} else if (row.get("special_name") != null
							&& !allowedSpecialRefEntryNames.contains(row.get("special_name").toLowerCase())) {
						if (row.get("special_ref") != null && row.getInteger("special_ref") != 0
								&& !allowedSpecialRefEntries.contains(row.get("special_ref"))) {
							log.info("Invalid special_ref '" + row.get("special_ref")
									+ "' found. Reference will be imported as free entry with special_name='"
									+ row.get("special_name") + "'.");
						}
						pSqlObjectReference.setInt(cnt++, -1); // special_ref
						pSqlObjectReference.setString(cnt++, row.get("special_name")); // special_name
					} else if (row.get("special_name") != null && row.get("special_ref") != null
							&& row.getInteger("special_ref").intValue() != 0
							&& !allowedSpecialRefEntries.contains(row.get("special_ref"))) {
						log.info("Invalid special_ref '" + row.get("special_ref")
								+ "' found. Reference will be imported as free entry with special_name='"
								+ row.get("special_name") + "'.");
						pSqlObjectReference.setInt(cnt++, -1); // special_ref
						pSqlObjectReference.setString(cnt++, row.get("special_name")); // special_name
					} else if (row.get("special_name") != null
							&& allowedSpecialRefEntryNames.contains(row.get("special_name").toLowerCase())) {
						int specialReferenceTypeId = Integer.parseInt(allowedSpecialRefEntries
								.get(allowedSpecialRefEntryNames.indexOf(row.get("special_name").toLowerCase())));
						// get object class
						Integer objClass = IDCStrategyHelper.getEntityFieldValueAsInteger(dataProvider, "t01_object", "obj_id", row.get("obj_id"), "obj_class");
						if (objClass == null) {
							pSqlObjectReference.setInt(cnt++, -1); // special_ref
							pSqlObjectReference.setString(cnt++, row.get("special_name")); // special_name
						} else if (specialReferenceTypeId == 3210 && objClass.intValue() == 3) {
							// if object class corresponds with special reference type id
							pSqlObjectReference.setInt(cnt++, specialReferenceTypeId); // special_ref
							pSqlObjectReference.setNull(cnt++, Types.VARCHAR); // special_name
						} else if (specialReferenceTypeId == 3345 && objClass.intValue() == 2) {
							// if object class corresponds with special reference type id
							pSqlObjectReference.setInt(cnt++, specialReferenceTypeId); // special_ref
							pSqlObjectReference.setNull(cnt++, Types.VARCHAR); // special_name
						} else if ((specialReferenceTypeId == 3100) && objClass.intValue() == 5) {
							// if object class corresponds with special reference type id
							pSqlObjectReference.setInt(cnt++, specialReferenceTypeId); // special_ref
							pSqlObjectReference.setNull(cnt++, Types.VARCHAR); // special_name
						} else if ((specialReferenceTypeId == 3515 || specialReferenceTypeId == 3520 || specialReferenceTypeId == 3535 || specialReferenceTypeId == 3555 || specialReferenceTypeId == 3570 || specialReferenceTypeId == 5066) && objClass.intValue() == 1) {
							// if object class corresponds with special reference type id
							pSqlObjectReference.setInt(cnt++, specialReferenceTypeId); // special_ref
							pSqlObjectReference.setNull(cnt++, Types.VARCHAR); // special_name
						} else {
							pSqlObjectReference.setInt(cnt++, -1); // special_ref
							pSqlObjectReference.setString(cnt++, row.get("special_name")); // special_name
						}
					} else {
						log.error("Invalid special_ref='" + row.get("special_ref") + "' with special_name='"
								+ row.get("special_name") + "' found.");
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

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT022AdrAdr() throws Exception {

		String entityName = "t022_adr_adr";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO address_node (id, addr_uuid, addr_id, addr_id_published, fk_addr_uuid) VALUES (?, ?, ?, ?, ?);";

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
							.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_to_id"))); // addr_to_uuid
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

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT021Communication() throws Exception {

		String entityName = "t021_communication";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t021_communication (id, adr_id, line, commtype_value, commtype_key, comm_value, descr) VALUES (?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t021_communication";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=4430;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (row.get("comm_type") != null && allowedSpecialRefEntryNames.contains(row.get("comm_type").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // legist_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("comm_type").toLowerCase())))); // legist_key
				} else {
					p.setString(cnt++, row.get("comm_type")); // legist_value
					p.setInt(cnt++, -1); // legist_key
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
				
			}
		}
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
				+ "description) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_literature";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=3385;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (row.get("typ") != null && allowedSpecialRefEntryNames.contains(row.get("typ").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // type_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("typ").toLowerCase())))); // type_key
				} else {
					p.setString(cnt++, row.get("typ")); // type_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("typ"), jdbc); // T011ObjLiterature.typeValue
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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjData() throws Exception {

		String entityName = "t011_obj_data";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_data (id, obj_id, base, description) VALUES ( ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjDataParam() throws Exception {

		String entityName = "t011_obj_data_para";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_data_para (id, obj_id, line, parameter, unit) VALUES ( ?, ?, ?, ?, ?);";

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
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=5100;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (row.get("type") != null && allowedSpecialRefEntryNames.contains(row.get("type").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // type_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("type").toLowerCase())))); // type_key
				} else {
					p.setString(cnt++, row.get("type")); // type_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("type"), jdbc); // T011ObjServ.typeValue
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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServVersion() throws Exception {

		String entityName = "t011_obj_serv_version";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_version (id, obj_serv_id, line, serv_version) VALUES ( ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOperation() throws Exception {

		String entityName = "t011_obj_serv_operation";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_operation (id, obj_serv_id, line, name_value, name_key, descr, invocation_name) VALUES ( ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_operation";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialWMSRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialWMSRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=5110;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialWMSRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialWMSRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		final List<String> allowedSpecialWFSRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialWFSRefEntryNames = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=5120;";
		rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialWFSRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialWFSRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (serviceType != null && serviceType.equalsIgnoreCase("WMS")) {
					if (row.get("name") != null && allowedSpecialWMSRefEntryNames.contains(row.get("name").toLowerCase())) {
						p.setNull(cnt++, Types.VARCHAR); // name_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialWMSRefEntries.get(allowedSpecialWMSRefEntryNames.indexOf(row.get("name").toLowerCase())))); // name_key
					} else {
						p.setString(cnt++, row.get("name")); // name_value
						p.setInt(cnt++, -1); // name_key
					}
				} else if (serviceType != null && serviceType.equalsIgnoreCase("WFS")) {
					if (row.get("name") != null && allowedSpecialWFSRefEntryNames.contains(row.get("name").toLowerCase())) {
						p.setNull(cnt++, Types.VARCHAR); // name_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialWFSRefEntries.get(allowedSpecialWFSRefEntryNames.indexOf(row.get("name").toLowerCase())))); // name_key
					} else {
						p.setString(cnt++, row.get("name")); // name_value
						p.setInt(cnt++, -1); // name_key
					}
				} else {
					p.setString(cnt++, row.get("name")); // name_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("name"), jdbc); // T011ObjServOperation.nameValue
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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpPlatform() throws Exception {

		String entityName = "t011_obj_serv_op_platform";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_platform (id, obj_serv_op_id, line, platform) VALUES (?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpPara() throws Exception {

		String entityName = "t011_obj_serv_op_para";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_para (id, obj_serv_op_id, line, name, direction, descr, optional, repeatability) VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpDepends() throws Exception {

		String entityName = "t011_obj_serv_op_depends";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_depends (id, obj_serv_op_id, line, depends_on) VALUES (?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjServOpConnpoint() throws Exception {

		String entityName = "t011_obj_serv_op_connpoint";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_op_connpoint (id, obj_serv_op_id, line, connect_point) VALUES (?, ?, ?, ?);";

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
				+ "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

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
				JDBCHelper.addInteger(p, cnt++, row.getInteger("referencesystem_id")); // referencesystem_key
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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoKeyc() throws Exception {

		String entityName = "t011_obj_geo_keyc";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_keyc (id, obj_geo_id, line, keyc_value, keyc_key, key_date, edition) VALUES ( ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_keyc";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=3535;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (row.get("subject_cat") != null && allowedSpecialRefEntryNames.contains(row.get("subject_cat").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // keyc_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("subject_cat").toLowerCase())))); // keyc_key
				} else {
					p.setString(cnt++, row.get("subject_cat")); // keyc_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("subject_cat"), jdbc); // T011ObjGeoKeyc.keycValue
				JDBCHelper.updateObjectIndex(objId, IDCStrategyHelper.transDateTime(row.get("key_date")), jdbc); // T011ObjGeoKeyc.keyDate
				JDBCHelper.updateObjectIndex(objId, row.get("edition"), jdbc); // T011ObjGeoKeyc.edition
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoScale() throws Exception {

		String entityName = "t011_obj_geo_scale";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_scale (id, obj_geo_id, line, scale, resolution_ground, resolution_scan) VALUES ( ?, ?, ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoSpatialRep() throws Exception {

		String entityName = "t011_obj_geo_spatial_rep";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_spatial_rep (id, obj_geo_id, line, type) VALUES ( ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoSupplInfo() throws Exception {

		String entityName = "t011_obj_geo_supplinfo";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_supplinfo (id, obj_geo_id, line, feature_type) VALUES ( ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoVector() throws Exception {

		String entityName = "t011_obj_geo_vector";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_vector (id, obj_geo_id, line, geometric_object_type, geometric_object_count) VALUES ( ?, ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoSymc() throws Exception {

		String entityName = "t011_obj_geo_symc";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo_symc (id, obj_geo_id, line, symbol_cat_value, symbol_cat_key, symbol_date, edition) VALUES ( ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_symc";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=3555;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (row.get("symbol_cat") != null && allowedSpecialRefEntryNames.contains(row.get("symbol_cat").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // symbol_cat_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("symbol_cat").toLowerCase())))); // symbol_cat_key
				} else {
					p.setString(cnt++, row.get("symbol_cat")); // symbol_cat_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("symbol_cat"), jdbc); // T011ObjGeoSymc.symbolCatValue
				JDBCHelper.updateObjectIndex(objId, IDCStrategyHelper.transDateTime(row.get("symbol_date")), jdbc); // T011ObjGeoSymc.symbolDate
				JDBCHelper.updateObjectIndex(objId, row.get("edition"), jdbc); // T011ObjGeoSymc.edition
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjGeoTopicCat() throws Exception {

		String entityName = "t011_obj_geo_topic_cat";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_topic_cat (id, obj_id, line, topic_category) VALUES ( ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011ObjProject() throws Exception {

		String entityName = "t011_obj_project";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_project (id, obj_id, leader, member, description) VALUES ( ?, ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT015Legist() throws Exception {

		String entityName = "t015_legist";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t015_legist (id, obj_id, line, legist_value, legist_key) VALUES (?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t015_legist";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1350;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();

		
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
					if (row.get("name") != null && allowedSpecialRefEntryNames.contains(row.get("name").toLowerCase())) {
						p.setNull(cnt++, Types.VARCHAR); // legist_value
						p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("name").toLowerCase())))); // legist_key
					} else {
						p.setString(cnt++, row.get("name")); // legist_value
						p.setInt(cnt++, -1); // legist_key
					}
					try {
						p.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}
					
					// update full text index
					JDBCHelper.updateObjectIndex(objId, row.get("name"), jdbc); // T015Legist.legistValue
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT0113DatasetReference() throws Exception {

		String entityName = "t0113_dataset_reference";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t0113_dataset_reference (id, obj_id, line, reference_date, type) VALUES (?, ?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT0110AvailFormat() throws Exception {

		String entityName = "t0110_avail_format";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t0110_avail_format (id, obj_id, line, format_value, format_key, ver, file_decompression_technique, specification) VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t0110_avail_format";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1320;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
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
				if (row.get("name") != null && allowedSpecialRefEntryNames.contains(row.get("name").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // format_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("name").toLowerCase())))); // format_key
				} else {
					p.setString(cnt++, row.get("name")); // format_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("name"), jdbc); // T0110AvailFormat.formatValue
				JDBCHelper.updateObjectIndex(objId, row.get("version"), jdbc); // T0110AvailFormat.ver
				JDBCHelper.updateObjectIndex(objId, row.get("file_decompression_technique"), jdbc); // T0110AvailFormat.fileDecompressionTechnique
				JDBCHelper.updateObjectIndex(objId, row.get("specification"), jdbc); // T0110AvailFormat.specification
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT0112MediaOption() throws Exception {

		String entityName = "t0112_media_option";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t0112_media_option (id, obj_id, line, medium_note, medium_name, transfer_size) VALUES (?, ?, ?, ?, ?, ?);";

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
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t017_url_ref";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedDatatypeValues = new ArrayList<String>();
		final List<String> allowedDatatypeKeys = new ArrayList<String>();

		String sql = "SELECT name, entry_id FROM sys_list WHERE LST_ID=2240;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedDatatypeValues.add(rs.getString("name").toLowerCase());
				allowedDatatypeKeys.add(rs.getString("entry_id").toLowerCase());
			}
		}
		rs.close();
		
		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=2000 AND entry_id IN (3100, 3210, 3345, 3515, 3520, 3535, 3555, 3570, 5066);";
		rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();

		
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
				if (row.get("special_ref") != null && allowedSpecialRefEntries.contains(row.get("special_ref").toLowerCase())) {
					JDBCHelper.addInteger(p, cnt++, row.getInteger("special_ref")); // special_ref
					p.setNull(cnt++, Types.VARCHAR); // special_name
				} else if (row.get("special_name") != null && allowedSpecialRefEntryNames.contains(row.get("special_name").toLowerCase()) ) {
					JDBCHelper.addInteger(p, cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("special_name").toLowerCase())))); // special_ref
					p.setNull(cnt++, Types.VARCHAR); // special_name
				} else {
					p.setInt(cnt++, -1); // special_ref
					p.setString(cnt++, row.get("special_name")); // special_name
				}
				
				p.setString(cnt++, row.get("content")); // content
				if (row.get("datatype") == null || !allowedDatatypeValues.contains(row.get("datatype").toLowerCase())) {
					p.setString(cnt++, row.get("datatype")); // datatype_value
					p.setInt(cnt++, -1); // datatype_key
				} else {
					p.setNull(cnt++, Types.VARCHAR); // datatype_value
					p.setInt(cnt++, Integer.parseInt(allowedDatatypeKeys.get(allowedDatatypeValues.indexOf(row.get("datatype").toLowerCase())))); // datatype_key
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
				JDBCHelper.updateObjectIndex(objId, row.get("special_name"), jdbc); // T017UrlRef.specialName
				JDBCHelper.updateObjectIndex(objId, row.get("content"), jdbc); // T017UrlRef.content
				JDBCHelper.updateObjectIndex(objId, row.get("datatype"), jdbc); // T017UrlRef.datatypeValue
				JDBCHelper.updateObjectIndex(objId, row.get("volume"), jdbc); // T017UrlRef.volume
				JDBCHelper.updateObjectIndex(objId, row.get("icon_text"), jdbc); // T017UrlRef.iconText
				JDBCHelper.updateObjectIndex(objId, row.get("descr"), jdbc); // T017UrlRef.descr
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT011Township() throws Exception {

		String entityName = "t011_township";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSpatialReference = "INSERT INTO spatial_reference (id, obj_id, line, spatial_ref_id) "
				+ "VALUES (?, ?, ?, ?);";

		String pSqlStrSpatialRefValue = "INSERT INTO spatial_ref_value (id, type, spatial_ref_sns_id, name_value, name_key, nativekey, x1, x2, y1, y2) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		String pSqlStrSpatialRefSns = "INSERT INTO spatial_ref_sns (id, sns_id, expired_at) " + "VALUES (?, ?, ?);";

		PreparedStatement pSpatialReference = jdbc.prepareStatement(pSqlStrSpatialReference);
		PreparedStatement pSpatialRefValue = jdbc.prepareStatement(pSqlStrSpatialRefValue);
		// TODO: import SNS key, ask Till about it
		PreparedStatement pSpatialRefSns = jdbc.prepareStatement(pSqlStrSpatialRefSns);

		sqlStr = "DELETE FROM spatial_reference";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM spatial_ref_value";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM spatial_ref_sns";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1100;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
		
		HashMap<String, Long> storedNativekeys = new HashMap<String, Long>();

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
				long pSpatialRefValueId;
				long spatialRefSnsId = 0;
				String locName = "";
				String topicId = null;
				// if the spatial ref has been stored already, refere to the
				// already stored id
				if (storedNativekeys.containsKey(row.get("township_no"))) {
					pSpatialRefValueId = ((Long) storedNativekeys.get(row.get("township_no"))).longValue();
					topicId = IDCStrategyHelper.transformNativeKey2TopicId(row.get("township_no"));
				} else {
					if (row.get("township_no") != null) {
						topicId = IDCStrategyHelper.transformNativeKey2TopicId(row.get("township_no"));
						if (topicId.length() > 0) {
							// store the spatial ref sns values
							dataProvider.setId(dataProvider.getId() + 1);
							pSpatialRefSns.setLong(cnt++, dataProvider.getId()); // id
							pSpatialRefSns.setString(cnt++, topicId); // sns_id
							pSpatialRefSns.setString(cnt++, null); // expired_at
							try {
								pSpatialRefSns.executeUpdate();
								spatialRefSnsId = dataProvider.getId();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSpatialRefSns.toString(), e);
								throw e;
							}
						}
					}

					// store the spatial ref
					cnt = 1;
					dataProvider.setId(dataProvider.getId() + 1);

					pSpatialRefValue.setLong(cnt++, dataProvider.getId()); // id
					pSpatialRefValue.setString(cnt++, "G"); // type
					if (spatialRefSnsId > 0) {
						pSpatialRefValue.setLong(cnt++, spatialRefSnsId); // spatial_ref_sns_id
					} else {
						pSpatialRefValue.setNull(cnt++, Types.INTEGER); // spatial_ref_sns_id
					}
					if (row.get("township_no") == null) {
						if (log.isDebugEnabled()) {
							log.debug("Invalid ags key length:" + row.get("township_no"));
						}
					} else if (row.get("township_no").length() == 2) {
						locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
								"loc_town_no", row.get("township_no"), "state");
					} else if (row.get("township_no").length() == 3) {
						locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
								"loc_town_no", row.get("township_no"), "district").concat(" (District)");
					} else if (row.get("township_no").length() == 5) {
						locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
								"loc_town_no", row.get("township_no"), "country");
					} else if (row.get("township_no").length() == 8) {
						locName = IDCStrategyHelper.getEntityFieldValueStartsWith(dataProvider, "t01_st_township",
								"loc_town_no", row.get("township_no"), "township");
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Invalid ags key length:" + row.get("township_no"));
						}
					}
					pSpatialRefValue.setString(cnt++, locName); // name_value
					pSpatialRefValue.setInt(cnt++, -1); // name_key
					pSpatialRefValue.setString(cnt++, IDCStrategyHelper.transformNativeKey2FullAgs(row
							.get("township_no"))); // nativekey
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(
							dataProvider, "t01_st_bbox", "loc_town_no", row.get("township_no"), "x1")); // x1
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(
							dataProvider, "t01_st_bbox", "loc_town_no", row.get("township_no"), "x2")); // x2
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(
							dataProvider, "t01_st_bbox", "loc_town_no", row.get("township_no"), "y1")); // y1
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(
							dataProvider, "t01_st_bbox", "loc_town_no", row.get("township_no"), "y2")); // y2
					try {
						pSpatialRefValue.executeUpdate();
						pSpatialRefValueId = dataProvider.getId();
						storedNativekeys.put(row.get("township_no"), new Long(pSpatialRefValueId));
					} catch (Exception e) {
						log.error("Error executing SQL: " + pSpatialRefValue.toString(), e);
						throw e;
					}
					

				}
				cnt = 1;
				long objId = IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"));
				pSpatialReference.setInt(cnt++, row.getInteger("primary_key")); // id
				pSpatialReference.setLong(cnt++, objId); // obj_id
				pSpatialReference.setInt(cnt++, row.getInteger("line")); // line
				pSpatialReference.setLong(cnt++, pSpatialRefValueId); // spatial_ref_id
				try {
					pSpatialReference.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + pSpatialReference.toString(), e);
					throw e;
				}
				
				// update full text index
				JDBCHelper.updateObjectIndex(objId, locName, jdbc);
				JDBCHelper.updateObjectIndex(objId, topicId, jdbc);
				JDBCHelper.updateObjectIndex(objId, IDCStrategyHelper.transformNativeKey2FullAgs(row.get("township_no")), jdbc);
				// update geothesaurus index
				if (topicId != null) {
					JDBCHelper.updateObjectIndex(objId, topicId, IDX_NAME_GEOTHESAURUS, jdbc); // SpatialRefSns.snsId
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT019Coordinates() throws Exception {

		String entityName = "t019_coordinates";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSpatialReference = "INSERT INTO spatial_reference (id, obj_id, line, spatial_ref_id) "
				+ "VALUES (?, ?, ?, ?);";

		String pSqlStrSpatialRefValue = "INSERT INTO spatial_ref_value (id, type, spatial_ref_sns_id, name_value, name_key, nativekey, x1, x2, y1, y2) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement pSpatialReference = jdbc.prepareStatement(pSqlStrSpatialReference);
		PreparedStatement pSpatialRefValue = jdbc.prepareStatement(pSqlStrSpatialRefValue);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();
		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1100;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
		
		HashMap<String, Long> storedNativekeys = new HashMap<String, Long>();

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
				/*
				 * } else if (row.get("geo_x1") == null) { if
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

			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long pSpatialRefValueId;
				// if the spatial ref has been stored already, refere to the
				// already stored id
				// create key
				String geoKey = row.get("geo_x1") + row.get("geo_x2") + row.get("geo_y1") + row.get("geo_y1")
						+ row.get("bezug");
				if (storedNativekeys.containsKey(geoKey)) {
					pSpatialRefValueId = ((Long) storedNativekeys.get(geoKey)).longValue();
				} else {
					// store the spatial ref
					dataProvider.setId(dataProvider.getId() + 1);

					pSpatialRefValue.setLong(cnt++, dataProvider.getId()); // id
					pSpatialRefValue.setString(cnt++, "F"); // type
					pSpatialRefValue.setNull(cnt++, Types.INTEGER); // spatial_ref_sns_id
					if (row.get("bezug") != null && allowedSpecialRefEntryNames.contains(row.get("bezug").toLowerCase())) {
						pSpatialRefValue.setNull(cnt++, Types.VARCHAR); // name_value
						pSpatialRefValue.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("bezug").toLowerCase())))); // name_key
					} else {
						pSpatialRefValue.setString(cnt++, row.get("bezug")); // name_value
						pSpatialRefValue.setInt(cnt++, -1); // name_key
					}
					pSpatialRefValue.setString(cnt++, ""); // nativekey
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, row.getDouble("geo_x1")); // x1
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, row.getDouble("geo_x2")); // x2
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, row.getDouble("geo_y1")); // y1
					JDBCHelper.addDouble(pSpatialRefValue, cnt++, row.getDouble("geo_y2")); // y1
					try {
						pSpatialRefValue.executeUpdate();
						pSpatialRefValueId = dataProvider.getId();
						storedNativekeys.put(geoKey, new Long(pSpatialRefValueId));
					} catch (Exception e) {
						log.error("Error executing SQL: " + pSpatialRefValue.toString(), e);
						throw e;
					}
				}
				cnt = 1;
				pSpatialReference.setInt(cnt++, row.getInteger("primary_key")); // id
				pSpatialReference.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
						.get("obj_id"))); // obj_id
				pSpatialReference.setInt(cnt++, row.getInteger("line")); // line
				pSpatialReference.setLong(cnt++, pSpatialRefValueId); // spatial_ref_id
				try {
					pSpatialReference.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + pSpatialReference.toString(), e);
					throw e;
				}
			}
		}
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
				+ "special_ref, special_name, mod_time) VALUES " + "( ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t012_obj_adr";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=2010 AND entry_id IN (3360, 3400, 3410);";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
		
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (row.get("obj_id") == null || row.get("obj_id").length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: obj_id not set. Skip record.");
					}
					row.clear();
				} else if (row.get("adr_id") == null || row.get("adr_id").length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: adr_id not set. Skip record.");
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
								+ "') not found in imported data of t02_address. Skip record.");
					}
					row.clear();
				} else if (!IDCStrategyHelper.isValidUdkAddressType(row.getInteger("typ"), row.get("special_name")) && row.get("typ") != null && row.getInteger("typ").intValue() != 0) {
					log.info("Invalid entry in " + entityName + " found: typ ('" + row.get("typ")
							+ "') does not correspond with special_name ('" + row.get("special_name")
							+ "'). Skip record.");
					row.clear();
				} else {
					int cnt = 1;
					p.setInt(cnt++, row.getInteger("primary_key")); // id
					p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
					p.setString(cnt++, row.get("adr_id")); // adr_uuid
					
					Integer type = null;
					Integer specialRef = null;
					String specialName = null;
					
					if (row.getInteger("typ") == null || row.getInteger("typ").intValue() == 999 || row.getInteger("typ").intValue() == -1) {
						type = new Integer(-1);
					// if type is valid
					} else if (row.getInteger("typ").intValue() >= 0 && row.getInteger("typ").intValue() <= 9) {
						type = IDCStrategyHelper.transAddressTypeUdk2Idc(row.getInteger("typ"));
						specialRef = new Integer(505);
					// if typ is invalid
					} else  {
						log.info("Invalid udk address type detected (type='" + row.getInteger("typ")
								+ "', special_name='" + row.get("special_name")
								+ "'). The record will be imported as free entry.");
						type = new Integer(-1);
					}
					
					if (specialRef == null && (row.get("special_ref") != null && allowedSpecialRefEntries.contains(row.get("special_ref")))) {
						// if special_ref is valid
						specialRef = row.getInteger("special_ref");
					} else if (specialRef == null && specialName != null) {
						if (!allowedSpecialRefEntryNames.contains(row.get("special_name").toLowerCase())) {
							// if special_name is not in lookup list
							specialName = row.get("special_name");
						} else {
							//	if special_name is in lookup list, check against object classes for valid ids
							Integer specialReferenceTypeId = Integer.getInteger(allowedSpecialRefEntries
									.get(allowedSpecialRefEntryNames.indexOf(row.get("special_name").toLowerCase())));
							Integer objClass = IDCStrategyHelper.getEntityFieldValueAsInteger(dataProvider, "t01_object", "obj_id", row.get("obj_id"), "obj_class");
							if (objClass == null) {
								// if not valid import as free entry
								specialRef = null;
								specialName = row.get("special_name");
							} else if (specialReferenceTypeId.intValue() == 3360 && objClass.intValue() == 2) {
								specialRef = specialReferenceTypeId;
							} else if ((specialReferenceTypeId.intValue() == 3400 || specialReferenceTypeId.intValue() == 3410)&& objClass.intValue() == 4) {
								specialRef = specialReferenceTypeId;
							} else {
								// if not valid import as free entry
								specialRef = null;
								specialName = row.get("special_name");
							}
						}
					} else {
						//import in all other cases as free value, should not be the case
						specialRef = null;
						specialName = row.get("special_name");
					}

					JDBCHelper.addInteger(p, cnt++, type ); // type
					JDBCHelper.addInteger(p, cnt++, row.getInteger("line") ); // line
					JDBCHelper.addInteger(p, cnt++, specialRef ); // special_ref
					JDBCHelper.addString(p, cnt++, specialName ); // special_name

					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("mod_time"))); // mod_time
					try {
						p.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + p.toString(), e);
						throw e;
					}
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT04Search() throws Exception {

		String entityName = "t04_search";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSearchtermObj = "INSERT INTO searchterm_obj (id, obj_id, line, searchterm_id) VALUES ( ?, ?, ?, ?);";
		String pSqlStrSearchtermAdr = "INSERT INTO searchterm_adr (id, adr_id, line, searchterm_id) VALUES ( ?, ?, ?, ?);";
		String pSqlStrSearchtermValue = "INSERT INTO searchterm_value (id, type, term, searchterm_sns_id) VALUES ( ?, ?, ?, ?);";
		String pSqlStrSearchtermSns = "INSERT INTO searchterm_sns (id, sns_id, expired_at) VALUES ( ?, ?, ?);";

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
				+ "VALUES (?, ?, ?, ?, ?, ?);";

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
				p.setString(cnt++, "de"); // lang_code
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT08AttrTyp() throws Exception {

		String entityName = "t08_attrtyp";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}
		pSqlStr = "INSERT INTO t08_attr_type (id, name, length, type) " + "VALUES (?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processT08Attr() throws Exception {

		String entityName = "t08_attr";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t08_attr (id, attr_type_id, obj_id, data) " + "VALUES (?, ?, ?, ?);";

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
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void processSysList() throws Exception {

		String entityName = "sys_list";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, description, maintainable) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM sys_list";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();

			if (row.getInteger("lst_id") == 1000) {
				// ignore list with id==1000, codelist 505 will be used instead
			} else if (row.getInteger("lst_id") == 3571 && row.getInteger("entry_id") == 4) {
				// ignore list with id==3571 and entry_id==4, codelist 505 will
				// be used instead
			} else {
				int cnt = 1;
				p.setInt(cnt++, row.getInteger("primary_key")); // id
				p.setInt(cnt++, row.getInteger("lst_id")); // lst_id
				p.setInt(cnt++, row.getInteger("entry_id")); // entry_id
				p.setString(cnt++, IDCStrategyHelper.transLanguageCode(row.get("lang_id"))); // lang_id
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, null); // description
				JDBCHelper.addInteger(p, cnt++, row.getInteger("maintainable")); // maintainable
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}

		entityName = "sys_codelist_domain";
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}
		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			int cnt = 1;
			p.setInt(cnt++, row.getInteger("primary_key")); // id
			p.setInt(cnt++, row.getInteger("codelist_id")); // lst_id
			p.setInt(cnt++, row.getInteger("domain_id")); // entry_id
			p.setString(cnt++, IDCStrategyHelper.transLanguageCode(row.get("lang_id"))); // lang_id
			p.setString(cnt++, row.get("name")); // name
			p.setString(cnt++, row.get("description")); // description
			p.setInt(cnt++, 0); // maintainable
			try {
				p.executeUpdate();
			} catch (Exception e) {
				log.error("Error executing SQL: " + p.toString(), e);
				throw e;
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
		if (log.isInfoEnabled()) {
			log.info("Importing special values...");
		}
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 1, 'de', 'Daten und Karten', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 2, 'de', 'Konzeptionelles', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 3, 'de', 'Rechtliches', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 4, 'de', 'Risikobewertungen', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 5, 'de', 'Statusberichte', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1400, 6, 'de', 'Umweltzustand', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 1, 'de', 'Abfall', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 2, 'de', 'Altlasten', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 3, 'de', 'Bauen', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 4, 'de', 'Boden', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 5, 'de', 'Chemikalien', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 6, 'de', 'Energie', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 7, 'de', 'Forstwirtschaft', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 8, 'de', 'Gentechnik', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 9, 'de', 'Geologie', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 10, 'de', 'Gesundheit', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 11, 'de', 'Lärm und Erschütterungen', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 12, 'de', 'Landwirtschaft', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 13, 'de', 'Luft und Klima', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 14, 'de', 'Nachhaltige Entwicklung', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 15, 'de', 'Natur und Landschaft', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 16, 'de', 'Strahlung', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 17, 'de', 'Tierschutz', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 18, 'de', 'Umweltinformationen', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 19, 'de', 'Umweltwirtschaft', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 20, 'de', 'Verkehr', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 1410, 21, 'de', 'Wasser', 0);");

		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3100, 'de', 'Methode / Datengrundlage', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3210, 'de', 'Basisdaten', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3345, 'de', 'Basisdaten', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3515, 'de', 'Herstellungsprozess', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3520, 'de', 'Fachliche Grundlage', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3535, 'de', 'Schlüsselkatalog', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3555, 'de', 'Symbolkatalog', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 3570, 'de', 'Datengrundlage', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2000, 5066, 'de', 'Verweis zu Dienst', 0);");

		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3360, 'de', 'Standort', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3400, 'de', 'Projektleiter', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2010, 3410, 'de', 'Beteiligte', 0);");

		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5100, 1, 'de', 'WMS', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5100, 2, 'de', 'WFS', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5110, 1, 'de', 'GetCapabilities', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5110, 2, 'de', 'GetMap', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5110, 3, 'de', 'GetFeatureInfo', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 1, 'de', 'DescribeFeatureType', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 2, 'de', 'GetFeature', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 3, 'de', 'GetFeature', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 4, 'de', 'LockFeature', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 5120, 5, 'de', 'Transaction', 0);");

		// remove old values
		jdbc.executeUpdate("DELETE FROM sys_list WHERE lst_id=2240;");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 1, 'de', 'HTML', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 2, 'de', 'JPG', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 3, 'de', 'PNG', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 4, 'de', 'GIF', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 5, 'de', 'PDF', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 6, 'de', 'DOC', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 7, 'de', 'PPT', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 8, 'de', 'XLS', 0);");
		dataProvider.setId(dataProvider.getId() + 1);
		jdbc.executeUpdate("INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable) VALUES ("
				+ dataProvider.getId() + ", 2240, 9, 'de', 'ASCII/Text', 0);");

		if (log.isInfoEnabled()) {
			log.info("Importing special values... done.");
		}
	}

	protected void processT014InfoImpart() throws Exception {

		String entityName = "t014_info_impart";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t014_info_impart (id, obj_id, line, impart_value, impart_key) " + "VALUES (?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t014_info_impart";
		jdbc.executeUpdate(sqlStr);

		final List<String> allowedSpecialRefEntries = new ArrayList<String>();
		final List<String> allowedSpecialRefEntryNames = new ArrayList<String>();

		String sql = "SELECT entry_id, name FROM sys_list WHERE lst_id=1370;";
		ResultSet rs = jdbc.executeQuery(sql);
		while (rs.next()) {
			if (rs.getString("name") != null) {
				allowedSpecialRefEntryNames.add(rs.getString("name").toLowerCase());
				allowedSpecialRefEntries.add(rs.getString("entry_id"));
			}
		}
		rs.close();
		
		
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
				if (row.get("name") != null && allowedSpecialRefEntryNames.contains(row.get("name").toLowerCase())) {
					p.setNull(cnt++, Types.VARCHAR); // impart_value
					p.setInt(cnt++, Integer.parseInt(allowedSpecialRefEntries.get(allowedSpecialRefEntryNames.indexOf(row.get("name").toLowerCase())))); // impart_key
				} else {
					p.setString(cnt++, row.get("name")); // impart_value
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
				JDBCHelper.updateObjectIndex(objId, row.get("name"), jdbc); // T014InfoImpart.impartValue
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "... done.");
		}
	}

	protected void setHiLoGenerator() throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + (int)(dataProvider.getId() / Short.MAX_VALUE + 1) + ")";
		jdbc.executeUpdate(sqlStr);
	}

	protected void importDefaultUserdata() throws Exception {
		sqlStr = "DELETE FROM idc_group";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM idc_user";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM permission";
		jdbc.executeUpdate(sqlStr);
		sqlStr = "DELETE FROM idc_user_permission";
		jdbc.executeUpdate(sqlStr);
		
		// import default admin adress
		dataProvider.setId(dataProvider.getId() + 1);
		long adrId = dataProvider.getId();
		String uuid = UuidGenerator.getInstance().generateUuid();
		String sqlStr = "INSERT INTO t02_address (id, adr_uuid, org_adr_id, "
			+ "adr_type, institution, lastname, firstname, address_value, address_key, title_value, title_key, "
			+ "street, postcode, postbox, postbox_pc, city, country_code, job, "
			+ "descr, lastexport_time, expiry_time, work_state, work_version, "
			+ "mark_deleted, create_time, mod_time, mod_uuid, responsible_uuid) VALUES "
			+ "( " + dataProvider.getId() + ", '" + uuid + "', NULL, 3, NULL, 'admin', 'admin', 'Frau', -1, 'Dr.', -1, "
			+ "NULL, NULL, NULL, NULL, NULL, NULL, 'Administrator of this catalog.', "
			+ "'Administrator of this catalog.', NULL, NULL, 'V', 0, "
			+ "'N', NULL, NULL, NULL, NULL);";
		jdbc.executeUpdate(sqlStr);
		
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO `address_node` ( `id` , `addr_uuid` , `addr_id` , `addr_id_published` , `fk_addr_uuid` ) VALUES ( " + dataProvider.getId() + ", '" + uuid + "', "+adrId+", "+adrId+", NULL )"; 
		jdbc.executeUpdate(sqlStr);

		// import default admin group
		dataProvider.setId(dataProvider.getId() + 1);
		long groupId = dataProvider.getId();
		sqlStr = "INSERT INTO idc_group ( id, name) VALUES (" + groupId + ", 'administrators');";
		jdbc.executeUpdate(sqlStr);
		
		// import default admin user
		dataProvider.setId(dataProvider.getId() + 1);
		long userId = dataProvider.getId();
		sqlStr = "INSERT INTO idc_user ( id, addr_uuid, idc_group_id, idc_role) VALUES (" + userId + ", '"+uuid+"', "+groupId+", 1 );";
		jdbc.executeUpdate(sqlStr);
		
		// import permissions
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcObjectPermission', 'object', 'write');";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcObjectPermission', 'object', 'write-tree');";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcAddressPermission', 'address', 'write');";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcAddressPermission', 'address', 'write-tree');";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		long permissionCreateCatalodId = dataProvider.getId();
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcUserPermission', 'catalog', 'create-root');";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		long permissionCreateQaId = dataProvider.getId();
		sqlStr = "INSERT INTO permission ( id , class_name , name , action ) VALUES ( " + dataProvider.getId() + ", 'IdcUserPermission', 'catalog', 'qa');";
		jdbc.executeUpdate(sqlStr);
		
		// import user permissions
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO idc_user_permission ( id , permission_id , idc_user_id ) VALUES ( " + dataProvider.getId() + ", "+permissionCreateCatalodId+", "+userId+");";
		jdbc.executeUpdate(sqlStr);
		dataProvider.setId(dataProvider.getId() + 1);
		sqlStr = "INSERT INTO idc_user_permission ( id , permission_id , idc_user_id ) VALUES ( " + dataProvider.getId() + ", "+permissionCreateQaId+", "+userId+");";
		jdbc.executeUpdate(sqlStr);
	}
	
}
