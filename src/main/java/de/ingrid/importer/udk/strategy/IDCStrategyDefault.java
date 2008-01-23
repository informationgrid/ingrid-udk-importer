/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.Row;

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

		pSqlStr = "INSERT INTO t01_object (id, obj_uuid, obj_name, org_obj_id, root, obj_class, "
				+ "obj_descr, cat_id, info_note, avail_access_note, loc_descr, time_from, time_to, "
				+ "time_descr, time_period, time_interval, time_status, time_alle, time_type, "
				+ "publish_id, dataset_alternate_name, dataset_character_set, dataset_usage, "
				+ "data_language_code, metadata_character_set, metadata_standard_name, "
				+ "metadata_standard_version, metadata_language_code, vertical_extent_minimum, "
				+ "vertical_extent_maximum, vertical_extent_unit, vertical_extent_vdatum, fees, "
				+ "ordering_instructions, lastexport_time, expiry_time, work_state, work_version, "
				+ "mark_deleted, create_time, mod_time, mod_uuid, responsible_uuid) "
				+ "VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t01_object";
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
				if (!row.get("root").equals("1") &&  IDCStrategyHelper.getEntityFieldValue(dataProvider, "t012_obj_obj", "object_to_id", row.get("obj_id"), "object_to_id").length() == 0) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry (outside the hierarchy) in " + entityName + " found: obj_id ('" + row.get("obj_id")
								+ "') not found in t012_obj_obj.object_to_id and root != 1. Skip record.");
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
					p.setInt(cnt++, row.getInt("primary_key")); // id
					p.setString(cnt++, row.get("obj_id")); // obj_uuid
					p.setString(cnt++, row.get("obj_name")); // obj_name
					p.setString(cnt++, row.get("org_id")); // org_obj_id
					p.setInt(cnt++, row.getInt("root")); // root
					p.setInt(cnt++, row.getInt("obj_class")); // class_id
					
					if (row.get("obj_descr") != null) {
						// check for max length of the underlying text field, take the multi byte characterset into account.
						byte[] bArray = row.get("obj_descr").getBytes("UTF-8");
						if (bArray.length > 65535) {
							p.setString(cnt++, new String(bArray, 0, 65535, "UTF-8")); // obj_descr
						} else { 
							p.setString(cnt++, row.get("obj_descr")); // obj_descr
						}
					} else {
						p.setString(cnt++, null); // obj_descr
					}
					p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t03_catalogue", "cat_id", row.get("cat_id"))); // cat_id
					p.setString(cnt++, row.get("info_note")); // info_note
					p.setString(cnt++, row.get("avail_access_note")); // avail_access_note
					p.setString(cnt++, row.get("loc_descr")); // loc_descr
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("time_from"))); // time_from
					p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("time_to"))); // time_to
					p.setString(cnt++, row.get("time_descr")); // time_descr
					p.setInt(cnt++, row.getInt("time_period")); // time_period
					p.setString(cnt++, row.get("time_interval")); // time_interval
					p.setInt(cnt++, row.getInt("time_status")); // time_status
					p.setString(cnt++, row.get("time_alle")); // time_alle
					p.setString(cnt++, row.get("time_type")); // time_type
					p.setInt(cnt++, row.getInt("publish_id")); // publish_id,
					p.setString(cnt++, row.get("dataset_alternate_name")); // dataset_alternate_name
					p.setInt(cnt++, row.getInt("dataset_character_set")); // dataset_character_set
					p.setString(cnt++, row.get("dataset_usage")); // dataset_usage
					p.setString(cnt++, row.get("data_language_code")); // data_language_code
					p.setInt(cnt++, row.getInt("metadata_character_set")); // metadata_character_set
					p.setString(cnt++, row.get("metadata_standard_name")); // metadata_standard_name
					p.setString(cnt++, row.get("metadata_standard_version")); // metadata_standard_version
					p.setString(cnt++, row.get("metadata_language_code")); // metadata_language_code
					p.setDouble(cnt++, row.getDouble("vertical_extent_minimum")); // vertical_extent_minimum
					p.setDouble(cnt++, row.getDouble("vertical_extent_maximum")); // vertical_extent_maximum
					p.setInt(cnt++, row.getInt("vertical_extent_unit")); // vertical_extent_unit
					p.setInt(cnt++, row.getInt("vertical_extent_vdatum")); // vertical_extent_vdatum
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

		pSqlStr = "INSERT INTO t02_address (id, adr_uuid, org_adr_id, cat_id, "
				+ "root, adr_type, institution, lastname, firstname, address, title, "
				+ "street, postcode, postbox, postbox_pc, city, country_code, job, "
				+ "descr, lastexport_time, expiry_time, work_state, work_version, "
				+ "mark_deleted, create_time, mod_time, mod_uuid, responsible_uuid) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t02_address";
		jdbc.executeUpdate(sqlStr);

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
				if (!row.get("root").equals("1") &&  IDCStrategyHelper.getEntityFieldValue(dataProvider, "t022_adr_adr", "adr_to_id", row.get("adr_id"), "adr_to_id").length() == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: adr_id ('" + row.get("adr_id")
								+ "') not found in t022_adr_adr and root != 1. Skip record.");
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
					p.setInt(cnt++, row.getInt("primary_key")); // id
					p.setString(cnt++, row.get("adr_id")); // adr_uuid
					p.setString(cnt++, row.get("org_adr_id")); // org_adr_id
					p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t03_catalogue", "cat_id", row.get("cat_id"))); // cat_id
					p.setInt(cnt++, row.getInt("root")); // root
					p.setInt(cnt++, row.getInt("typ")); // adr_type
					p.setString(cnt++, row.get("institution")); // institution
					p.setString(cnt++, row.get("lastname")); // lastname
					p.setString(cnt++, row.get("firstname")); // firstname
					p.setString(cnt++, row.get("address")); // address
					p.setString(cnt++, row.get("title")); // title
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

		pSqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, country_code, "
				+ "workflow_control, expiry_duration, create_time, mod_uuid, mod_time) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?);";

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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("cat_id")); // cat_uuid
				p.setString(cnt++, row.get("catalogue")); // cat_name
				p.setString(cnt++, IDCStrategyHelper.transCountryCode(row.get("country"))); // country_code
				p.setString(cnt++, "N"); // workflow_control
				p.setInt(cnt++, 0); // expiry_duration
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("create_time"))); // create_time
				String modId = row.get("mod_id");
				if (IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", modId) == 0) {
					modId = row.get("create_id");
				}
				p.setString(cnt++, modId); // mod_uuid,
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("mod_time"))); // mod_time
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
				if (row.getInt("typ") == 0) {
					// structure
					pSqlObjectNode.setInt(cnt++, row.getInt("primary_key")); // id
					pSqlObjectNode.setString(cnt++, row.get("object_to_id")); // object_uuid
					pSqlObjectNode.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
							.get("object_to_id"))); // object_id
					pSqlObjectNode.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
							.get("object_to_id"))); // object_id_published
					pSqlObjectNode.setString(cnt++, row.get("object_from_id")); // fk_obj_uuid
					try {
						pSqlObjectNode.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + pSqlObjectNode.toString(), e);
						throw e;
					}
				} else if (row.getInt("typ") == 1) {
					pSqlObjectReference.setInt(cnt++, row.getInt("primary_key")); // id
					pSqlObjectReference.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
							.get("object_from_id"))); // object_from_uuid
					pSqlObjectReference.setString(cnt++, row.get("object_to_id")); // object_to_uuid
					pSqlObjectReference.setInt(cnt++, row.getInt("line")); // line
					pSqlObjectReference.setInt(cnt++, row.getInt("special_ref")); // special_ref
					pSqlObjectReference.setString(cnt++, row.get("special_name")); // special_name
					pSqlObjectReference.setString(cnt++, row.get("descr")); // descr
					try {
						pSqlObjectReference.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + pSqlObjectReference.toString(), e);
						throw e;
					}
				}
			}
		}
		// insert root objects into object_node
		duplicateEntries = new ArrayList<String>();
		for (Iterator<Row> i = dataProvider.getRowIterator("t01_object"); i.hasNext();) {
			Row row = i.next();
			int cnt = 1;
			if (row.getInt("root") == 1 && row.get("mod_type") != null
					&& !invalidModTypes.contains(row.get("mod_type"))) {
				long id = dataProvider.getId();
				id++;
				pSqlObjectNode.setLong(cnt++, id); // id
				dataProvider.setId(id);
				pSqlObjectNode.setString(cnt++, row.get("obj_id")); // object_uuid
				pSqlObjectNode.setInt(cnt++, row.getInt("primary_key")); // object_id
				pSqlObjectNode.setInt(cnt++, row.getInt("primary_key")); // object_id_published
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
					p.setInt(cnt++, row.getInt("primary_key")); // id
					p.setString(cnt++, row.get("adr_to_id")); // addr_uuid
					p.setInt(cnt++, IDCStrategyHelper
							.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_to_id"))); // addr_to_uuid
					p.setInt(cnt++, IDCStrategyHelper
							.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_to_id"))); // addr_id_published
					p.setString(cnt++, row
							.get("adr_from_id")); // fk_addr_uuid
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
			if (row.getInt("root") == 1 && row.get("mod_type") != null
					&& !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long id = dataProvider.getId();
				id++;
				p.setLong(cnt++, id); // id
				dataProvider.setId(id);
				p.setString(cnt++, row.get("adr_id")); // addr_uuid
				p.setInt(cnt++, row.getInt("primary_key")); // addr_id
				p.setInt(cnt++, row.getInt("primary_key")); // addr_id_published
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

		pSqlStr = "INSERT INTO t021_communication (id, adr_id, line, comm_type, comm_value, descr) VALUES (?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t021_communication";
		jdbc.executeUpdate(sqlStr);

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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id", row.get("adr_id"))); // adr_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("comm_type")); // comm_type
				p.setString(cnt++, row.get("comm_value")); // comm_value
				p.setString(cnt++, row.get("descr")); // descr
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

	protected void processT011ObjLiteratur() throws Exception {

		String entityName = "t011_obj_literatur";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_literature (id, obj_id, author, publisher, type, publish_in, "
				+ "volume, sides, publish_year, publish_loc, loc, doc_info, base, isbn, publishing, "
				+ "description) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_literature";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setString(cnt++, row.get("autor")); // author
				p.setString(cnt++, row.get("publisher")); // publisher
				p.setString(cnt++, row.get("typ")); // type
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setString(cnt++, row.get("base")); // base
				p.setString(cnt++, row.get("description")); // description
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("parameter")); // parameter
				p.setString(cnt++, row.get("unit")); // unit
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

	protected void processT011ObjServ() throws Exception {

		String entityName = "t011_obj_serv";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv (id, obj_id, type, history, environment, base, description) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setString(cnt++, row.get("special_base")); // special_base
				p.setString(cnt++, row.get("type")); // type
				p.setString(cnt++, row.get("history")); // history
				p.setString(cnt++, row.get("base")); // base
				p.setString(cnt++, row.get("description")); // description
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("version")); // serv_version
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

	protected void processT011ObjServOperation() throws Exception {

		String entityName = "t011_obj_serv_operation";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_serv_operation (id, obj_serv_id, line, name, descr, invocation_name) VALUES ( ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_serv_operation";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_serv", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, row.get("descr")); // descr
				p.setString(cnt++, row.get("invocation_name")); // invocation_name
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				p.setInt(cnt++, row.getInt("dcp_line")); // line
				p.setString(cnt++, row.get("platform")); // platform
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				p.setInt(cnt++, row.getInt("para_line")); // line
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, row.get("direction")); // direction
				p.setString(cnt++, row.get("descr")); // descr
				p.setInt(cnt++, row.getInt("optional")); // optional
				p.setInt(cnt++, row.getInt("repeatability")); // repeatability
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				p.setInt(cnt++, row.getInt("dep_line")); // line
				p.setString(cnt++, row.get("depends_on")); // depends_on
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, fk); // obj_id
				p.setInt(cnt++, row.getInt("dep_line")); // line
				p.setString(cnt++, row.get("depends_on")); // depends_on
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

	protected void processT011ObjGeo() throws Exception {

		String entityName = "t011_obj_geo";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t011_obj_geo (id, obj_id, special_base, data_base, method, coord, rec_exact, rec_grade, hierarchy_level, "
				+ "vector_topology_level, referencesystem_id, pos_accuracy_vertical, keyc_incl_w_dataset) "
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
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setString(cnt++, row.get("special_base")); // special_base
				p.setString(cnt++, row.get("data_base")); // data_base
				p.setString(cnt++, row.get("method")); // method
				p.setString(cnt++, row.get("coord")); // coord
				p.setDouble(cnt++, row.getDouble("rec_exact")); // rec_exact
				p.setDouble(cnt++, row.getDouble("rec_grade")); // rec_grade
				p.setInt(cnt++, row.getInt("hierarchy_level")); // hierarchy_level
				p.setInt(cnt++, row.getInt("vector_topology_level")); // vector_topology_level
				p.setInt(cnt++, row.getInt("referencesystem_id")); // referencesystem_id
				p.setDouble(cnt++, row.getDouble("pos_accuracy_vertical")); // pos_accuracy_vertical
				p.setInt(cnt++, row.getInt("keyc_incl_w_dataset")); // keyc_incl_w_dataset
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

		pSqlStr = "INSERT INTO t011_obj_geo_keyc (id, obj_geo_id, line, subject_cat, key_date, edition) VALUES ( ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_keyc";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("subject_cat")); // subject_cat
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("key_date"))); // subject_cat
				p.setString(cnt++, row.get("edition")); // subject_cat
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setInt(cnt++, row.getInt("scale")); // scale
				p.setDouble(cnt++, row.getDouble("resolution_ground")); // resolution_ground
				p.setDouble(cnt++, row.getDouble("resolution_scan")); // resolution_scan
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setInt(cnt++, row.getInt("type")); // type
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setInt(cnt++, row.getInt("geometric_object_type")); // geometric_object_type
				p.setInt(cnt++, row.getInt("geometric_object_count")); // geometric_object_count
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

		pSqlStr = "INSERT INTO t011_obj_geo_symc (id, obj_geo_id, line, symbol_cat, symbol_date, edition) VALUES ( ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_symc";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("symbol_cat")); // symbol_cat
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("symbol_date"))); // symbol_date
				p.setString(cnt++, row.get("edition")); // edition
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setInt(cnt++, row.getInt("topic_category")); // topic_category
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setString(cnt++, row.get("leader")); // leader
				p.setString(cnt++, row.get("member")); // member
				p.setString(cnt++, row.get("description")); // description
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

	protected void processT015Legist() throws Exception {

		String entityName = "t015_legist";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t015_legist (id, obj_id, line, name) VALUES (?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t015_legist";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
					if (log.isDebugEnabled()) {
						log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of t01_object. Skip record.");
					}
					row.clear();
				} else  {
					int cnt = 1;
					p.setInt(cnt++, row.getInt("primary_key")); // id
					p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
					p.setInt(cnt++, row.getInt("line")); // line
					p.setString(cnt++, row.get("name")); // name
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("reference_date"))); // reference_date
				p.setInt(cnt++, row.getInt("type")); // type
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

		pSqlStr = "INSERT INTO t0110_avail_format (id, obj_id, line, name, ver, file_decompression_technique, specification) VALUES (?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t0110_avail_format";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("name")); // name
				p.setString(cnt++, row.get("version")); // ver
				p.setString(cnt++, row.get("file_decompression_technique")); // file_decompression_technique
				p.setString(cnt++, row.get("specification")); // specification
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("medium_note")); // medium_note
				p.setString(cnt++, row.get("medium_name")); // medium_name
				p.setDouble(cnt++, row.getDouble("transfer_size")); // transfer_size
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

	protected void processT017UrlRef() throws Exception {

		String entityName = "t017_url_ref";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t017_url_ref (id, obj_id, line, url_link, special_ref, special_name, content, datatype, volume, icon, icon_text, descr, url_type) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t017_url_ref";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("url_link")); // url_link
				p.setInt(cnt++, row.getInt("special_ref")); // special_ref
				p.setString(cnt++, row.get("special_name")); // special_name
				p.setString(cnt++, row.get("content")); // content
				p.setString(cnt++, row.get("datatype")); // datatype
				p.setString(cnt++, row.get("volume")); // volume
				p.setString(cnt++, row.get("icon")); // icon
				p.setString(cnt++, row.get("icon_text")); // icon_text
				p.setString(cnt++, row.get("descr")); // descr
				p.setInt(cnt++, row.getInt("url_type")); // url_type
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

	protected void processT011Township() throws Exception {

		String entityName = "t011_township";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSpatialReference = "INSERT INTO spatial_reference (id, obj_id, line, spatial_ref_id) "
				+ "VALUES (?, ?, ?, ?);";

		String pSqlStrSpatialRefValue = "INSERT INTO spatial_ref_value (id, type, spatial_ref_sns_id, name, nativekey, x1, x2, y1, y2) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";

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
				// if the spatial ref has been stored already, refere to the
				// already stored id
				if (storedNativekeys.containsKey(row.get("township_no"))) {
					pSpatialRefValueId = ((Long) storedNativekeys.get(row.get("township_no"))).longValue();
				} else {
					if (row.get("township_no") != null) {
						String topicId = IDCStrategyHelper.transformNativeKey2TopicId(row.get("township_no"));
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
					String locName = "";
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
					pSpatialRefValue.setString(cnt++, locName); // name
					pSpatialRefValue.setString(cnt++, IDCStrategyHelper.transformNativeKey2FullAgs(row.get("township_no"))); // nativekey
					pSpatialRefValue.setDouble(cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(dataProvider,
							"t01_st_bbox", "loc_town_no", row.get("township_no"), "x1")); // x1
					pSpatialRefValue.setDouble(cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(dataProvider,
							"t01_st_bbox", "loc_town_no", row.get("township_no"), "x2")); // x1
					pSpatialRefValue.setDouble(cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(dataProvider,
							"t01_st_bbox", "loc_town_no", row.get("township_no"), "y1")); // x1
					pSpatialRefValue.setDouble(cnt++, IDCStrategyHelper.getEntityFieldValueAsDouble(dataProvider,
							"t01_st_bbox", "loc_town_no", row.get("township_no"), "y2")); // x1
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
				pSpatialReference.setInt(cnt++, row.getInt("primary_key")); // id
				pSpatialReference.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
						.get("obj_id"))); // obj_id
				pSpatialReference.setInt(cnt++, row.getInt("line")); // line
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

	protected void processT019Coordinates() throws Exception {

		String entityName = "t019_coordinates";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		String pSqlStrSpatialReference = "INSERT INTO spatial_reference (id, obj_id, line, spatial_ref_id) "
				+ "VALUES (?, ?, ?, ?);";

		String pSqlStrSpatialRefValue = "INSERT INTO spatial_ref_value (id, type, spatial_ref_sns_id, name, nativekey, x1, x2, y1, y2) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement pSpatialReference = jdbc.prepareStatement(pSqlStrSpatialReference);
		PreparedStatement pSpatialRefValue = jdbc.prepareStatement(pSqlStrSpatialRefValue);

		HashMap<String, Long> storedNativekeys = new HashMap<String, Long>();

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id")) == 0) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: obj_id ('" + row.get("obj_id")
							+ "') not found in imported data of t01_object. Skip record.");
				}
				row.clear();
/*			} else if (row.get("geo_x1") == null) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: geo_x1 is null. Skip record.");
				}
				row.clear();
			} else if (row.get("geo_x2") == null) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: geo_x2 is null. Skip record.");
				}
				row.clear();
			} else if (row.get("geo_y1") == null) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: geo_y1 is null. Skip record.");
				}
				row.clear();
			} else if (row.get("geo_y2") == null) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid entry in " + entityName + " found: geo_y2 is null. Skip record.");
				}
				row.clear();
*/

			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				long pSpatialRefValueId;
				// if the spatial ref has been stored already, refere to the
				// already stored id
				// create key
				String geoKey = row.get("geo_x1") + row.get("geo_x2") + row.get("geo_y1") + row.get("geo_y1") + row.get("bezug");
				if (storedNativekeys.containsKey(geoKey)) {
					pSpatialRefValueId = ((Long) storedNativekeys.get(geoKey)).longValue();
				} else {
					// store the spatial ref
					dataProvider.setId(dataProvider.getId() + 1);

					pSpatialRefValue.setLong(cnt++, dataProvider.getId()); // id
					pSpatialRefValue.setString(cnt++, "F"); // type
					pSpatialRefValue.setNull(cnt++, Types.INTEGER); // spatial_ref_sns_id
					pSpatialRefValue.setString(cnt++, row.get("bezug")); // name
					pSpatialRefValue.setString(cnt++, ""); // nativekey
					pSpatialRefValue.setDouble(cnt++, row.getDouble("geo_x1")); // x1
					pSpatialRefValue.setDouble(cnt++, row.getDouble("geo_x2")); // x2
					pSpatialRefValue.setDouble(cnt++, row.getDouble("geo_y1")); // y1
					pSpatialRefValue.setDouble(cnt++, row.getDouble("geo_y2")); // y2
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
				pSpatialReference.setInt(cnt++, row.getInt("primary_key")); // id
				pSpatialReference.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row
						.get("obj_id"))); // obj_id
				pSpatialReference.setInt(cnt++, row.getInt("line")); // line
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
				} else if (row.get("typ").equals("0") && row.get("special_ref").equals("0") && (row.get("special_name") == null || row.get("special_name").length() == 0)) {
					if (log.isInfoEnabled()) {
						log.info("Invalid entry in " + entityName + " found: TYP ('" + row.get("typ") + "'), SPECIAL_REF ('" + row.get("special_ref") + "'), SPECIAL_NAME ('" + row.get("special_name") + "'). Skip record.");
					}
					row.clear();
				} else  {
					int cnt = 1;
					p.setInt(cnt++, row.getInt("primary_key")); // id
					p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
					p.setString(cnt++, row.get("adr_id")); // adr_uuid
					if (row.getInt("typ") == 999) {
						p.setInt(cnt++, -1); // type
					} else {
						p.setInt(cnt++, row.getInt("typ")); // type
					}
					p.setInt(cnt++, row.getInt("line")); // line
					p.setInt(cnt++, row.getInt("special_ref")); // special_ref
					p.setString(cnt++, row.get("special_name")); // special_name
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
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				// free searchterm object
				if (row.getInt("type") == 1) {
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
					}
					// thesaurus searchterm object
				} else if (row.getInt("type") == 2) {
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
						// if the term has been stored already, refere to the
						// already stored id
						if (searchTermValues.containsKey(row.get("searchterm").concat("_T"))) {
							pSearchtermValueId = ((Long) searchTermValues.get(row.get("searchterm").concat("_T")))
									.longValue();
						} else {

							// this is a thesaurus term: store the sns id in
							// table searchterm_sns
							cnt = 1;
							dataProvider.setId(dataProvider.getId() + 1);
							pSearchtermSns.setLong(cnt++, dataProvider.getId()); // id
							pSearchtermSns.setString(cnt++, "uba_thes_"
									.concat(IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid",
											"th_desc_no", row.get("th_desc_no"), "th_orig_desc_no"))); // sns_id
							pSearchtermSns.setNull(cnt++, java.sql.Types.VARCHAR); // expired_at
							try {
								pSearchtermSns.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSearchtermSns.toString(), e);
								throw e;
							}
							long pSearchtermSnsId = dataProvider.getId();

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
							searchTermValues.put(row.get("searchterm").concat("_T"), new Long(dataProvider.getId()));
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
					}
				} else if (row.getInt("type") == 3) {
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
						dataProvider.setId(dataProvider.getId() + 1);
						pSearchtermAdr.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermAdr.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id",
								row.get("obj_id"))); // obj_id
						pSearchtermAdr.setString(cnt++, row.get("line")); // term
						pSearchtermAdr.setLong(cnt++, pSearchtermValueId); // searchterm_id
						try {
							pSearchtermAdr.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSearchtermAdr.toString(), e);
							throw e;
						}
					}
				} else if (row.getInt("type") == 4) {
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
						// if the term has been stored already, refere to the
						// already stored id
						if (searchTermValues.containsKey(row.get("searchterm").concat("_T"))) {
							pSearchtermValueId = ((Long) searchTermValues.get(row.get("searchterm").concat("_T")))
									.longValue();
						} else {

							// this is a thesaurus term: store the sns id in
							// table searchterm_sns
							cnt = 1;
							dataProvider.setId(dataProvider.getId() + 1);
							pSearchtermSns.setLong(cnt++, dataProvider.getId()); // id
							pSearchtermSns.setString(cnt++, "uba_thes_"
									.concat(IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid",
											"th_desc_no", row.get("th_desc_no"), "th_orig_desc_no"))); // sns_id
							pSearchtermSns.setNull(cnt++, java.sql.Types.VARCHAR);
							try {
								pSearchtermSns.executeUpdate();
							} catch (Exception e) {
								log.error("Error executing SQL: " + pSearchtermSns.toString(), e);
								throw e;
							}
							long pSearchtermSnsId = dataProvider.getId();

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
							searchTermValues.put(row.get("searchterm").concat("_T"), new Long(dataProvider.getId()));
							pSearchtermValueId = dataProvider.getId();
						}

						// store the object -> searchterm relation
						cnt = 1;
						dataProvider.setId(dataProvider.getId() + 1);
						pSearchtermAdr.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermAdr.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t02_address", "adr_id",
								row.get("obj_id"))); // obj_id
						pSearchtermAdr.setString(cnt++, row.get("line")); // term
						pSearchtermAdr.setLong(cnt++, pSearchtermValueId); // searchterm_id
						try {
							pSearchtermAdr.executeUpdate();
						} catch (Exception e) {
							log.error("Error executing SQL: " + pSearchtermAdr.toString(), e);
							throw e;
						}
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t08_attrtyp", "attr_id", row.get("attr_id"))); // attr_type_id
				p.setString(cnt++, "Z"); // type
				p.setInt(cnt++, row.getInt("counter")); // listitem_line
				p.setString(cnt++, row.get("data")); // listitem_value
				p.setString(cnt++, "deu"); // lang_code
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("attr_name")); // attr_id
				p.setInt(cnt++, row.getInt("length")); // length
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t08_attrtyp", "attr_id", row.get("attr_id"))); // attr_id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setString(cnt++, row.get("data")); // data
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

	protected void processSysGui() throws Exception {

		String entityName = "sys_gui";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO sys_gui (id, gui_id, class_id, name, help, sample, link_to, type) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM sys_gui";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			int cnt = 1;
			p.setInt(cnt++, row.getInt("primary_key")); // id
			p.setInt(cnt++, row.getInt("gui_id")); // gui_id
			p.setInt(cnt++, row.getInt("class_id")); // class_id
			p.setString(cnt++, row.get("name")); // name
			p.setString(cnt++, row.get("help")); // help
			p.setString(cnt++, row.get("bsp")); // sample
			p.setInt(cnt++, row.getInt("link_to")); // link_to
			p.setInt(cnt++, row.getInt("typ")); // type
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
	}

	protected void processSysList() throws Exception {

		String entityName = "sys_list";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO sys_gui_list (id, gui_id, entry_id, lang_id, db_id, name, data, codelist_id, domain_id, maintainable, rowid) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM sys_gui_list";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator(entityName); i.hasNext();) {
			Row row = i.next();
			int cnt = 1;
			p.setInt(cnt++, row.getInt("primary_key")); // id
			p.setInt(cnt++, row.getInt("lst_id")); // gui_id
			p.setInt(cnt++, row.getInt("entry_id")); // entry_id
			p.setInt(cnt++, row.getInt("lang_id")); // lang_id
			p.setString(cnt++, row.get("db_id")); // db_id
			p.setString(cnt++, row.get("name")); // name
			p.setInt(cnt++, row.getInt("data")); // data
			p.setInt(cnt++, row.getInt("codelist_id")); // codelist_id
			p.setInt(cnt++, row.getInt("domain_id")); // domain_id
			p.setInt(cnt++, row.getInt("maintainable")); // maintainable
			p.setString(cnt++, row.get("rowid")); // rowid
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
	}
	
	
	protected void processT014InfoImpart() throws Exception {

		String entityName = "t014_info_impart";

		if (log.isInfoEnabled()) {
			log.info("Importing " + entityName + "...");
		}

		pSqlStr = "INSERT INTO t014_info_impart (id, obj_id, line, name) "
				+ "VALUES (?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t014_info_impart";
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
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "t01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, row.getInt("line")); // line
				p.setString(cnt++, row.get("name")); // name
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
	
	protected void setHiLoGenerator() throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + dataProvider.getId() + ")";
		jdbc.executeUpdate(sqlStr);
	}

}
