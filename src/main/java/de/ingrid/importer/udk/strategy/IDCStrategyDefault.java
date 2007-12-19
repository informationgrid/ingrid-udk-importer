/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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
		invalidModTypes.add("U");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public abstract void execute();

	protected void processT01Object() throws Exception {

		pSqlStr = "INSERT INTO t01_object (id, obj_uuid, obj_name, org_obj_id, root, class_id, "
				+ "obj_descr, cat_id, info_note, avail_access_note, loc_descr, time_from, time_to, "
				+ "time_descr, time_period, time_interval, time_status, time_alle, time_type, "
				+ "publish_id, dataset_alternate_name, dataset_character_set, dataset_usage, "
				+ "data_language_code, metadata_character_set, metadata_standard_name, "
				+ "metadata_standard_version, metadata_language_code, vertical_extent_minimum, "
				+ "vertical_extent_maximum, vertical_extent_unit, vertical_extent_vdatum, fees, "
				+ "ordering_instructions, lastexport_time, expiry_time, work_state, work_version, "
				+ "mark_deleted, create_time, mod_time, mod_id, responsible_id) "
				+ "VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t01_object";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T01_object"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {

				if (IDCStrategyHelper.getPK(dataProvider, "T03_catalogue", "cat_id", row.get("cat_id")) == 0) {
					log.warn("Invalid entry in T01_object found: cat_id ('" + row.get("cat_id")
							+ "') not found in imported data of T03_catalogue.");
				}
				if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) == 0) {
					log.warn("Invalid entry in T01_object found: mod_id ('" + row.get("mod_id")
							+ "') not found in imported data of T02_address. Trying to use create_id instead.");
					if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("create_id")) == 0) {
						log.warn("Invalid entry in T01_object found: create_id ('" + row.get("create_id")
								+ "') not found in imported data of T02_address.");
					}
				}

				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("obj_id")); // obj_uuid
				p.setString(cnt++, row.get("obj_name")); // obj_name
				p.setString(cnt++, row.get("org_id")); // org_obj_id
				p.setInt(cnt++, row.getInt("root")); // root
				p.setInt(cnt++, row.getInt("obj_class")); // class_id
				p.setString(cnt++, row.get("obj_descr")); // obj_descr
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T03_catalogue", "cat_id", row.get("cat_id"))); // cat_id
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
				int modId = IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"));
				if (modId == 0) {
					modId = IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("create_id"));
				}
				p.setInt(cnt++, modId); // mod_id,
				p.setInt(cnt++, modId); // responsible_id
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				log.info("Skip record of T01_object (obj_id='" + row.get("obj_id") + "'; mod_type='"
						+ row.get("mod_type") + "')");
				row.clear();
			}
		}
	}

	protected void processT02Address() throws Exception {

		pSqlStr = "INSERT INTO t02_address (id, adr_uuid, org_adr_id, cat_id, "
				+ "root, adr_type, institution, lastname, firstname, address, title, "
				+ "street, postcode, postbox, postbox_pc, city, country_code, job, "
				+ "descr, lastexport_time, expiry_time, work_state, work_version, "
				+ "mark_deleted, create_time, mod_time, mod_id, responsible_id) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t02_address";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T02_address"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (IDCStrategyHelper.getPK(dataProvider, "T03_catalogue", "cat_id", row.get("cat_id")) == 0) {
					log.warn("Invalid entry in T02_address found: cat_id ('" + row.get("cat_id")
							+ "') not found in imported data of T03_catalogue.");
				}
				if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) == 0) {
					log.warn("Invalid entry in T02_address found: mod_id ('" + row.get("mod_id")
							+ "') not found in imported data of T02_address. Trying to use create_id instead.");
					if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("create_id")) == 0) {
						log.warn("Invalid entry in T02_address found: create_id ('" + row.get("create_id")
								+ "') not found in imported data of T02_address.");
					}
				}
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("adr_id")); // adr_uuid
				p.setString(cnt++, row.get("org_adr_id")); // org_adr_id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T03_catalogue", "cat_id", row.get("cat_id"))); // cat_id
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
				int modId = IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"));
				if (modId == 0) {
					modId = IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("create_id"));
				}
				p.setInt(cnt++, modId); // mod_id,
				p.setInt(cnt++, modId); // responsible_id

				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			} else {
				// clear row: we do not want invalid references
				// in other entities refering to this entity
				log.info("Skip record of T02_address (adr_id='" + row.get("adr_id") + "'; mod_type='"
						+ row.get("mod_type") + "')");
				row.clear();
			}

		}
	}

	protected void processT03Catalogue() throws Exception {

		pSqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, country_code, "
				+ "workflow_control, expiry_duration, create_time, mod_id, mod_time) VALUES "
				+ "( ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t03_catalogue";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T03_catalogue"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) == 0) {
					log.warn("Invalid entry in T03_catalogue found: mod_id ('" + row.get("mod_id")
							+ "') not found in T02_address. Trying to use create_id instead.");
					if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("create_id")) == 0) {
						log.warn("Invalid entry in T03_catalogue found: create_id ('" + row.get("create_id")
								+ "') not found in imported data of T02_address.");
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
				int modId = IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"));
				if (modId == 0) {
					modId = IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("create_id"));
				}
				p.setInt(cnt++, modId); // mod_id
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
				log.info("Skip record of T03_catalogue (cat_id='" + row.get("cat_id") + "'; mod_type='"
						+ row.get("mod_type") + "')");
				row.clear();
			}
		}
	}

	protected void processT012ObjObj() throws Exception {

		pSqlStr = "INSERT INTO t012_obj_obj (id, object_from_uuid, object_to_uuid, type, line, "
				+ "special_ref, special_name, descr) VALUES " + "( ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t012_obj_obj";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T012_obj_obj"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("object_from_uuid")) == 0) {
				log.warn("Invalid entry in T012_obj_obj found: object_from_uuid ('" + row.get("object_from_uuid")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("object_to_uuid")) == 0) {
				log.warn("Invalid entry in T012_obj_obj found: object_to_uuid ('" + row.get("object_to_uuid")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("object_from_id")); // object_from_uuid
				p.setString(cnt++, row.get("object_to_id")); // object_to_uuid
				p.setInt(cnt++, row.getInt("typ")); // type
				p.setInt(cnt++, row.getInt("line")); // line
				p.setInt(cnt++, row.getInt("special_ref")); // special_ref
				p.setString(cnt++, row.get("special_name")); // special_name
				p.setString(cnt++, row.get("descr")); // descr
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
	}

	protected void processT022AdrAdr() throws Exception {

		pSqlStr = "INSERT INTO t022_adr_adr (id, adr_from_uuid, adr_to_uuid) VALUES ( ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t022_adr_adr";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T022_adr_adr"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("adr_from_id")) == 0) {
				log.warn("Invalid entry in T022_adr_adr found: adr_from_id ('" + row.get("adr_from_id")
						+ "') not found in imported data of T02_address.");
			} else if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("adr_to_id")) == 0) {
				log.warn("Invalid entry in T022_adr_adr found: adr_to_id ('" + row.get("adr_to_id")
						+ "') not found in imported data of T02_address.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("adr_from_id")); // adr_from_uuid
				p.setString(cnt++, row.get("adr_to_id")); // adr_to_uuid
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
			}
		}
	}

	protected void processT011ObjLiteratur() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_literature (id, obj_id, author, publisher, type, publish_in, "
				+ "volume, sides, publish_year, publish_loc, loc, doc_info, base, isbn, publishing, "
				+ "description) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_literature";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_literatur"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_literatur found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjData() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_data (id, obj_id, base, description) VALUES ( ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_data";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_data"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_data found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjDataParam() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_data_para (id, obj_id, line, parameter, unit) VALUES ( ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_data_para";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_data_para"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_data_para found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjGeo() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo (id, obj_id, special_base, data_base, method, coord, rec_exact, rec_grade, hierarchy_level, "
				+ "vector_topology_level, referencesystem_id, pos_accuracy_vertical, keyc_incl_w_dataset) "
				+ "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id"))); // obj_id
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
				log.info("Skip record of T011_obj_geo (obj_id='" + row.get("obj_id") + "'; mod_type='"
						+ row.get("mod_type") + "')");
				row.clear();
			}
		}
	}

	protected void processT011ObjGeoKeyc() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_keyc (id, obj_geo_id, line, subject_cat, key_date, edition) VALUES ( ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_keyc";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_keyc"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_keyc found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}	
	
	protected void processT011ObjGeoScale() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_scale (id, obj_geo_id, line, scale, resolution_ground, resolution_scan) VALUES ( ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_scale";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_scale"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_scale found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}		
	
	protected void processT011ObjGeoSpatialRep() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_spatial_rep (id, obj_geo_id, line, type) VALUES ( ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_spatial_rep";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_spatial_rep"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_spatial_rep found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}	
	
	protected void processT011ObjGeoSupplInfo() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_supplinfo (id, obj_geo_id, line, feature_type) VALUES ( ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_supplinfo";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_supplinfo"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_supplinfo found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjGeoVector() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_vector (id, obj_geo_id, line, geometric_object_type, geometric_object_count) VALUES ( ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_vector";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_vector"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_vector found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjGeoSymc() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_symc (id, obj_geo_id, line, symbol_cat, symbol_date, edition) VALUES ( ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_symc";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_symc"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_symc found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjGeoTopicCat() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_geo_topic_cat (id, obj_geo_id, line, topic_category) VALUES ( ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_geo_topic_cat";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_geo_topic_cat"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_geo_topic_cat found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T011_obj_geo. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T011_obj_geo", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT011ObjProject() throws Exception {

		pSqlStr = "INSERT INTO t011_obj_project (id, obj_id, leader, member, description) VALUES ( ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t011_obj_project";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T011_obj_project"); i.hasNext();) {
			Row row = i.next();
			if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T011_obj_project found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id"))); // obj_id
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
	}

	protected void processT012ObjAdr() throws Exception {

		pSqlStr = "INSERT INTO t012_obj_adr (id, obj_id, adr_id, type, line, "
				+ "special_ref, special_name, mod_time) VALUES " + "( ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t012_obj_adr";
		jdbc.executeUpdate(sqlStr);

		for (Iterator<Row> i = dataProvider.getRowIterator("T012_obj_adr"); i.hasNext();) {
			Row row = i.next();
			if (row.get("obj_id") == null || row.get("obj_id").length() == 0) {
				log.warn("Invalid entry in T012_obj_adr found: obj_id not set. Skip record.");
			} else if (row.get("adr_id") == null || row.get("adr_id").length() == 0) {
				log.warn("Invalid entry in T012_obj_adr found: adr_id not set. Skip record.");
			} else if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T012_obj_adr found: obj_id ('" + row.get("obj_id")
						+ "') not found in imported data of T01_object. Skip record.");
			} else if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("adr_id")) == 0) {
				log.warn("Invalid entry in T012_obj_adr found: adr_id ('" + row.get("adr_id")
						+ "') not found in imported data of T02_address. Skip record.");
			} else if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id"))); // obj_id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("adr_id"))); // adr_id
				p.setInt(cnt++, row.getInt("typ")); // type
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

	protected void processT04Search() throws Exception {

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

		for (Iterator<Row> i = dataProvider.getRowIterator("T04_search"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				// free searchterm object
				if (row.getInt("type") == 1) {
					// check for invalid record
					if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
						log.warn("Invalid entry in T04_search found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of T01_object. Skip record.");
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
							pSearchtermValue.setInt(cnt++, 0); // searchterm_sns_id
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
						pSearchtermObj.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row
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
					if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
						log.warn("Invalid entry in T04_search found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of T01_object. Skip record.");
					} else if (IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid", "th_desc_no",
							row.get("th_desc_no"), "th_orig_desc_no").length() == 0) {
						log.warn("Invalid entry in T04_search found: th_desc_no ('" + row.get("th_desc_no")
								+ "') not found in thesorigid. Skip record.");
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
						pSearchtermObj.setLong(cnt++, dataProvider.getId()); // id
						pSearchtermObj.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row
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
					if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("obj_id")) == 0) {
						log.warn("Invalid entry in T04_search found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of T02_address. Skip record.");
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
							pSearchtermValue.setInt(cnt++, 0); // searchterm_sns_id
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
						pSearchtermAdr.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id",
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
					if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("obj_id")) == 0) {
						log.warn("Invalid entry in T04_search found: obj_id ('" + row.get("obj_id")
								+ "') not found in imported data of T02_address. Skip record.");
					} else if (IDCStrategyHelper.getEntityFieldValue(dataProvider, "thesorigid", "th_desc_no",
							row.get("th_desc_no"), "th_orig_desc_no").length() == 0) {
						log.warn("Invalid entry in T04_search found: th_desc_no ('" + row.get("th_desc_no")
								+ "') not found in thesorigid. Skip record.");
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
						pSearchtermAdr.setLong(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id",
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
	}

	protected void setHiLoGenerator() throws SQLException {
		sqlStr = "DELETE FROM hibernate_unique_key";
		jdbc.executeUpdate(sqlStr);

		sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + dataProvider.getId() + ")";
		jdbc.executeUpdate(sqlStr);
	}

}
