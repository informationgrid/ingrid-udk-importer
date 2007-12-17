/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
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
					log.warn("Invalid entry in T01_object found: cat_id ('" + row.get("cat_id") + "') not found in T03_catalogue.");
				}
				if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) == 0) {
					log.warn("Invalid entry in T01_object found: mod_id ('" + row.get("mod_id") + "') not found in T02_address.");
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
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"))); // mod_id,
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"))); // responsible_id
				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
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
					log.warn("Invalid entry in T02_address found: cat_id ('" + row.get("cat_id") + "') not found in T03_catalogue.");
				}
				if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) == 0) {
					log.warn("Invalid entry in T02_address found: mod_id ('" + row.get("mod_id") + "') not found in T02_address.");
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
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"))); // mod_id
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"))); // responsible_id

				try {
					p.executeUpdate();
				} catch (Exception e) {
					log.error("Error executing SQL: " + p.toString(), e);
					throw e;
				}
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

		// import all catalog data
		for (Iterator<Row> i = dataProvider.getRowIterator("T03_catalogue"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) == 0) {
					log.warn("Invalid entry in T03_catalogue found: mod_id ('" + row.get("mod_id") + "') not found in T02_address.");
				}
				int cnt = 1;
				p.setInt(cnt++, row.getInt("primary_key")); // id
				p.setString(cnt++, row.get("cat_id")); // cat_uuid
				p.setString(cnt++, row.get("catalogue")); // cat_name
				p.setString(cnt++, IDCStrategyHelper.transCountryCode(row.get("country"))); // country_code
				p.setString(cnt++, "N"); // workflow_control
				p.setInt(cnt++, 0); // expiry_duration
				p.setString(cnt++, IDCStrategyHelper.transDateTime(row.get("create_time"))); // create_time
				p.setInt(cnt++, IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id"))); // mod_id
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

	protected void processT012ObjObj() throws Exception {

		pSqlStr = "INSERT INTO t012_obj_obj (id, object_from_uuid, object_to_uuid, type, line, "
				+ "special_ref, special_name, descr) VALUES " + "( ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t012_obj_obj";
		jdbc.executeUpdate(sqlStr);

		// import all catalog data
		for (Iterator<Row> i = dataProvider.getRowIterator("T012_obj_obj"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
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

	protected void processT012ObjAdr() throws Exception {

		pSqlStr = "INSERT INTO t012_obj_adr (id, obj_id, adr_id, type, line, "
				+ "special_ref, special_name, mod_time) VALUES " + "( ?, ?, ?, ?, ?, ?, ?, ?);";

		PreparedStatement p = jdbc.prepareStatement(pSqlStr);

		sqlStr = "DELETE FROM t012_obj_adr";
		jdbc.executeUpdate(sqlStr);

		// import all catalog data
		for (Iterator<Row> i = dataProvider.getRowIterator("T012_obj_adr"); i.hasNext();) {
			Row row = i.next();
			if (row.get("obj_id") == null || row.get("obj_id").length() == 0) {
				log.warn("Invalid entry in T012_obj_adr found: obj_id not set. Skip import of record.");
			} else if (row.get("adr_id") == null || row.get("adr_id").length() == 0) {
				log.warn("Invalid entry in T012_obj_adr found: adr_id not set. Skip import of record.");
			} else if (IDCStrategyHelper.getPK(dataProvider, "T01_object", "obj_id", row.get("obj_id")) == 0) {
				log.warn("Invalid entry in T012_obj_adr found: obj_id ('" + row.get("obj_id")
						+ "') not found in T01_object. Skip import of record.");
			} else if (IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("adr_id")) == 0) {
				log.warn("Invalid entry in T012_obj_adr found: adr_id ('" + row.get("adr_id")
						+ "') not found in T02_address. Skip import of record.");
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

	protected void processT04Serachterm() throws Exception {

		String pSqlStrSearchtermObj = "INSERT INTO searchterm_obj (id, obj_id, line, searchterm_id) VALUES ( ?, ?, ?, ?);";
		String pSqlStrSearchtermAdr = "INSERT INTO searchterm_adr (id, adr_id, line, searchterm_id) VALUES ( ?, ?, ?, ?);";
		String pSqlStrSearchtermValue = "INSERT INTO searchterm_value (id, type, term, sns_hist_id) VALUES ( ?, ?, ?, ?);";

		PreparedStatement pSearchtermObj = jdbc.prepareStatement(pSqlStrSearchtermObj);
		PreparedStatement pSearchtermAdr = jdbc.prepareStatement(pSqlStrSearchtermAdr);
		PreparedStatement pSearchtermValue = jdbc.prepareStatement(pSqlStrSearchtermValue);

		ArrayList searchTermValues = new ArrayList();
		
		
		sqlStr = "DELETE FROM t012_obj_obj";
		jdbc.executeUpdate(sqlStr);

		// import all catalog data
		for (Iterator<Row> i = dataProvider.getRowIterator("T04_search"); i.hasNext();) {
			Row row = i.next();
			if (row.get("mod_type") != null && !invalidModTypes.contains(row.get("mod_type"))) {
				int cnt = 1;
				if (row.getInt("type") == 1) {
					dataProvider.setId(dataProvider.getId() + 1);
					pSearchtermValue.setLong(cnt++, dataProvider.getId()); // id
					pSearchtermValue.setString(cnt++, "F"); // 1 = F, 2 = T, 3 = F, 4 = T
					pSearchtermValue.setString(cnt++, row.get("searchterm")); // term
					pSearchtermValue.setInt(cnt++, 0); // sns_hist_id
					try {
						pSearchtermValue.executeUpdate();
					} catch (Exception e) {
						log.error("Error executing SQL: " + pSearchtermValue.toString(), e);
						throw e;
					}
					
					// pSearchtermObj.
					
				
					
					
					
				} else if (row.getInt("type") == 2) {
					
				} else if (row.getInt("type") == 3) {
					
				} else if (row.getInt("type") == 4) {
					
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
