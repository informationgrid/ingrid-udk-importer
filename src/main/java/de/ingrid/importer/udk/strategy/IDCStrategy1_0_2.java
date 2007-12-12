/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_2 implements IDCStrategy {

	private DataProvider dataProvider = null;

	private ImportDescriptor importDescriptor = null;

	private JDBCConnectionProxy jdbc = null;

	private static Log log = LogFactory.getLog(JDBCConnectionProxy.class);

	public void setDataProvider(DataProvider data) {
		dataProvider = data;
	}

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc) {
		this.jdbc = jdbc;
	}

	public void setImportDescriptor(ImportDescriptor descriptor) {
		importDescriptor = descriptor;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() {

		String sqlStr = null;
		try {

			jdbc.setAutoCommit(false);

			// import all address data
			for (Iterator<HashMap<String, String>> i = dataProvider.getRowIterator("T02_address"); i.hasNext();) {
				HashMap<String, String> row = i.next();

				sqlStr = "DELETE FROM t02_address";
				jdbc.executeUpdate(sqlStr);

				sqlStr = "INSERT INTO t02_address (id, version, adr_uuid, org_adr_id, cat_id, "
						+ "root, adr_type, institution, lastname, firstname, address, title, "
						+ "street, postcode, postbox, postbox_pc, city, country_code, job, "
						+ "descr, lastexport_time, expiry_time, work_state, work_version, "
						+ "mark_deleted, create_time, mod_time, mod_id, responsible_id) VALUES " + "("
						+ row.get("primary_key") + ", " + // id
						"0, " + // version
						"'" + row.get("adr_id") + "', " + // adr_uuid
						"'" + row.get("org_adr_id") + "', " + // org_adr_id
						IDCStrategyHelper.getPK(dataProvider, "T03_catalogue", "cat_id", row.get("cat_id")) + ", " + // cat_id
						row.get("root") + ", " + // root
						row.get("typ") + ", " + // adr_type
						"'" + row.get("institution") + "', " + // institution
						"'" + row.get("lastname") + "', " + // lastname
						"'" + row.get("firstname") + "', " + // firstname
						"'" + row.get("address") + "', " + // address
						"'" + row.get("title") + "', " + // title
						"'" + row.get("street") + "', " + // street
						"'" + row.get("postcode") + "', " + // postcode
						"'" + row.get("postbox") + "', " + // postbox
						"'" + row.get("postbox_pc") + "', " + // postbox_pc
						"'" + row.get("city") + "', " + // city
						"'" + IDCStrategyHelper.transCountryCode(row.get("state_id")) + "', " + // country_code
						"'" + row.get("job") + "', " + // job
						"'" + row.get("descr") + "', " + // descr
						"'', " + // lastexport_time
						"'', " + // expiry_time
						"'V', " + // work_state
						"0 , " + // work_version
						"0 , " + // mark_deleted
						"'" + IDCStrategyHelper.transDateTime(row.get("create_time")) + "', " + // create_time
						"'" + IDCStrategyHelper.transDateTime(row.get("mod_time")) + "', " + // mod_time
						IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) + ", " + // mod_id
						IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) + // responsible_id
						")";
				jdbc.executeUpdate(sqlStr);

			}

			// import all catalog data
			for (Iterator<HashMap<String, String>> i = dataProvider.getRowIterator("T03_catalogue"); i.hasNext();) {
				HashMap<String, String> row = i.next();

				sqlStr = "DELETE FROM t03_catalogue";
				jdbc.executeUpdate(sqlStr);

				sqlStr = "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, country_code, "
						+ "workflow_control, expiry_duration, create_time, mod_id, mod_time) VALUES " + "("
						+ row.get("primary_key") + ", " + // id
						"'" + row.get("cat_id") + "', " + // cat_uuid
						"'" + row.get("catalogue") + "', " + // cat_name
						"'" + IDCStrategyHelper.transCountryCode(row.get("country")) + "', " + // country_code
						"'N', " + // workflow_control
						"0, " + // expiry_duration
						"'" + IDCStrategyHelper.transDateTime(row.get("create_time")) + "', " + // create_time
						IDCStrategyHelper.getPK(dataProvider, "T02_address", "adr_id", row.get("mod_id")) + ", " + // mod_id
						"'" + IDCStrategyHelper.transDateTime(row.get("mod_time")) + "'" + // mod_time
						")";
				jdbc.executeUpdate(sqlStr);
			}

			sqlStr = "DELETE FROM hibernate_unique_key";
			jdbc.executeUpdate(sqlStr);

			sqlStr = "INSERT INTO hibernate_unique_key (next_hi) VALUES (" + dataProvider.getId() + ")";
			jdbc.executeUpdate(sqlStr);

			jdbc.commit();
		} catch (Exception e) {
			log.error("Error executing SQL: " + sqlStr, e);
			if (jdbc != null) {
				try {
					jdbc.rollback();
				} catch (SQLException e1) {
					log.error("Error rolling back transaction!", e);
				}
			}
		} finally {
			if (jdbc != null) {
				try {
					jdbc.close();
				} catch (SQLException e) {
					log.error("Error closing DB connection!", e);
				}
			}
		}

		/*
		 * // remove all entries in T03_catalogue for (Iterator<HashMap<String,
		 * String>> i = dataProvider.getRowIterator("T03_catalogue");
		 * i.hasNext(); ) { HashMap<String,String> row = i.next(); sqlStr =
		 * "INSERT INTO t03_catalogue (id, cat_uuid, cat_name, country_code,
		 * workflow_control, expiry_duration, create_time, mod_id, mod_time) " +
		 * "VALUES (" + row.get("primary_key") +", '" + row.get("cat_id")+ "')"; }
		 */
	}

}
