package de.ingrid.importer.udk.strategy;

import java.sql.PreparedStatement;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.utils.udk.UtilsCountryCodelist;

/**
 * Update Country Codelist according to current Country Codelist in INGRID-UTILS !!!
 */
public class IDCStrategy1_0_5_fixCountryCodelist extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_5_fixCountryCodelist.class);

	/**
	 * Deliver NO Version, this strategy should NOT trigger a strategy workflow and
	 * can be executed on its own ! NOTICE: BUT may be executed in workflow (part of workflow array) !
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#getIDCVersion()
	 */
	public String getIDCVersion() {
		return null;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		System.out.print("  Updating Country Codelist...");
		updateSysList();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void updateSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Updating sys_list...");
		}

		// ---------------------------------------------

		if (log.isInfoEnabled()) {
			log.info("Updating syslist " + UtilsCountryCodelist.COUNTRY_SYSLIST_ID +	" Country ...");
		}

		// clean up, to guarantee no old values !
		sqlStr = "DELETE FROM sys_list where lst_id = " + UtilsCountryCodelist.COUNTRY_SYSLIST_ID;
		jdbc.executeUpdate(sqlStr);

		pSqlStr = "INSERT INTO sys_list (id, lst_id, entry_id, lang_id, name, maintainable, is_default) " +
			"VALUES ( ?, " + UtilsCountryCodelist.COUNTRY_SYSLIST_ID + ", ?, ?, ?, 0, ?);";

		PreparedStatement pS = jdbc.prepareStatement(pSqlStr);

		Iterator<Integer> itr = UtilsCountryCodelist.countryCodelist_de.keySet().iterator();
		while (itr.hasNext()) {
			Integer key = itr.next();

			// german version
			int cnt = 1;
			pS.setLong(cnt++, getNextId()); // id
			pS.setInt(cnt++, key); // entry_id
			pS.setString(cnt++, "de"); // lang_id
			pS.setString(cnt++, UtilsCountryCodelist.countryCodelist_de.get(key)); // name
			pS.setString(cnt++, (key.equals(UtilsCountryCodelist.COUNTRY_KEY_GERMANY)) ? "Y" : "N"); // is_default
			pS.executeUpdate();

			// english version
			cnt = 1;
			pS.setLong(cnt++, getNextId()); // id
			pS.setInt(cnt++, key); // entry_id
			pS.setString(cnt++, "en"); // lang_id
			pS.setString(cnt++, UtilsCountryCodelist.countryCodelist_en.get(key)); // name
			pS.setString(cnt++, (key.equals(UtilsCountryCodelist.COUNTRY_KEY_GBR)) ? "Y" : "N"); // is_default
			pS.executeUpdate();
		}

		pS.close();

		if (log.isInfoEnabled()) {
			log.info("Updating sys_list... done");
		}
	}
}
