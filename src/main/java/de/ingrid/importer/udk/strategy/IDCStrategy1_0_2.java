/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Administrator
 * 
 */
public class IDCStrategy1_0_2 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy1_0_2.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() {

		try {

			jdbc.setAutoCommit(false);

			processT02Address();
			processT03Catalogue();
			processT01Object();
			processT012ObjObj();
			processT012ObjAdr();
			
			setHiLoGenerator();

			jdbc.commit();
		} catch (Exception e) {
			log.error("Error executing SQL!", e);
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
	}

}
