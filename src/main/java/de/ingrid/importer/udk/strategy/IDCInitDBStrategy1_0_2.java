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
public class IDCInitDBStrategy1_0_2 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCInitDBStrategy1_0_2.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.ingrid.importer.udk.strategy.IDCStrategy#execute()
	 */
	public void execute() {

		try {

			jdbc.setAutoCommit(false);

			System.out.print("  Importing sys_list...");
			// must be processed first because other methods depend on that data
			processSysList();
			System.out.println("done.");
			
			System.out.print("  Importing default address/permission for admin...");
			importDefaultUserdata();
			System.out.println("done.");
			jdbc.commit();
			
			jdbc.setAutoCommit(false);
			System.out.print("  Set HI/LO table...");
			setHiLoGenerator();
			System.out.println("done.");
			jdbc.commit();
			System.out.println("Import finished successfully.");

		} catch (Exception e) {
			System.out.println("Error executing sql! See log file for further information.");
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
