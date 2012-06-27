package de.ingrid.importer.udk;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.jdbc.JDBCHelper;
import de.ingrid.importer.udk.provider.DataProvider;
import de.ingrid.importer.udk.provider.LazyInMemoryDataProvider;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyFactory;

/**
 * UDK Data Importer UDK5.0 -> IDC.
 * 
 */
public class Importer {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(Importer.class);

	public static void main(String[] args) {

		System.out.println("Starting import.");
		ImportDescriptor descriptor = null;
		try {
			System.out.print("  reading input parameter...");
			descriptor = ImportDescriptorHelper.getDescriptor(args);
			System.out.println("done.");
		} catch (IllegalArgumentException e) {
			System.out.println("Error parsing input parameters.\n\nusage: java -Xmx1024M -jar <ingrid-udk-importer.jar> -c <config file> " +
				"[<udk data file/directory> <udk data file/directory> ...] " +
				"[-v <igc version>] [-l <igc language>] [-email <igc email>] [-partner <igc partner name>] [-provider <igc provider name>] " +
				"[-name <igc catalog name>] [-country <igc country code>] " +
				"[-u <db user>] [-p <db password>] [-dburl <db url>] [-dbdriver <db driver>]");
			return;
		}

		System.out.print("  initializing data provider...");
		DataProvider data = new LazyInMemoryDataProvider(descriptor);
		System.out.println(" done.");

		JDBCConnectionProxy jdbc = null;
		try {
			jdbc = new JDBCConnectionProxy(descriptor);
			jdbc.setAutoCommit(false);
		} catch (Exception e) {
			log.error(e.getMessage());
			System.out.println("Problems establishing JDBC connection, see log for details.");
			return;
		}
		
		// fetch current IDC Version ("old" version)
		String oldIDCVersion;
		try {
			oldIDCVersion = JDBCHelper.getCurrentIDCVersion(jdbc, true);
		} catch (Exception e) {
			log.error(e.getMessage());
			System.out.println("Problems determining current IDC Version, see log for details.");
			return;
		}
		String msg = "Current version of IDC: " + oldIDCVersion + 
			", requested version of IDC from passed \"properties\": " + descriptor.getIdcVersion();
		System.out.println("\ninfo: " + msg);
		log.info(msg);

		List<IDCStrategy> strategiesToExecute = new ArrayList<IDCStrategy>();
		try {
			IDCStrategyFactory idcStrategyFactory = new IDCStrategyFactory();
			strategiesToExecute = idcStrategyFactory.getIdcStrategiesToExecute(oldIDCVersion, descriptor);
		} catch (Exception e) {
			log.error("Problems extracting strategies to execute !", e);
			System.out.println("\nProblems determining strategies to execute, see log for details.");
		}

		boolean errorOccured = false;
		for (IDCStrategy strategy : strategiesToExecute) {

			String targetVersionInfo = "";
			if (strategy.getIDCVersion() != null) {
				targetVersionInfo = " (target IDC Version: " + strategy.getIDCVersion() + ")";
			}
			msg = "\n\nExecuting strategy " + strategy + targetVersionInfo;
			System.out.println(msg);
			log.info(msg);

			boolean executed = false;
			try {
				strategy.setImportDescriptor(descriptor);
				strategy.setDataProvider(data);
				strategy.setJDBCConnectionProxy(jdbc);
				
				strategy.execute();
				// we assure a commit for every strategy !
				jdbc.commit();

				executed = true;

			} catch (Exception e) {
				log.error("Error executing strategy " + strategy + targetVersionInfo, e);
				try {
					jdbc.rollback();
				} catch (SQLException e1) {
					log.error("Error rolling back transaction!", e);
				}

			} finally {
				// remove temp dir after every strategy ? ok, to avoid conflicts with existing data in tmp.
				ImportDescriptorHelper.removeTempDir();

				// close connection if problems !
				if (!executed) {
					try {
						jdbc.close();
					} catch (SQLException e) {
						log.error("Error closing DB connection!", e);
					}
				}					
			}
			
			if (!executed) {
				System.out.println("\nError executing strategy " + strategy + targetVersionInfo + ", see log for details");
				errorOccured = true;
				// STOP !!!
				break;
			}
		}
		
		if (!errorOccured) {
			System.out.println("\nImporter/Updater executed succesfully.");
		}
	}
}
