package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * UDK HELP Text importer
 * 
 */
public class HelpImporter {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(HelpImporter.class);

	public static void main(String[] args) {

		System.out.println("DO NOT USE SEPARATE IMPORTER HERE ! BETTER USE MAIN IMPORTER WITH TARGET 1.0.2_help !!!");
/*
		System.out.println("Starting help import.");
		ImportDescriptor descriptor = null;
		try {
			System.out.print("  reading input parameter...");
			descriptor = ImportDescriptorHelper.getDescriptor(args);
			System.out.println("done.");
		} catch (IllegalArgumentException e) {
			System.out.println("Error parsing input parameters.\n\nusage: java -cp ingrid-udk-importer.jar de.ingrid.importer.udk.HelpImporter  [-u <user>] [-p <password>] -c <config file> <file/directory> [file/directory]");
			return;
		}

		System.out.print("  reading data...");
		InMemoryDataProvider data = null;
		try {
			data = new InMemoryDataProvider(descriptor);
		} catch (Exception e) {
			System.out.println("Error setting up DataProvider, see log for details !");
			log.error(e.getMessage());
			return;
		}
		System.out.println(" done.");

		// remove temp dir
		ImportDescriptorHelper.removeTempDir();

		JDBCConnectionProxy jdbc = null;
		try {
			jdbc = new JDBCConnectionProxy(descriptor);
		} catch (Exception e) {
			System.out.println("Error setting up jdbc connection, see log for details !");
			log.error(e.getMessage());
			return;
		}

		IDCStrategyFactory idcStrategyFactory = new IDCStrategyFactory();

		IDCStrategy strategy = null;
		try {
			strategy = idcStrategyFactory.getHelpImporterStrategy();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		strategy.setImportDescriptor(descriptor);
		strategy.setDataProvider(data);
		strategy.setJDBCConnectionProxy(jdbc);

		try {
			strategy.execute();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
*/
	}
}
