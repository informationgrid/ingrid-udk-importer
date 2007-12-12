package de.ingrid.importer.udk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.InMemoryDataProvider;
import de.ingrid.importer.udk.strategy.IDCStrategy;
import de.ingrid.importer.udk.strategy.IDCStrategyFactory;

/**
 * Hello world!
 * 
 */
public class Importer {

	/**
	 * The logging object
	 */
	private static Log log = LogFactory.getLog(Importer.class);

	public static void main(String[] args) {

		ImportDescriptor descriptor = null;
		try {
			descriptor = ImportDescriptorHelper.getDescriptor(args);
		} catch (IllegalArgumentException e) {
			return;
		}

		InMemoryDataProvider data = new InMemoryDataProvider(descriptor);

		JDBCConnectionProxy jdbc = null;
		try {
			jdbc = new JDBCConnectionProxy(descriptor);
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		IDCStrategyFactory idcStrategyFactory = new IDCStrategyFactory();

		IDCStrategy strategy = null;
		try {
			strategy = idcStrategyFactory.getIdcStrategy(descriptor.getIdcVersion());
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
	}
}
