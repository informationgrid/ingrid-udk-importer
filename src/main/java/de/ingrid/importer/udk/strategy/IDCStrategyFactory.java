/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author joachim
 * 
 */
public class IDCStrategyFactory {

	private static Log log = LogFactory.getLog(IDCStrategyFactory.class);

	public IDCStrategy getIdcStrategy(String idcVersion) throws Exception {
		if (idcVersion == null) {
			log.error("IDC version  not set in import descriptor.");
			throw new IllegalArgumentException("IDC version  not set in import descriptor.");
		} else if (idcVersion.equals("1.0.2")) {
			return new IDCStrategy1_0_2();
		} else if (idcVersion.equals("1.0.2_init")) {
			return new IDCInitDBStrategy1_0_2();
		} else if (idcVersion.equals("1.0.2_help")) {
			return new IDCHelpImporterStrategy();
		} else {
			log.error("Unknown IDC version '" + idcVersion + "'.");
			throw new IllegalArgumentException("Unknown IDC version '" + idcVersion + "'.");
		}
	}
	
	public IDCStrategy getHelpImporterStrategy() {
		return new IDCHelpImporterStrategy();
	}
}
