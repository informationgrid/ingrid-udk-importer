/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author joachim
 * 
 */
public class IDCStrategyFactory {

	private static Log log = LogFactory.getLog(IDCStrategyFactory.class);

	private IDCStrategy getIdcStrategy(String idcVersion) throws Exception {
		if (idcVersion == null) {
			log.error("IDC version  not set in import descriptor.");
			throw new IllegalArgumentException("IDC version  not set in import descriptor.");
		} else if (idcVersion.equals("1.0.2_init")) {
			return new IDCInitDBStrategy1_0_2();
		} else if (idcVersion.equals("1.0.2_help")) {
			return new IDCHelpImporterStrategy();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_102)) {
			return new IDCStrategy1_0_2();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_103)) {
			return new IDCStrategy1_0_3();
		} else {
			log.error("Unknown IDC version '" + idcVersion + "'.");
			throw new IllegalArgumentException("Unknown IDC version '" + idcVersion + "'.");
		}
	}
	
	/**
	 * Get all strategies to execute to obtain the new version.
	 * @param oldIdcVersion "old" version of idc-catalog (set in database), PASS NULL TO START FROM SCRATCH !
	 * @param newIdcVersion version of idc-catalog to obtain
	 * @return ordered list of strategies to execute one by one (start with index 0) 
	 * @throws Exception
	 */
	public List<IDCStrategy> getIdcStrategiesToExecute(String oldIdcVersion, String newIdcVersion) throws Exception {
		ArrayList<IDCStrategy> strategiesToExecute = new ArrayList<IDCStrategy>();

		if (newIdcVersion == null) {
			String errorMsg = "\"new\" IDC version  not set in import/update descriptor.";
			log.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}

		// compare index of old and new version, obtain indices in between
		List<String> allVersions = Arrays.asList(IDCStrategy.ALL_IDC_VERSIONS);

		int oldIndex = -1;
		if (oldIdcVersion != null) {
			oldIndex = allVersions.indexOf(oldIdcVersion);
			if (oldIndex == -1) {
				String errorMsg = "INVALID \"old\" IDC version " + oldIdcVersion;
				log.error(errorMsg);
				throw new IllegalArgumentException(errorMsg);				
			}
		}

		int newIndex = allVersions.indexOf(newIdcVersion);
		if (newIndex == -1) {
			String errorMsg = "INVALID \"new\" IDC version " + newIdcVersion;
			log.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);				
		}

		if (oldIndex < newIndex) {
			for (int i = oldIndex+1; i <= newIndex; i++) {
				strategiesToExecute.add(getIdcStrategy(IDCStrategy.ALL_IDC_VERSIONS[i]));
			}
		}
		
		return strategiesToExecute;
	}
	
	public IDCStrategy getHelpImporterStrategy() {
		return new IDCHelpImporterStrategy();
	}
}
