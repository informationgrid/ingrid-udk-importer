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
		} else if (idcVersion.equals("1.0.2_fix_import")) {
			return new IDCFixImportStrategy();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_102_FIX_SYSLIST)) {
			return new IDCFixSysList100_101Strategy();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE)) {
			return new IDCSNSSpatialTypeStrategy();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_103)) {
			return new IDCStrategy1_0_3();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_103_FIX_TREE_PATH)) {
			return new IDCFixTreePathStrategy();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_104)) {
			return new IDCStrategy1_0_4();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_104_FIX_INSPIRE_THEMES)) {
			return new IDCStrategy1_0_4_fixInspireThemes();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_105)) {
			return new IDCStrategy1_0_5();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_105_FIX_COUNTRY_CODELIST)) {
			return new IDCStrategy1_0_5_fixCountryCodelist();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_106)) {
			return new IDCStrategy1_0_6();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_106_FIX_SYSLIST_INSPIRE)) {
			return new IDCStrategy1_0_6_fixSysListInspire();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_107)) {
			return new IDCStrategy1_0_7();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_108)) {
			return new IDCStrategy1_0_8();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_109)) {
			return new IDCStrategy1_0_9();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_2_3_0_CHECK_INSPIRE_OBJECTS)) {
			return new IDCStrategy2_3_0_checkInspireObjects();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_2_3_0)) {
			return new IDCStrategy2_3_0();
        } else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_2_3_1_ADD_SUBTREE_PERMISSION)) {
            return new IDCStrategy2_3_1_add_subtree_permission();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_2_3_1)) {
			return new IDCStrategy2_3_1();
        } else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_2_3_1_1_FIX_SUBNODE_PERMISSION)) {
            return new IDCStrategy2_3_1_1_fix_subnode_permission();
        } else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_0_0_FIX_ERFASSUNGSGRAD)) {
            return new IDCStrategy3_0_0_fixErfassungsgrad();
        } else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_0_0_FIX_SYSLIST)) {
            return new IDCStrategy3_0_0_fixSyslist();
        } else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_0_0_FIX_FREE_ENTRY)) {
            return new IDCStrategy3_0_0_fixFreeEntry();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_0_0)) {
			return new IDCStrategy3_0_0();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_0_1)) {
			return new IDCStrategy3_0_1();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_2_0_a)) {
			return new IDCStrategy3_2_0_a();
		} else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_2_0_FIX_VARCHAR)) {
			return new IDCStrategy3_2_0_fixVarchar();
        } else if (idcVersion.equals(IDCStrategy.VALUE_IDC_VERSION_3_2_0)) {
            return new IDCStrategy3_2_0();
		} else {
			log.error("Unknown IDC version '" + idcVersion + "'.");
			throw new IllegalArgumentException("Unknown IDC version '" + idcVersion + "'.");
		}
	}
	
	/**
	 * Get all strategies to execute to obtain the new version.
	 * @param oldIdcVersion "old" version of idc-catalog (set in database), PASS NULL TO START FROM SCRATCH !
	 * @param newIdcVersionFromDescriptor requested new version of idc-catalog passed via properties or descriptor file (specifies strategy !)
	 * @return ordered list of strategies to execute one by one (start with index 0) 
	 * @throws Exception
	 */
	public List<IDCStrategy> getIdcStrategiesToExecute(String oldIdcVersion, String newIdcVersionFromDescriptor) throws Exception {
		ArrayList<IDCStrategy> strategiesToExecute = new ArrayList<IDCStrategy>();

		if (newIdcVersionFromDescriptor == null) {
			String errorMsg = "\"new\" IDC version  not set in import/update descriptor.";
			log.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}

		// get REAL version from requested strategy
		// NOTICE: may be null, if strategy is independent from idc version, meaning can be executed any time
		// -> we don't assure executing of former strategies !
		IDCStrategy newStrategy = getIdcStrategy(newIdcVersionFromDescriptor);
		String newIdcVersion = newStrategy.getIDCVersion();
		
		if (newIdcVersion == null) {
			// version independent, simply execute this strategy !
			strategiesToExecute.add(newStrategy);

		} else {
			// new strategy generates a new version !
			// execute strategies for versions in between and finally requested new strategy. 

			// compare index of old and new version, obtain indices in between
			List<String> allVersions = Arrays.asList(IDCStrategy.STRATEGY_WORKFLOW);

			// old version is null if initial state of idc (no version set yet)
			int oldIndex = -1;
			if (oldIdcVersion != null) {
				oldIndex = allVersions.indexOf(oldIdcVersion);
				if (oldIndex == -1) {
					String errorMsg = "INVALID \"old\" IDC version " + oldIdcVersion;
					log.error(errorMsg);
					throw new IllegalArgumentException(errorMsg);				
				}
			}

			// new version has to exist in ordered version array
			int newIndex = allVersions.indexOf(newIdcVersion);
			if (newIndex == -1) {
				String errorMsg = "INVALID \"new\" IDC version " + newIdcVersion + " (was extracted from requested strategy)";
				log.error(errorMsg);
				throw new IllegalArgumentException(errorMsg);				
			}

			// set up default strategies for obtaining versions in between. Newest strategy is requested strategy.
			if (oldIndex < newIndex) {
				for (int i = oldIndex+1; i <= newIndex; i++) {
					if (i == newIndex) {
						strategiesToExecute.add(newStrategy);
					} else {
						// add default strategy for that version !
						strategiesToExecute.add(getIdcStrategy(IDCStrategy.STRATEGY_WORKFLOW[i]));
					}
				}
			}
		}
		
		return strategiesToExecute;
	}
	
	public IDCStrategy getHelpImporterStrategy() {
		return new IDCHelpImporterStrategy();
	}
}
