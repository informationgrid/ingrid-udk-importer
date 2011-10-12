/**
 * 
 */
package de.ingrid.importer.udk.strategy;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.mdek.beans.ProfileBean;
import de.ingrid.mdek.beans.Rubric;
import de.ingrid.mdek.beans.controls.Controls;
import de.ingrid.mdek.profile.ProfileMapper;

/**
 * Changes InGrid 3.1.1:<p>
 * - remove table t0114_env_category and according syslist 1400
 * - remove env category legacyControl from profile
 */
public class IDCStrategy3_1_1 extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_1_1.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_1_1;
	private static final String ID_RUBRIC_ENV_CATEGORIES = "thesaurus";
	private static final String ID_CONTROL_ENV_CATEGORIES = "uiElementN016";
	
	String profileXml = null;
    ProfileMapper profileMapper;
	ProfileBean profileBean = null;
    Rubric additionalFieldRubric = null;;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Clean up sys_list...");
		cleanUpSysList();
		System.out.println("done.");

		System.out.print("  Update Profile in database...");
		updateProfile();
		System.out.println("done.");


		// FINALLY EXECUTE ALL "DROPPING" DDL OPERATIONS ! These ones may cause commit (e.g. on MySQL)
		// ---------------------------------

		System.out.print("  Clean up datastructure...");
		cleanUpDataStructure();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void cleanUpSysList() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list...");
		}
		
		int numDeleted;
		if (log.isInfoEnabled()) {
			log.info("Delete syslist 1400 (environmental categories)...");
		}

		sqlStr = "DELETE FROM sys_list where lst_id = 1400";
		numDeleted = jdbc.executeUpdate(sqlStr);
		if (log.isDebugEnabled()) {
			log.debug("Deleted " + numDeleted +	" entries (all languages).");
		}
		
		if (log.isInfoEnabled()) {
			log.info("Clean up sys_list... done");
		}
	}

	private void updateProfile() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Update Profile in database...");
		}
		
        // read profile
		String profileXml = readGenericKey(KEY_PROFILE_XML);
		if (profileXml == null) {
			throw new Exception("igcProfile not set !");
		}
        profileMapper = new ProfileMapper();
		profileBean = profileMapper.mapStringToBean(profileXml);			

		// remove env category control from profile (in according rubric)
		boolean removed = false;
        for (Rubric rubric : profileBean.getRubrics()) {
        	if (!ID_RUBRIC_ENV_CATEGORIES.equals(rubric.getId())) {
        		continue;
        	}

        	List<Controls> rubricControls = rubric.getControls();
            for (Controls control : rubricControls) {
            	if (ID_CONTROL_ENV_CATEGORIES.equals(control.getId()) &&
            			control.getType().equals(Controls.LEGACY_CONTROL)) {
            		removed = rubricControls.remove(control);
            		break;
            	}
            }
            
            if (removed) {
    			log.info("Removed legacyControl for \"env-categories\" from Profile");
            	break;
            }
        } 

        if (!removed) {
        	String msg = "Problems removing legacyControl for \"env-categories\" from Profile !";
			log.warn(msg);
			System.out.println(msg);
        } else {
    		// write Profile !
            profileXml = profileMapper.mapBeanToXmlString(profileBean);
    		if (log.isDebugEnabled()) {
    			log.debug("Resulting IGC Profile (removed legacyControl \"uiElementN016\" = env categories):" + profileXml);
    		}
    		setGenericKey(KEY_PROFILE_XML, profileXml);        	
        }

		if (log.isInfoEnabled()) {
			log.info("Update Profile in database... done");
		}
	}

	private void cleanUpDataStructure() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure -> CAUSES COMMIT ! ...");
		}

		if (log.isInfoEnabled()) {
			log.info("Drop table 't0114_env_category' ...");
		}
		jdbc.getDBLogic().dropTable("t0114_env_category", jdbc);

		if (log.isInfoEnabled()) {
			log.info("Cleaning up datastructure... done");
		}
	}
}
