/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2019 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
/**
 * 
 */
package de.ingrid.importer.udk.strategy.v32;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.Rubric;
import de.ingrid.utils.ige.profile.beans.controls.Controls;

/**
 * Changes InGrid 3.2.0 first part (a):<p>
 * - remove table t0114_env_category and according syslist 1400
 * - remove env category legacyControl from profile
 */
public class IDCStrategy3_2_0_a extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_2_0_a.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_2_0_a;
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
    			log.debug("Resulting IGC Profile (removed legacyControl \"uiElementN016\" = env categories):");
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
