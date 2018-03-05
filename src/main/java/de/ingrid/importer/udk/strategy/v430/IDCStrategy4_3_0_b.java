/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v430;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.Rubric;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 3.4.0<p>
 * <ul>
 *   <li>Profile: Fix JS in "Verschlagwortung - INSPIRE Themen", adapt dependent fields now via call of "applyRuleThesaurusInspire()", see INGRID-2302
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy4_3_0_b extends IDCStrategyDefault {

	private static final Log LOG = LogFactory.getLog(IDCStrategy4_3_0_b.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_4_3_0_b;

	private static final String INSPIRE_THEME_ELEMENT = "uiElement5064";
	private static final String OPTIONAL_KEYWORDS_ELEMENT = "uiElement1409";

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		LOG.info("Update Profile in database...");
		updateProfile();
		deleteKeinInspireThemaSearchTermValue();
		LOG.info("done.");

		jdbc.commit();
		LOG.info("Update finished successfully.");
	}

	private void updateProfile() throws Exception {
        // read profile
		String profileXml = readGenericKey(KEY_PROFILE_XML);
		if (profileXml == null) {
		    LOG.warn("No profile found in database! Exiting.");
		    return;
		}
        ProfileMapper profileMapper = new ProfileMapper();
		ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);

		updateJavaScript(profileBean);
		updateControl(profileBean, INSPIRE_THEME_ELEMENT, false, "optional");
		updateControl(profileBean, OPTIONAL_KEYWORDS_ELEMENT, false, "show");

		// write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
		setGenericKey(KEY_PROFILE_XML, profileXml);        	

		LOG.info("Update Profile in database... done\n");
	}

	private void updateJavaScript(ProfileBean profileBean) {
		LOG.info("Don't make 'INSPIRE-Themen' a required input. For INSPIRE-relevant metadatesets, this input will be made required in JS behaviours.");

		Controls control = MdekProfileUtils.findControl(profileBean, INSPIRE_THEME_ELEMENT);

		String previousStartTag = "// START 3.4.0_a update";
		String previousEndTag = "// 3.4.0_a END";
		String script = getUpdateJavaScript();

		// Replace the JavaScript code and log success or failure
		if (MdekProfileUtils.replaceInScriptedProperties(control, previousStartTag, previousEndTag, script)) {
		    String msg = String.format(
		    		"'INSPIRE-Themen' (%s): Updated props to ->%n%s%n",
					INSPIRE_THEME_ELEMENT,
					control.getScriptedProperties());
			LOG.debug(msg);
		} else {
			String msg = String.format(
					"'INSPIRE-Themen' (%s): Failed to update props. Start/end tags not found. Current props -> ->%n%s%n",
					INSPIRE_THEME_ELEMENT,
					control.getScriptedProperties());
			LOG.warn(msg);
		}
	}

	private String getUpdateJavaScript() {
		String startTag = "// Start 4.3.0_b update\n";
		String endTag = "// 4.3.0_b end\n\n";

		StringBuilder sb = new StringBuilder();
		sb.append(startTag                                                                                          ).append('\n');
		sb.append("// Start UDK Strategy 4.3.0_b"                                                                   ).append('\n');
		sb.append("// This snippet is implemented in igcProfile, not in behaviours"                                 ).append('\n');
		sb.append("// It can be found in UDK Strategy 4.3.0_b"                                                      ).append('\n');
		sb.append(                                                                                                  '\n');
		sb.append("applyRule7();"                                                                                   ).append('\n');
		sb.append(                                                                                                  '\n');
		sb.append("function uiElement5064InputHandler() {"                                                          ).append('\n');
		sb.append("    var objClass = dijit.byId('objectClass').getValue();"                                        ).append('\n');
		sb.append("    if (objClass == 'Class1') {"                                                                 ).append('\n');
		sb.append("        // Show/hide DQ tables in class 1 depending on themes"                                   ).append('\n');
		sb.append("        applyRule7();"                                                                           ).append('\n');
		sb.append("    }"                                                                                           ).append('\n');
		sb.append('}'                                                                                               ).append('\n');
		sb.append(                                                                                                  '\n');
		sb.append("dojo.connect(UtilGrid.getTable('thesaurusInspire'), 'onDataChanged', uiElement5064InputHandler);").append('\n');
		sb.append(                                                                                                  '\n');
		sb.append("// End UDK Strategy 4.3.0_b"                                                                     ).append('\n').append('\n');
		sb.append(endTag                                                                                            );

		return sb.toString();
	}

	private void updateControl (
			ProfileBean profileBean,
			String uiElement,
			boolean isMandatory,
			String visibility) {
		LOG.info(String.format("Updating properties for element: %s", uiElement));

		Controls control = new Controls();
		control.setIsLegacy(true);
		control.setId(uiElement);
		control.setIsMandatory(isMandatory);
		control.setIsVisible(visibility);
		Rubric rubric = MdekProfileUtils.findRubric( profileBean, "thesaurus" );
		int index = MdekProfileUtils.findControlIndex( profileBean, rubric, uiElement );
		MdekProfileUtils.removeControl(profileBean, uiElement);
		MdekProfileUtils.addControl(profileBean, control, rubric, index);
		LOG.info(String.format("Updated properties for element: %s", uiElement));
	}

	private void deleteKeinInspireThemaSearchTermValue() throws SQLException {
		LOG.info("Deleting 'Kein INSPIRE-Thema' entry from searchterm_value table.");
		String sql = "DELETE FROM searchterm_value WHERE type = 'I' AND entry_id = 99999";
		int count = jdbc.executeUpdate(sql);
		if (count > 0) {
			LOG.info(String.format("%d entries deleted from searchterm_value table.", count));
		} else {
			LOG.info("No deletable entries found in searchterm_value table.");
		}
	}
}
