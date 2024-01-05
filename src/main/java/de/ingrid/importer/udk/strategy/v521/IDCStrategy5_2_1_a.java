/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
package de.ingrid.importer.udk.strategy.v521;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 5.2.1_a
 * <p>
 * Remove all javascript behaviours from profile, since they were migrated to separate files.
 */
public class IDCStrategy5_2_1_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_2_1_a.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_2_1_a;

    public String getIDCVersion() {
        // Returning version here enables strategy workflow !
        // So all former versions in IDCStrategy.STRATEGY_WORKFLOW are executed !
        // Returning null disables version tracking ...
        // Well we keep version here having a special strategy:
        // - no version written to catalog
        // - but all former versions in workflow are executed, if catalog version is below this one !
        // - return null here if you want to execute this one on its own without strategy workflow (can be changed later on when higher strategy added !)
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // NOTICE:
        // This is a "fix strategy" writing no version !

        // do write version of IGC structure, since migration shall only be run once!
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print("Remove Javascript behaviour ...");
        // read profile
        String profileXml = readGenericKey(KEY_PROFILE_XML);
        if (profileXml == null) {
            throw new Exception("igcProfile not set !");
        }
        ProfileMapper profileMapper = new ProfileMapper();
        ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);

        removeJavascriptBehaviour(profileBean);

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
        setGenericKey(KEY_PROFILE_XML, profileXml);

        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void removeJavascriptBehaviour(ProfileBean profileBean) {

        String[] fields = {"uiElement1000", "uiElement5064", "uiElement5060", "uiElement6000", "uiElementN014", "uiElementN015", "uiElement5062", "uiElement3555", "uiElement3535", "uiElement5070", "uiElement3565", "uiElement7509", "uiElement3221", "uiElementN004", "uiElement3345", "uiElement3110", "uiElement3670", "uiElementN006", "uiElementN008","uiElementN010", "uiElement5030", "uiElementN011", "uiElement1230", "uiElement5042", "uiElement5043", "uiElementN024", "uiElementN025"};

        for (String field : fields) {
            Controls control = MdekProfileUtils.findControl(profileBean, field);
            MdekProfileUtils.removeAllScriptedProperties(control);
        }
    }
}
