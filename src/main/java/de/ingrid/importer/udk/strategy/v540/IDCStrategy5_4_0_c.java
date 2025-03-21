/*-
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v540;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class IDCStrategy5_4_0_c extends IDCStrategyDefault {
    private static final Log LOG = LogFactory.getLog(IDCStrategy5_4_0_c.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_4_0_c;

    @Override
    public String getIDCVersion() {
        return MY_VERSION;
    }

    @Override
    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------
        LOG.info("Removing fields for preview image and preview image description from profile...");
        updateProfile();
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
        } else {
            LOG.info("Successfully read profile from database");
        }
        ProfileMapper profileMapper = new ProfileMapper();
        ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);

        Controls control;
        // Remove control for preview image path
        control = MdekProfileUtils.removeControl(profileBean, "uiElement5100");
        if (control != null) {
            LOG.info(String.format("Removing control with id '%s' from profile", control.getId()));
        }

        // Remove control for preview image description
        control = MdekProfileUtils.removeControl(profileBean, "uiElement5105");
        if (control != null) {
            LOG.info(String.format("Removing control with id '%s' from profile", control.getId()));
        }

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
        setGenericKey(KEY_PROFILE_XML, profileXml);

        LOG.info("Update Profile in database... done\n");
    }
}
