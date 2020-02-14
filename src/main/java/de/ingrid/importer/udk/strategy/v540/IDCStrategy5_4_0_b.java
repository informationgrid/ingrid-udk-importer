package de.ingrid.importer.udk.strategy.v540;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class IDCStrategy5_4_0_b extends IDCStrategyDefault {
    private static final Log LOG = LogFactory.getLog(IDCStrategy5_4_0_b.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_4_0_b;

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
