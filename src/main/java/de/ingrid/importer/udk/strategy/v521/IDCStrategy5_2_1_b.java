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
package de.ingrid.importer.udk.strategy.v521;

import de.ingrid.importer.udk.jdbc.DBLogic;
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
 * Changes InGrid 5.2.1_b
 * <p>
 * Add date field for application schema
 */
public class IDCStrategy5_2_1_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_2_1_b.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_2_1_b;

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

        System.out.print("Add new field 'date' ...");

        // addDateColumn();

        // read profile
        String profileXml = readGenericKey(KEY_PROFILE_XML);
        if (profileXml == null) {
            throw new Exception("igcProfile not set !");
        }
        ProfileMapper profileMapper = new ProfileMapper();
        ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);
//        ProfileBean profileBean = MdekProfileUtils.getProfileBean();

        addFieldToProfile(profileBean);
        makeFieldOptional(profileBean);

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
        setGenericKey(KEY_PROFILE_XML, profileXml);

        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void addFieldToProfile(ProfileBean profileBean) {
        Rubric rubric = MdekProfileUtils.findRubric(profileBean, "availability");
        Integer uiElement1315Index = MdekProfileUtils.findControlIndex(profileBean, rubric, "uiElement1315");

        Controls control = new Controls();
        control.setIsLegacy(true);
        control.setId("uiElement1316");
        control.setIsMandatory(false);
        control.setIsVisible("optional");

        MdekProfileUtils.addControl(profileBean, control, rubric, uiElement1315Index + 1);
    }

    private void makeFieldOptional(ProfileBean profileBean) {


        Controls control = MdekProfileUtils.findControl(profileBean, "uiElement1315");
        control.setIsMandatory(false);

    }

    private void addDateColumn() throws SQLException {

        jdbc.getDBLogic().addColumn("date", DBLogic.ColumnType.DATE, "object_format_inspire", false, null, jdbc);

    }
}
