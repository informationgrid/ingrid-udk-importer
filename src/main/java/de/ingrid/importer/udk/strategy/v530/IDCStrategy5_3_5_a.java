/*
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
package de.ingrid.importer.udk.strategy.v530;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;

/**
 * <p>
 * InGrid 5.3.5_a<p>
 * <ul>
 *   <li>Show some data quality fields via profile</li>
 * </ul>
 */
public class IDCStrategy5_3_5_a extends IDCStrategyDefault {

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_3_5_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // delete time stamp of last update of syslists to reload all syslists
        // (reload from initial codelist file from codelist service if no repo connected).
        // Thus we guarantee syslists are up to date !
        deleteGenericKey("lastModifiedSyslist");

        System.out.print("Update profile ...");
        updateProfile();

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void updateProfile() throws Exception {
        // read profile
        String profileXml = readGenericKey(KEY_PROFILE_XML);
        if (profileXml == null) {
            throw new Exception("igcProfile not set !");
        }
        ProfileMapper profileMapper = new ProfileMapper();
        ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);

        Controls altAccuracy = MdekProfileUtils.findControl(profileBean, "uiElement5069");
        altAccuracy.setIsVisible("show");
        Controls posAccuracy = MdekProfileUtils.findControl(profileBean, "uiElement3530");
        posAccuracy.setIsVisible("show");

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
        setGenericKey(KEY_PROFILE_XML, profileXml);
    }
}
