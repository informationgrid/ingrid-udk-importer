/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2023 wemove digital solutions GmbH
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

package de.ingrid.importer.udk.strategy.v401;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.Rubric;
import de.ingrid.utils.ige.profile.beans.controls.Controls;

/**
 * <p>
 * Changes InGrid 4.0.1
 * <p>
 * <ul>
 * <li>Profile: Add missing legacy fields "Vorschaugrafik" / "Dateibeschreibung", see
 * https://dev.informationgrid.eu/redmine/issues/459
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy4_0_1_c extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_0_1_c.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_0_1_c;

    String profileXml = null;
    ProfileMapper profileMapper;
    ProfileBean profileBean = null;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Update Profile in database..." );
        updateProfile();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void updateProfile() throws Exception {
        log.info( "\nUpdate Profile in database..." );

        // read profile
        String profileXml = readGenericKey( KEY_PROFILE_XML );
        if (profileXml == null) {
            throw new Exception( "igcProfile not set !" );
        }
        profileMapper = new ProfileMapper();
        profileBean = profileMapper.mapStringToBean( profileXml );

        updateRubricsAndControls( profileBean );

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString( profileBean );
        setGenericKey( KEY_PROFILE_XML, profileXml );

        log.info( "Update Profile in database... done\n" );
    }

    /**
     * Manipulate structure of rubrics / controls, NO Manipulation of JS. Also
     * removes/adds controls
     */
    private void updateRubricsAndControls(ProfileBean profileBean) {
        log.info( "Add new LEGACY control 'Allgemeines - Vorschaugrafik' after 'Kurzbezeichnung'" );
        Controls control = new Controls();
        control.setIsLegacy( true );
        control.setId( "uiElement5100" );
        control.setIsMandatory( false );
        control.setIsVisible( "optional" );
        Rubric rubric = MdekProfileUtils.findRubric( profileBean, "general" );
        int index = MdekProfileUtils.findControlIndex( profileBean, rubric, "uiElement5000" );
        MdekProfileUtils.addControl( profileBean, control, rubric, index + 1 );

        log.info( "Add new LEGACY control 'Allgemeines - Dateibeschreibung' after 'Vorschaugrafik'" );
        control = new Controls();
        control.setIsLegacy( true );
        control.setId( "uiElement5105" );
        control.setIsMandatory( false );
        control.setIsVisible( "optional" );
        MdekProfileUtils.addControl( profileBean, control, rubric, index + 2 );
    }
}
