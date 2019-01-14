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
package de.ingrid.importer.udk.strategy.v450;

import de.ingrid.importer.udk.jdbc.DBLogic.ColumnType;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.Rubric;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 4.5.0_b
 * <p>
 * <ul>
 * <li>add new columns for storing source of a license, see https://redmine.informationgrid.eu/issues/1066</li>
 * <li>update profile and make availabilityUseConstraints optional</li>
 * </ul>
 */
public class IDCStrategy4_5_0_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy4_5_0_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_5_0_b;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        System.out.print( "  Extend datastructure..." );
        extendDataStructure();
        System.out.println( "done." );

        System.out.print( "  Update Profile..." );
        updateProfile();
        System.out.println( "done." );

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure t01_object -> CAUSES COMMIT ! ..." );

        jdbc.getDBLogic().addColumn( "source", ColumnType.TEXT, "object_use_constraint", false, null, jdbc );

        log.info( "Extending datastructure... done\n" );
    }

    private void updateProfile() throws Exception {
        log.info( "\nUpdate Profile in database..." );

        // read profile
        String profileXml = readGenericKey( KEY_PROFILE_XML );
        if (profileXml == null) {
            throw new Exception( "igcProfile not set !" );
        }
        ProfileMapper profileMapper = new ProfileMapper();
        ProfileBean profileBean = profileMapper.mapStringToBean(profileXml);

        updateRubricsAndControls(profileBean);

        // write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
        setGenericKey( KEY_PROFILE_XML, profileXml );

        log.info( "Update Profile in database... done\n" );
    }

    /**
     * Manipulate structure of rubrics / controls, NO Manipulation of JS. Also
     * removes/adds controls
     */
    private void updateRubricsAndControls(ProfileBean profileBean) {
        log.info( "Updating visibility of access constraints to 'optional'" );

        Controls control = MdekProfileUtils.findControl( profileBean, "uiElementN026" );
        control.setIsVisible("optional");
    }
}
