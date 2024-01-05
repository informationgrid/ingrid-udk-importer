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
/**
 * 
 */
package de.ingrid.importer.udk.strategy.v590;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * <p>
 * Changes InGrid 5.9.0_a
 * <p>
 * <ul>
 * <li>Increase Varchar size for obj_name field in t01_object
 * </ul>
 */
public class IDCStrategy5_9_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_9_0_a.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_9_0_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        try {
            log.info( "Updating obj_name field in t01_object ..." );
            jdbc.getDBLogic().modifyColumn("obj_name", DBLogic.ColumnType.VARCHAR4096, "t01_object", false, jdbc);
        } catch (Exception ex) {
            log.warn("Problems updating obj_name field in t01_object: ", ex);
        }
        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }
}
