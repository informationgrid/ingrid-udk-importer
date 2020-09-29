/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2020 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v560;

import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.strategy.v521.IDCStrategy5_2_1_c;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.5.0_a
 * <p>
 * Add explanation column to object_conformity table
 */
public class IDCStrategy5_6_0_a extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_6_0_a.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_6_0_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN PERFORM DATA MANIPULATIONS !
        // ---------------------------------

        System.out.print( "  Add column explanation ..." );
        extendDataStructure();
        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void extendDataStructure() throws Exception {
        log.info( "\nExtending datastructure -> CAUSES COMMIT ! ..." );

        log.info( "Add column 'explanation'..." );
        jdbc.getDBLogic().addColumn("explanation", DBLogic.ColumnType.VARCHAR1024, "object_conformity", false, null, jdbc);

        log.info( "Extending datastructure... done\n" );
    }
}
