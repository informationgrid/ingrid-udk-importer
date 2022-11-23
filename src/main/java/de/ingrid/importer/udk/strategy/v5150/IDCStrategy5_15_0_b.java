/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2022 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy.v5150;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.15.0_b
 * <p>
 * <ul>
 * <li>Migrate adv-product group (#1535)</li>
 * </ul>
 */
public class IDCStrategy5_15_0_b extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog( IDCStrategy5_15_0_b.class );

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_15_0_b;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit( false );

        // write version of IGC structure !
        setGenericKey( KEY_IDC_VERSION, MY_VERSION );

        // delete time stamp of last update of syslists to reload all syslists
        // (reload from initial codelist file from codelist service if no repo connected).
        // Thus we guarantee syslists are up to date !
        deleteGenericKey( "lastModifiedSyslist" );

        migrateData();

        jdbc.commit();
        System.out.println( "Update finished successfully." );
    }

    private void migrateData() {
        try {
            PreparedStatement pstmt = jdbc.prepareStatement( "UPDATE adv_product_group SET product_key='28', product_value='Fachübergreifende Anzeigesysteme' WHERE product_key=7" );
            pstmt.execute();
            log.info( "Migrated dataset successfully" );
        } catch (Exception ex) {
            log.warn( "Problems migrating fields in adv_product_group: ", ex );
        }
    }
}

