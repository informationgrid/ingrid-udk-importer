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
package de.ingrid.importer.udk.strategy.v5150;

import de.ingrid.importer.udk.jdbc.PostgreSQLLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Changes InGrid 5.15.0_a
 * <p>
 * <ul>
 * <li>Adapt t012_obj_adr table
 * </ul>
 */
public class IDCStrategy5_15_0_a extends IDCStrategyDefault {

    private static final Log log = LogFactory.getLog(IDCStrategy5_15_0_a.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_15_0_a;

    public String getIDCVersion() {
        return MY_VERSION;
    }

    public void execute() throws Exception {
        jdbc.setAutoCommit(false);

        // write version of IGC structure !
        setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        try {
            log.info("Updating obj_name field in t01_object ...");
            String key = "obj_id";
            if (jdbc.getDBLogic() instanceof PostgreSQLLogic) {
                key = "t012_obj_adr_obj_id_adr_uuid_type_key";
            }
            jdbc.getDBLogic().dropKey("t012_obj_adr", key, jdbc);
            jdbc.getDBLogic().addKey("t012_obj_adr", key, "obj_id, adr_uuid, type, special_name", jdbc);
        } catch (Exception ex) {
            log.warn("Problems updating obj_name field in t01_object: ", ex);
        }
        jdbc.commit();
        System.out.println("Update finished successfully.");
    }
}
