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
package de.ingrid.importer.udk.strategy.v440;

import de.ingrid.codelists.model.CodeListEntry;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.InitialCodeListServiceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * <p>
 * Changes InGrid 4.4.0
 *
 * <ul>
 * <li>migrate UVP checkbox, see
 * https://redmine.informationgrid.eu/issues/881
 * </ul>
 */
public class IDCStrategy4_4_0_b extends IDCStrategyDefault {

    public static final String DEFAULT_UVP_CODELIST = "9000";
    private static Log log = LogFactory.getLog(IDCStrategy4_4_0_b.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_4_4_0_b;

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

        // do not write version of IGC structure, since migration can be done multiple times !
        // setGenericKey(KEY_IDC_VERSION, MY_VERSION);

        // THEN EXECUTE ALL "CREATING" DDL OPERATIONS ! NOTICE: causes commit
        // (e.g. on MySQL)
        // ---------------------------------

        System.out.print("  Migrate UVP data ...");
        migrateUVP();
        System.out.println("done.");

        jdbc.commit();
        System.out.println("Update finished successfully.");
    }

    private void migrateUVP() throws Exception {
        PreparedStatement psNeedsExamination = jdbc.prepareStatement(
                "SELECT data " +
                        "FROM additional_field_data " +
                        "WHERE obj_id = ? " +
                        "AND field_key = 'uvpNeedsExamination'");

        PreparedStatement psUVPCategoryItems = jdbc.prepareStatement(
                "SELECT data " +
                        "FROM additional_field_data " +
                        "WHERE field_key = 'categoryId' AND parent_field_id = (" +
                        "SELECT id FROM additional_field_data " +
                        "WHERE obj_id = ? AND field_key = 'uvpgCategory')");

        // get correct UVP codelist which is defined in the catalog?
        String uvpCodelistId = determineUvpCodelistId();
        List<CodeListEntry> uvpCodelistEntries = InitialCodeListServiceFactory.instance().getCodeList(uvpCodelistId).getEntries();

        String sql = "select id from t01_object";

        Statement st = jdbc.createStatement();
        ResultSet rs = jdbc.executeQuery(sql, st);
        while (rs.next()) {
            long objId = rs.getLong("id");

            boolean migrated = migrateByPreviousState(psNeedsExamination, objId);

            if (!migrated) {
                migrated = migrateByUvpNumber(uvpCodelistEntries, psUVPCategoryItems, objId);
            }

            // set new checkbox to "NO" if none of the above conditions apply
            if (!migrated) {
                tickCheckboxPreExaminationAccomplished(objId, false);
            }
        }

        psNeedsExamination.close();
        psUVPCategoryItems.close();
    }

    @SuppressWarnings("unchecked")
    private String determineUvpCodelistId() throws Exception {
        JSONParser jsonParser = new JSONParser();

        String behaviours = readGenericKey("BEHAVIOURS");
        if (behaviours == null) {
            return DEFAULT_UVP_CODELIST;
        }

        JSONArray json = (JSONArray) jsonParser.parse(behaviours);

        // get the defined category id or return default id (9000)
        return (String) json.stream()
                .filter(item -> "uvpPhaseField".equals(((JSONObject) item).get("id")))
                .findFirst()
                .map(uvpPhaseField -> {
                    JSONArray params = (JSONArray) ((JSONObject) uvpPhaseField).get("params");
                    return params.stream()
                            .filter(param -> "categoryCodelist".equals(((JSONObject) param).get("id")))
                            .findFirst().map(categoryCodelist -> ((JSONObject) categoryCodelist).get("value"))
                            .orElse(DEFAULT_UVP_CODELIST);
                })
                .orElse(DEFAULT_UVP_CODELIST); // default codelist for UVP
    }

    /**
     * If old checkbox "Eine Änderung/Erweiterung oder ein kumulierendes Vorhaben, für das eine Vorprüfung
     * durchgeführt wurde" was checked, then check the new one
     */
    private boolean migrateByPreviousState(PreparedStatement psNeedsExamination, long objId) throws Exception {
        psNeedsExamination.setLong(1, objId);
        ResultSet resultSet = psNeedsExamination.executeQuery();
        String valueNeedsExamination;
        boolean result = false;

        if (resultSet.next()) {
            valueNeedsExamination = resultSet.getString("data");

            if ("true".equals(valueNeedsExamination)) {
                log.debug("migrate UVP checkbox");
                tickCheckboxPreExaminationAccomplished(objId, true);
                result = true;
            }

        }

        resultSet.close();
        return result;
    }

    /**
     * If one of the UVP numbers of an element is of type "A" or "S", then check the new checkbox.
     *
     * @param uvpCodelistEntries contains the UVP codelist entries
     * @param psUVPCategoryItems is the SQL statement to query the UVP numbers of a dataset
     * @param objId              is the ID of the dataset
     * @throws Exception gets thrown if an error occurred
     */
    private boolean migrateByUvpNumber(List<CodeListEntry> uvpCodelistEntries, PreparedStatement psUVPCategoryItems, long objId) throws Exception {
        JSONParser jsonParser = new JSONParser();

        psUVPCategoryItems.setLong(1, objId);
        ResultSet resultSet = psUVPCategoryItems.executeQuery();

        // iterate over all UVP category numbers
        while (resultSet.next()) {
            String catId = resultSet.getString("data");

            // if an UVP number with classification "A" or "S" then tick the new checkbox
            for (CodeListEntry entry : uvpCodelistEntries) {
                if (entry.getId().equals(catId)) {

                    // convert data field to JsonObject
                    JSONObject json = (JSONObject) jsonParser.parse(entry.getData());
                    String type = (String) json.get("type");

                    if ("A".equals(type) || "S".equals(type)) {
                        tickCheckboxPreExaminationAccomplished(objId, true);

                        // one is enough
                        resultSet.close();
                        return true;
                    }
                }
            }
        }
        resultSet.close();
        return false;
    }

    /**
     * Set the new checkbox to checked for a dataset with the given "objId".
     *
     * @param objId is the ID of the dataset
     * @throws Exception gets thrown if an error occurred
     */
    private void tickCheckboxPreExaminationAccomplished(long objId, boolean isAccomplished) throws Exception {
        String accomplishedField = isAccomplished ? "uvpPreExaminationAccomplished" : "uvpPreExaminationNotAccomplished";
        String sqlDelete = "DELETE FROM additional_field_data WHERE obj_id = " + objId + " AND (field_key = 'uvpPreExaminationAccomplished' OR field_key = 'uvpPreExaminationNotAccomplished')";
        String sqlAdd = "INSERT INTO additional_field_data (id, obj_id, field_key, list_item_id, data) "
                + "VALUES (" + getNextId() + ", " + objId + ", '" + accomplishedField + "', NULL, 'true')";

        jdbc.executeUpdate(sqlDelete);
        jdbc.executeUpdate(sqlAdd);
    }
}
