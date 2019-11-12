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

import de.ingrid.codelists.model.CodeList;
import de.ingrid.codelists.model.CodeListEntry;
import de.ingrid.importer.udk.jdbc.DBLogic;
import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.importer.udk.util.InitialCodeListServiceFactory;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.Rubric;
import de.ingrid.utils.ige.profile.beans.controls.Controls;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Changes InGrid 5.2.1_b
 * <p>
 * Add date field for application schema
 */
public class IDCStrategy5_2_1_d extends IDCStrategyDefault {

    private static Log log = LogFactory.getLog(IDCStrategy5_2_1_d.class);

    private static final String MY_VERSION = VALUE_IDC_VERSION_5_2_1_d;

    CodeList codeList6300 = InitialCodeListServiceFactory.instance().getCodeList(Integer.toString(6300));

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

        log.info("Add new field 'date' ...");
        addDateColumn();

        log.info("Migrate data ...");
        migrateData();

        log.info("Update Profile ...");
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

    private void migrateData() throws Exception {

        handleGMLEntry();
        handleApplicationSchemaDate();

    }

    private void handleGMLEntry() throws Exception {

        PreparedStatement psClasses1AndInspire = jdbc
                .prepareStatement("SELECT * FROM t01_object WHERE obj_class='1' AND is_inspire_relevant='Y'");
        PreparedStatement psHasGMLEntry = jdbc
                .prepareStatement("SELECT * FROM t0110_avail_format WHERE obj_id=? AND format_key=14");
        PreparedStatement psAddGMLEntry = jdbc
                .prepareStatement("INSERT INTO t0110_avail_format (id, version, obj_id, line, format_value, format_key, ver, file_decompression_technique, specification) VALUES (?, 0, ?, 1, 'GML', 14, '3.2', null, null)");
        PreparedStatement psUpdateGMLVersion = jdbc
                .prepareStatement("UPDATE t0110_avail_format SET ver='3.2' WHERE obj_id=? AND format_key=14");

        // check if GML with version exists for INSPIRE relevant datasets
        ResultSet inspireResult = psClasses1AndInspire.executeQuery();
        while (inspireResult.next()) {
            long objId = inspireResult.getLong("id");
            psHasGMLEntry.setLong(1, objId);
            ResultSet gmlEntries = psHasGMLEntry.executeQuery();
            boolean hasGML = false;
            while (gmlEntries.next()) {
                hasGML = true;
                String version = gmlEntries.getString("ver");
                if (version == null || version.isEmpty()) {
                    // update version
                    log.info("Add version to existing GML entry for objId: " + objId);
                    psUpdateGMLVersion.setLong(1, objId);
                    psUpdateGMLVersion.executeUpdate();
                }
            }
            if (!hasGML) {
                log.info("Add new GML entry for objId: " + objId);
                psAddGMLEntry.setLong(1, getNextId());
                psAddGMLEntry.setLong(2, objId);
                psAddGMLEntry.execute();
            }
        }
        psClasses1AndInspire.close();
        psHasGMLEntry.close();
        psAddGMLEntry.close();
        psUpdateGMLVersion.close();

    }

    private void handleApplicationSchemaDate() throws SQLException {

        PreparedStatement psFormatInspire = jdbc.prepareStatement("SELECT * FROM object_format_inspire");
        PreparedStatement psUpdateFormatInspire = jdbc.prepareStatement("UPDATE object_format_inspire SET date = ? WHERE obj_id = ?");

        ResultSet resultSet = psFormatInspire.executeQuery();
        while (resultSet.next()) {
            int format_key = resultSet.getInt("format_key");
            Date date = resultSet.getDate("date");
            if (format_key != -1 && date == null) {
                long objId = resultSet.getLong("obj_id");
                psUpdateFormatInspire.setString(1, mapFormatKeyToDate(String.valueOf(format_key)));
                psUpdateFormatInspire.setLong(2, objId);
                psUpdateFormatInspire.executeUpdate();
            }
        }

        psFormatInspire.close();
        psUpdateFormatInspire.close();

    }

    private String mapFormatKeyToDate(String key) {
        for (CodeListEntry entry : codeList6300.getEntries()) {
            if (entry.getId().equalsIgnoreCase(key)) {
                return entry.getData().split("\",\"")[2];
            }
        }
        return null;
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
