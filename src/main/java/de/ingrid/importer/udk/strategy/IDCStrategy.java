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
/**
 *
 */
package de.ingrid.importer.udk.strategy;

import de.ingrid.importer.udk.ImportDescriptor;
import de.ingrid.importer.udk.jdbc.JDBCConnectionProxy;
import de.ingrid.importer.udk.provider.DataProvider;

/**
 * @author Administrator
 *
 */
public interface IDCStrategy {

	/** Key for sys_generic_key table to set/extract version of idc schema. */
	static String KEY_IDC_VERSION = "IDC_VERSION";
	/** Key for sys_generic_key table to set/extract Profile of catalog (flexible datamodel). */
	static String KEY_PROFILE_XML = "profileXML";

	// ALL VERSIONS/STRATEGIES EXECUTED IN STRATEGY WORKFLOW !

	/** initial version/strategy */
	static String VALUE_IDC_VERSION_102 = "1.0.2";
	static String VALUE_STRATEGY_102_CLEAN = "1.0.2_clean";

	/** FIX strategy (writes NO version) for fixing wrong syslists. ALSO INTEGRATED IN STRATEGY WORKFLOW */
	static String VALUE_IDC_VERSION_102_FIX_SYSLIST = "1.0.2_fix_syslist_100_101";
	/** SNS Spatial Type Update */
	static String VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE = "1.0.2_sns_spatial_type";
	/** INSPIRE update version/strategy */
	static String VALUE_IDC_VERSION_103 = "1.0.3";
	/** Single Strategy for fixing tree path attribute in object/address nodes ! */
	static String VALUE_IDC_VERSION_103_FIX_TREE_PATH = "1.0.3_fix_tree_path";
	/** IMPORT/EXPORT etc. version/strategy -> InGrid 2.0 internal release ! */
	static String VALUE_IDC_VERSION_104 = "1.0.4";
	/** Update INSPIRE themes version/strategy -> can be executed on its own ! */
	static String VALUE_IDC_VERSION_104_FIX_INSPIRE_THEMES = "1.0.4_fix_inspire_themes";
	/** Official InGrid 2.0 release with fixes, addons */
	static String VALUE_IDC_VERSION_105 = "1.0.5";
	/** Update country codelist to all countries -> can be executed on its own ! */
	static String VALUE_IDC_VERSION_105_FIX_COUNTRY_CODELIST = "1.0.5_fixCountryCodelist";
	/** Extend all VARCHAR to 255 */
	static String VALUE_IDC_VERSION_106 = "1.0.6";
	/** FIX strategy (writes NO version) for fixing syslists. ALSO INTEGRATED IN STRATEGY WORKFLOW */
	static String VALUE_IDC_VERSION_106_FIX_SYSLIST_INSPIRE = "1.0.6_fixSyslistInspire";
	/** Changes for Running on Oracle */
	static String VALUE_IDC_VERSION_107 = "1.0.7";
	/** InGrid 2.3: separate object_access and object_use */
	static String VALUE_IDC_VERSION_108 = "1.0.8";
	/** InGrid 2.3: split class 3 (services) into geographic services and non geographic services */
	static String VALUE_IDC_VERSION_109 = "1.0.9";
	/** CHECK strategy (writes NO version). ALSO INTEGRATED IN STRATEGY WORKFLOW !
	 * <br>InGrid 2.3: Check INSPIRE Objects on missing data and do protocol -> can be executed on its own !
	 * <br>SWITCH OF STRATEGY VERSION ! Strategy Version (first two digits) correlates now with InGrid
	 * project version !!! */
	static String VALUE_IDC_VERSION_2_3_0_CHECK_INSPIRE_OBJECTS = "2.3.0_checkInspireObjects";
	/** InGrid 2.3: INSPIRE theme affects fields of Geoinformation/Karte.
	 * <br>SWITCH OF STRATEGY VERSION ! Strategy Version (first two digits) correlates now with InGrid
	 * project version !!! */
	static String VALUE_IDC_VERSION_2_3_0 = "2.3.0";
	/** InGrid 2.3 NI: new write_subtree permission */
    static String VALUE_IDC_VERSION_2_3_1_ADD_SUBTREE_PERMISSION = "2.3.1_addSubtreePermission";
	/** InGrid 2.3 NI: connect user with multiple groups */
	static String VALUE_IDC_VERSION_2_3_1 = "2.3.1";
	/** InGrid 2.3 NI: fix write_subtree permission ! Rename Permission to write_subnode ! */
    static String VALUE_IDC_VERSION_2_3_1_1_FIX_SUBNODE_PERMISSION = "2.3.1.1";
	/** InGrid 3.0: fix Erfassungsgrad data -> writes NO version, can be executed on its own !
	 * see https://dev.wemove.com/jira/browse/INGRID23-147 */
    static String VALUE_IDC_VERSION_3_0_0_FIX_ERFASSUNGSGRAD = "3.0.0_fixErfassungsgrad";
	/** InGrid 3.0: fix Syslist -> writes NO version, can be executed on its own !
	 * see https://dev.wemove.com/jira/browse/INGRID23-58 */
    static String VALUE_IDC_VERSION_3_0_0_FIX_SYSLIST = "3.0.0_fixSyslist";
	/** InGrid 3.0: fix selection lists allow free entries -> writes NO version, can be executed on its own !
	 * see https://dev.wemove.com/jira/browse/INGRID23-59 */
    static String VALUE_IDC_VERSION_3_0_0_FIX_FREE_ENTRY = "3.0.0_fixFreeEntry";
	/** InGrid 3.0: flexible data model */
	static String VALUE_IDC_VERSION_3_0_0 = "3.0.0";
	/** InGrid 3.0.1: new functionality (+ bugfixes 3.0) */
	static String VALUE_IDC_VERSION_3_0_1 = "3.0.1";
	/** InGrid 3.2.0: first part (a), e.g. remove environmental categories. */
	static String VALUE_IDC_VERSION_3_2_0_a = "3.2.0_a";
	/** InGrid 3.1.1: fix VARCHAR 255 to TEXT -> writes NO version, can be executed on its own !
	 * see https://dev.wemove.com/jira/browse/INGRID32-55 */
    static String VALUE_IDC_VERSION_3_2_0_FIX_VARCHAR = "3.2.0_fixVarchar";
	/** InGrid 3.2.0: migrating user addresses. */
	static String VALUE_IDC_VERSION_3_2_0_MIGRATE_USERS = "3.2.0_migrateUsers";
	/** InGrid 3.2 changes */
    static String VALUE_IDC_VERSION_3_2_0 = "3.2.0";

	/** InGrid 3.3 */
    static String VALUE_IDC_VERSION_3_3_0_a = "3.3.0-SNAPSHOT"; // was -SNAPSHOT before introducing _a _b ...
    /** Migrate 'Basisdaten (3210)' to 'Gekoppelte Ressource', see INGRID33-26
     * -> writes NO version, can be executed on its own ! */
    static String VALUE_IDC_VERSION_3_3_0_FIX_SERVICE_TO_DATA = "3.3.0_fixServiceToData";
    static String VALUE_IDC_VERSION_3_3_0_b = "3.3.0_b";
    /** -> writes NO version, can be executed on its own ! */
    static String VALUE_IDC_VERSION_3_3_0_FIX_CATALOG_NAMESPACE = "3.3.0_fixCatalogNamespace";
    static String VALUE_IDC_VERSION_3_3_0_c = "3.3.0_c";
    /** Release strategy just updating database version to 3.3.0 !!! */
    static String VALUE_IDC_VERSION_3_3_0_RELEASE = "3.3.0";

    /** InGrid 3.3.1 */
    /** -> writes NO version, can be executed on its own ! */
    static String VALUE_IDC_VERSION_3_3_1_FIX_ORIG_ID = "3.3.1_fixOrigId";
    static String VALUE_IDC_VERSION_3_3_1_a = "3.3.1_a";
    static String VALUE_IDC_VERSION_3_3_1_b = "3.3.1_b";
    static String VALUE_IDC_VERSION_3_3_1_c = "3.3.1_c";
    static String VALUE_IDC_VERSION_3_3_1_d = "3.3.1_d";
    /** Release strategy just updating database version to 3.3.1 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_3_1_RELEASE = "3.3.1";

    /** InGrid 3.3.2 */
    static String VALUE_IDC_VERSION_3_3_2_a = "3.3.2_a";
    /** Release strategy just updating database version to 3.3.2 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_3_2_RELEASE = "3.3.2";

	/** InGrid 3.4 */
    static String VALUE_IDC_VERSION_3_4_0_a = "3.4.0_a";
    static String VALUE_IDC_VERSION_3_4_0_b = "3.4.0_b";
    /** Release strategy just updating database version to 3.4.0 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_4_0_RELEASE = "3.4.0";

    /** InGrid 3.5 */
    // 3.4.1 never released ! we just keep these strategies in v341 package !
    static String VALUE_IDC_VERSION_3_4_1_a = "3.4.1_a";
    static String VALUE_IDC_VERSION_3_4_1_b = "3.4.1_b";
    /** Release strategy just updating database version to 3.5.0 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_5_0_RELEASE = "3.5.0";

    /** InGrid 3.6.1 */
    /** -> writes NO version, can be executed on its own ! */
    static String VALUE_IDC_VERSION_3_6_1_FIX_NAMESPACE_SEPARATOR = "3.6.1_fixNamespaceSeparator";
    static String VALUE_IDC_VERSION_3_6_1_FIX_SYSLIST_6100 = "3.6.1_fixSyslist6100";
    static String VALUE_IDC_VERSION_3_6_1_FIX_INSPIRE_ISO = "3.6.1_fixInspireISO";
    static String VALUE_IDC_VERSION_3_6_1_a = "3.6.1_a";
    static String VALUE_IDC_VERSION_3_6_1_b = "3.6.1_b";
    /** Release strategy just updating database version to 3.6.1 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_6_1_RELEASE = "3.6.1";
    static String VALUE_IDC_VERSION_3_6_1_1_a = "3.6.1.1_a";
    /** Release strategy just updating database version to 3.6.1.1 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_6_1_1_RELEASE = "3.6.1.1";

    /** InGrid 3.6.2 */
    static String VALUE_IDC_VERSION_3_6_2_a = "3.6.2_a";
    /** Release strategy just updating database version to 3.6.2 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_3_6_2_RELEASE = "3.6.2";
    static String VALUE_IDC_VERSION_3_6_2_FIX_CONSTRAINTS_HH = "3.6.2_fixConstraintsHH";

    /** InGrid 4.0.0 */
    static String VALUE_IDC_VERSION_4_0_0_a = "4.0.0_a";
    /** Release strategy just updating database version to 4.0.0 AND reloading syslists !!! */
    static String VALUE_IDC_VERSION_4_0_0_RELEASE = "4.0.0";

    /** InGrid 4.0.1 */
    static String VALUE_IDC_VERSION_4_0_1_b = "4.0.1_b";
    static String VALUE_IDC_VERSION_4_0_1_c = "4.0.1_c";
    static String VALUE_IDC_VERSION_4_0_1_d = "4.0.1_d";
    /** Release strategy just updating database version to 4.0.1 */
    static String VALUE_IDC_VERSION_4_0_1_RELEASE = "4.0.1";

    /** InGrid 4.0.3 */
    static String VALUE_IDC_VERSION_4_0_3_a = "4.0.3_a";
    static String VALUE_IDC_VERSION_4_0_3_b = "4.0.3_b";
    static String VALUE_IDC_VERSION_4_0_3_RELEASE = "4.0.3";
    static String VALUE_IDC_VERSION_4_0_3_fixKeywordsAdVMIS = "4.0.3_fixKeywordsAdVMIS";

    /** InGrid 4.1.0 */
    // 4.0.4 never released ! we just keep these strategies in v404 package !
    static String VALUE_IDC_VERSION_4_0_4_a = "4.0.4_a";
    static String VALUE_IDC_VERSION_4_0_4_b = "4.0.4_b";
    static String VALUE_IDC_VERSION_4_1_0_RELEASE = "4.1.0";

    /** InGrid 4.2.0 */
    static String VALUE_IDC_VERSION_4_2_0_a = "4.2.0_a";
    static String VALUE_IDC_VERSION_4_2_0_RELEASE = "4.2.0";

	/** InGrid 4.3.0 */
	static String VALUE_IDC_VERSION_4_3_0_a = "4.3.0_a";
	static String VALUE_IDC_VERSION_4_3_0_b = "4.3.0_b";
	static String VALUE_IDC_VERSION_4_3_0_RELEASE = "4.3.0";

	/** InGrid 4.3.1 */
	/** writes no version, triggers no workflow */
	static String VALUE_IDC_VERSION_4_3_1_fixSearchtermReferences = "4.3.1_fixSearchtermReferences";
	static String VALUE_IDC_VERSION_4_3_1_RELEASE = "4.3.1";

    /** InGrid 4.4.0 */
    static String VALUE_IDC_VERSION_4_4_0_a = "4.4.0_a";
    /** writes no version, BUT triggers workflow */
    static String VALUE_IDC_VERSION_4_4_0_b = "4.4.0_b";
    /** writes no version, BUT triggers workflow */
    static String VALUE_IDC_VERSION_4_4_0_c = "4.4.0_c";
    static String VALUE_IDC_VERSION_4_4_0_RELEASE = "4.4.0";

    /** InGrid 4.5.0 */
    static String VALUE_IDC_VERSION_4_5_0_a = "4.5.0_a";
    static String VALUE_IDC_VERSION_4_5_0_b = "4.5.0_b";
    static String VALUE_IDC_VERSION_4_5_0_RELEASE = "4.5.0";

    /** InGrid 4.6.0 */
    // 4.5.3 never released ! we just keep these strategies in v453 package !
    /** writes no version, BUT triggers workflow */
    static String VALUE_IDC_VERSION_4_5_3_fixISOThemes = "4.5.3_fixISOThemes";
    static String VALUE_IDC_VERSION_4_6_0_RELEASE = "4.6.0";

    /** InGrid 5.0.0 */
    // 4.7.0 never released ! we just keep these strategies in v470 package !
    /** writes no version, BUT triggers workflow */
    static String VALUE_IDC_VERSION_4_7_0_a = "4.7.0_a";
    static String VALUE_IDC_VERSION_5_0_0_RELEASE = "5.0.0";

    static String VALUE_IDC_VERSION_5_1_0_a = "5.1.0_a";
    static String VALUE_IDC_VERSION_5_1_0_RELEASE = "5.1.0";

    static String VALUE_IDC_VERSION_5_2_0_a = "5.2.0_a";
    static String VALUE_IDC_VERSION_5_2_0_RELEASE = "5.2.0";

    static String VALUE_IDC_VERSION_5_2_1_a = "5.2.1_a";
    static String VALUE_IDC_VERSION_5_2_1_b = "5.2.1_b";
    static String VALUE_IDC_VERSION_5_2_1_c = "5.2.1_c";
    static String VALUE_IDC_VERSION_5_2_1_d = "5.2.1_d";
    // 5.2.1 never released ! we just keep above strategies in v521 package and release in v530 package
    static String VALUE_IDC_VERSION_5_3_0_RELEASE = "5.3.0";
    static String VALUE_IDC_VERSION_5_3_5_a = "5.3.5_a";

    static String VALUE_IDC_VERSION_5_4_0_a = "5.4.0_a";
    static String VALUE_IDC_VERSION_5_4_0_b = "5.4.0_b";
	static String VALUE_IDC_VERSION_5_4_0_c = "5.4.0_c";
    static String VALUE_IDC_VERSION_5_4_0_d = "5.4.0_d";
    static String VALUE_IDC_VERSION_5_4_0_RELEASE = "5.4.0";

    static String VALUE_IDC_VERSION_5_6_0_a = "5.6.0_a";
    static String VALUE_IDC_VERSION_5_6_0_b = "5.6.0_b";
    static String VALUE_IDC_VERSION_5_6_0_c = "5.6.0_c";
    static String VALUE_IDC_VERSION_5_6_0_d = "5.6.0_d";

	/** Order of strategies to execute to obtain most recent IGC.
	 * Contains all according versions/strategies in ascending order.
	 * NOTICE: THESE VERSIONS ARE MAPPED TO STRATEGIES IN STRATEGY FACTORY !!! */
	static String[] STRATEGY_WORKFLOW = new String[] {
		VALUE_IDC_VERSION_102, // may be changed to VALUE_STRATEGY_102_CLEAN if no UDK data passed
		VALUE_IDC_VERSION_102_FIX_SYSLIST,
		VALUE_IDC_VERSION_102_SNS_SPATIAL_TYPE,
		VALUE_IDC_VERSION_103,

		// Syslist csv have to be imported till version 104 ! in 104 and afterwards syslists are updated via java code !

		VALUE_IDC_VERSION_104,
		VALUE_IDC_VERSION_104_FIX_INSPIRE_THEMES,
		// InGrid 2.0 Release (including post fixes, addons)
		VALUE_IDC_VERSION_105,
		// At the moment strategy VALUE_IDC_VERSION_105_FIX_COUNTRY_CODELIST is NOT part of workflow !
		// So default is only countries in europe and NOT all countries. The strategy can be executed on
		// its own if all countries needed !
//		VALUE_IDC_VERSION_105_FIX_COUNTRY_CODELIST,
		VALUE_IDC_VERSION_106,
		VALUE_IDC_VERSION_106_FIX_SYSLIST_INSPIRE, // writes no Version
		VALUE_IDC_VERSION_107,
		VALUE_IDC_VERSION_108,
		VALUE_IDC_VERSION_109,
		VALUE_IDC_VERSION_2_3_0_CHECK_INSPIRE_OBJECTS, // writes no Version !
		VALUE_IDC_VERSION_2_3_0,
        VALUE_IDC_VERSION_2_3_1_ADD_SUBTREE_PERMISSION, // writes no Version !
		VALUE_IDC_VERSION_2_3_1,
		VALUE_IDC_VERSION_2_3_1_1_FIX_SUBNODE_PERMISSION, // WRITES VERSION !!!
		VALUE_IDC_VERSION_3_0_0_FIX_ERFASSUNGSGRAD, // writes no Version
		VALUE_IDC_VERSION_3_0_0_FIX_SYSLIST, // writes no Version
		VALUE_IDC_VERSION_3_0_0_FIX_FREE_ENTRY, // writes no Version
		VALUE_IDC_VERSION_3_0_0,
		VALUE_IDC_VERSION_3_0_1,
		VALUE_IDC_VERSION_3_2_0_a,
		VALUE_IDC_VERSION_3_2_0_FIX_VARCHAR, // writes no Version
		VALUE_IDC_VERSION_3_2_0_MIGRATE_USERS,
		VALUE_IDC_VERSION_3_2_0,
		VALUE_IDC_VERSION_3_3_0_a,
		VALUE_IDC_VERSION_3_3_0_FIX_SERVICE_TO_DATA,
		VALUE_IDC_VERSION_3_3_0_b,
		VALUE_IDC_VERSION_3_3_0_FIX_CATALOG_NAMESPACE,
		VALUE_IDC_VERSION_3_3_0_c,
		VALUE_IDC_VERSION_3_3_0_RELEASE,

		VALUE_IDC_VERSION_3_3_1_FIX_ORIG_ID, // writes no Version
		VALUE_IDC_VERSION_3_3_1_a,
		VALUE_IDC_VERSION_3_3_1_b,
		VALUE_IDC_VERSION_3_3_1_c,
		VALUE_IDC_VERSION_3_3_1_d,
		VALUE_IDC_VERSION_3_3_1_RELEASE,

		VALUE_IDC_VERSION_3_3_2_a,
		VALUE_IDC_VERSION_3_3_2_RELEASE,

		VALUE_IDC_VERSION_3_4_0_a,
		VALUE_IDC_VERSION_3_4_0_b,
		VALUE_IDC_VERSION_3_4_0_RELEASE,

		VALUE_IDC_VERSION_3_4_1_a,
		VALUE_IDC_VERSION_3_4_1_b,
		VALUE_IDC_VERSION_3_5_0_RELEASE,

        VALUE_IDC_VERSION_3_6_1_FIX_NAMESPACE_SEPARATOR, // writes no Version
        VALUE_IDC_VERSION_3_6_1_FIX_SYSLIST_6100, // writes no Version
        VALUE_IDC_VERSION_3_6_1_FIX_INSPIRE_ISO, // writes no Version
        VALUE_IDC_VERSION_3_6_1_a,
        VALUE_IDC_VERSION_3_6_1_b,
        VALUE_IDC_VERSION_3_6_1_RELEASE,
        VALUE_IDC_VERSION_3_6_1_1_a,
        VALUE_IDC_VERSION_3_6_1_1_RELEASE,

        VALUE_IDC_VERSION_3_6_2_a,
        VALUE_IDC_VERSION_3_6_2_RELEASE,

        VALUE_IDC_VERSION_4_0_0_a,
        VALUE_IDC_VERSION_4_0_0_RELEASE,

        VALUE_IDC_VERSION_4_0_1_b,
        VALUE_IDC_VERSION_4_0_1_c,
        VALUE_IDC_VERSION_4_0_1_d,
        VALUE_IDC_VERSION_4_0_1_RELEASE,

        VALUE_IDC_VERSION_4_0_3_a,
        VALUE_IDC_VERSION_4_0_3_fixKeywordsAdVMIS, // writes no Version
        VALUE_IDC_VERSION_4_0_3_b,
        VALUE_IDC_VERSION_4_0_3_RELEASE,

        VALUE_IDC_VERSION_4_0_4_a,
        VALUE_IDC_VERSION_4_0_4_b,
        VALUE_IDC_VERSION_4_1_0_RELEASE,

        VALUE_IDC_VERSION_4_2_0_a,
        VALUE_IDC_VERSION_4_2_0_RELEASE,

        VALUE_IDC_VERSION_4_3_0_a,
        VALUE_IDC_VERSION_4_3_0_b,
        VALUE_IDC_VERSION_4_3_0_RELEASE,

        VALUE_IDC_VERSION_4_3_1_fixSearchtermReferences,
        VALUE_IDC_VERSION_4_3_1_RELEASE,

        VALUE_IDC_VERSION_4_4_0_a,
        VALUE_IDC_VERSION_4_4_0_b,
        VALUE_IDC_VERSION_4_4_0_c,
        VALUE_IDC_VERSION_4_4_0_RELEASE,

        VALUE_IDC_VERSION_4_5_0_a,
        VALUE_IDC_VERSION_4_5_0_b,
        VALUE_IDC_VERSION_4_5_0_RELEASE,

        VALUE_IDC_VERSION_4_5_3_fixISOThemes,
        VALUE_IDC_VERSION_4_6_0_RELEASE,

        VALUE_IDC_VERSION_4_7_0_a,
        VALUE_IDC_VERSION_5_0_0_RELEASE,

        VALUE_IDC_VERSION_5_1_0_a,
        VALUE_IDC_VERSION_5_1_0_RELEASE,

		VALUE_IDC_VERSION_5_2_0_a,
		VALUE_IDC_VERSION_5_2_0_RELEASE,

		VALUE_IDC_VERSION_5_2_1_a,
		VALUE_IDC_VERSION_5_2_1_b,
		VALUE_IDC_VERSION_5_2_1_c,
		VALUE_IDC_VERSION_5_2_1_d,
		VALUE_IDC_VERSION_5_3_0_RELEASE,

		VALUE_IDC_VERSION_5_3_5_a,
		VALUE_IDC_VERSION_5_4_0_a,
		VALUE_IDC_VERSION_5_4_0_b,
		VALUE_IDC_VERSION_5_4_0_c,
		VALUE_IDC_VERSION_5_4_0_d,
		VALUE_IDC_VERSION_5_4_0_RELEASE,

		VALUE_IDC_VERSION_5_6_0_a,
        VALUE_IDC_VERSION_5_6_0_b,
        VALUE_IDC_VERSION_5_6_0_c,
        VALUE_IDC_VERSION_5_6_0_d
	};


	public void execute() throws Exception;

	public void setDataProvider(DataProvider data);

	public void setImportDescriptor(ImportDescriptor descriptor);
	public ImportDescriptor getImportDescriptor();

	public void setJDBCConnectionProxy(JDBCConnectionProxy jdbc);

	/**
	 * Get the IDC Version generated by this strategy (written into IDC catalog).
	 * After executing the strategy, the IDC structure is according to this version (but may have
	 * different data in tables, depending from strategy executed, e.g. full initial import or "empty" IDC).
	 * NOTICE: may be null, if strategy is independent from idc version, meaning can be executed any time.
	 * If null, then NO former strategies are executed if this is the target strategy. But this strategy
	 * can also be part of strategy workflow and will then be executed when "newer" target strategy is called !
	 * @return IDC version of catalog after execution. MAY RETURN NULL IF STRATEGY IS INDEPENDENT FROM
	 * VERSION AND CAN BE EXECUTED AT ANY TIME (no tracking and execution of strategies for obtaining
	 * former versions !)
	 */
	public String getIDCVersion();
}
