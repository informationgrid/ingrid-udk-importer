/**
 * 
 */
package de.ingrid.importer.udk.strategy.v34;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;

/**
 * <p>
 * Changes InGrid 3.4.0<p>
 * <ul>
 *   <li>Profile: Fix JS in "Verschlagwortung - INSPIRE Themen", set combo box via entry id, see INGRID-2302
 * </ul>
 * Writes NEW Catalog Schema Version to catalog !
 */
public class IDCStrategy3_4_0_a extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_4_0_a.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_4_0_a;

	String profileXml = null;
    ProfileMapper profileMapper;
	ProfileBean profileBean = null;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// THEN PERFORM DATA MANIPULATIONS !
		// ---------------------------------

		System.out.print("  Update Profile in database...");
		updateProfile();
		System.out.println("done.");

		jdbc.commit();
		System.out.println("Update finished successfully.");
	}

	private void updateProfile() throws Exception {
		log.info("\nUpdate Profile in database...");

        // read profile
		String profileXml = readGenericKey(KEY_PROFILE_XML);
		if (profileXml == null) {
			throw new Exception("igcProfile not set !");
		}
        profileMapper = new ProfileMapper();
		profileBean = profileMapper.mapStringToBean(profileXml);			

//		updateRubricsAndControls(profileBean);

		updateJavaScript(profileBean);

		// write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
		setGenericKey(KEY_PROFILE_XML, profileXml);        	

		log.info("Update Profile in database... done\n");
	}

	/** Manipulate JS in Controls */
	private void updateJavaScript(ProfileBean profileBean) {
		// tags for marking newly added javascript code (for later removal)
		String startTag = "\n// START 3.4.0_a update\n";
		String endTag = "// 3.4.0_a END\n";

		//------------- 'INSPIRE-Themen'
		log.info("'INSPIRE-Themen'(uiElement5064): " +
			"FIX 3.2.0 JS IN PROFILE: setComboBySyslistValue via ID and not via text !");
		Controls control = MdekProfileUtils.findControl(profileBean, "uiElement5064");
    	String jsCode = startTag +
"// On change of object class:\n" +
"// Make 'INSPIRE-Themen'(uiElement5064) optional, mandatory or hide it dependent from new class.\n" +
"\n" +
"dojo.subscribe(\"/onObjectClassChange\", function(c) {\n" +
"if (c.objClass == \"Class3\" || c.objClass == \"Class5\" || c.objClass == \"Class6\") {\n" +
"  // optional in 'Geodatendienst' + 'Datensammlung/Datenbank' + 'Informationssystem/Dienst/Anwendung'\n" +
"  UtilUI.setOptional(\"uiElement5064\");\n" +
"} else if (c.objClass == \"Class1\") {\n" +
"  // mandatory in class 'Geo-Information/Karte'\n" +
"  UtilUI.setMandatory(\"uiElement5064\");\n" +
"} else {\n" +
"  UtilUI.setHide(\"uiElement5064\");\n" +
"}});\n" +
"\n" +
"// Class 1: On input 'INSPIRE-Themen'(uiElement5064 / thesaurusInspire)\n" +
"// - adapt content of 'Kodierungsschema der geographischen Daten'(uiElement1315 / availabilityDataFormatInspire)\n" +
"// - show/hide DQ tables dependent from themes\n" +
"\n" +
"// initial show/hide of DQ tables dependent from themes\n" +
"applyRule7();\n" +
"\n" +
"// Function for adapting 'Kodierungsschema der geographischen Daten' to passed 'INSPIRE-Themen'\n" +
"function uiElement1315AdaptToThemes(themes) {\n" +
"  // Set 'Kodierungsschema' as text (from syslist value) because of ComboBox(!).\n" +
"\n" +
"  // Set default here. May be changed below.\n" +
"  UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"1\");\n" +
"\n" +
"  // Geographical names (103) -> Geographical names GML Application Schema (18)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 103); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"18\");\n" +
"  }\n" +
"  // Administrative units (104) -> Administrative units GML application schema (16)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 104); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"16\");\n" +
"  }\n" +
"  // Addresses (105) -> Addresses GML application schema (15)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 105); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"15\");\n" +
"  }\n" +
"  // Cadastral parcels (106) -> Cadastral Parcels GML Application Schema (17)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 106); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"17\");\n" +
"  }\n" +
"  // Transport networks (107) -> Common Transport Elements GML Application Schema (7)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 107); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"7\");\n" +
"  }\n" +
"  // Hydrography (108) -> Hydrography GML application schema (2)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 108); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"2\");\n" +
"  }\n" +
"  // Protected sites (109) -> Protected Sites - Full GML Application Schema (14)\n" +
"  if (dojo.some(themes, function(themeKey) {return (themeKey == 109); })) {\n" +
"    UtilUI.setComboBySyslistValue(\"availabilityDataFormatInspire\", \"14\");\n" +
"  }\n" +
"}\n" +
"// Input Handler for 'INSPIRE-Themen' called when changed\n" +
"function uiElement5064InputHandler() {\n" +
"  var objClass = dijit.byId(\"objectClass\").getValue();\n" +
"  if (objClass == \"Class1\") {\n" +
"    // Get INSPIRE themes\n" +
"    var themes = UtilList.tableDataToList(UtilGrid.getTableData(\"thesaurusInspire\"));\n" +
"\n" +
"    // Adapt 'Kodierungsschema' to 'INSPIRE-Themen'\n" +
"// !!! UNCOMMENT the following line if 'Kodierungsschema' should be adapted to 'INSPIRE-Themen' !!!\n" +
"//    uiElement1315AdaptToThemes(themes);\n" +
"\n" +
"    //  Show/hide DQ tables in class 1 dependent from themes\n" +
"    applyRule7();\n" +
"  }\n" +
"}\n" +
"dojo.connect(UtilGrid.getTable(\"thesaurusInspire\"), \"onDataChanged\", uiElement5064InputHandler);\n"
+ endTag;
		// -------------------------------------
		// !!! REPLACE FORMER 3.2.0 stuff with fixed stuff !
		// -------------------------------------
    	boolean propsFixed =
    		MdekProfileUtils.replaceInScriptedProperties(control, "// START 3.2.0 update", "// 3.2.0 END", jsCode);
		if (propsFixed) {
			log.info("'INSPIRE-Themen'(uiElement5064): Fixed props to ->\n'" + control.getScriptedProperties() + "'");
		} else {
			log.warn("'INSPIRE-Themen'(uiElement5064): " +
				"NO fixing of props, start/end tags not found, keep props ->\n'" + control.getScriptedProperties() + "'");			
		}
	}
}
