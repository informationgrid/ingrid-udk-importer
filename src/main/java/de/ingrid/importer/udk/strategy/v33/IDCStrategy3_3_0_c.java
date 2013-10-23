package de.ingrid.importer.udk.strategy.v33;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.ingrid.importer.udk.strategy.IDCStrategyDefault;
import de.ingrid.utils.ige.profile.MdekProfileUtils;
import de.ingrid.utils.ige.profile.ProfileMapper;
import de.ingrid.utils.ige.profile.beans.ProfileBean;
import de.ingrid.utils.ige.profile.beans.controls.Controls;

/**
 * <p>
 * Changes InGrid 3.3<p>
 * <ul>
 *   <li>add JS to Profile to avoid null-Einträge bei Sachdaten/Attributinformationen, see INGRID-2220
 * </ul>
 */
public class IDCStrategy3_3_0_c extends IDCStrategyDefault {

	private static Log log = LogFactory.getLog(IDCStrategy3_3_0_c.class);

	private static final String MY_VERSION = VALUE_IDC_VERSION_3_3_0_c;

    ProfileMapper profileMapper;
	ProfileBean profileBean = null;

	public String getIDCVersion() {
		return MY_VERSION;
	}

	public void execute() throws Exception {
		jdbc.setAutoCommit(false);

		// write version of IGC structure !
		setGenericKey(KEY_IDC_VERSION, MY_VERSION);

		// PERFORM DATA MANIPULATIONS !
		// ----------------------------

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

		updateJavaScript(profileBean);

		// write Profile !
        profileXml = profileMapper.mapBeanToXmlString(profileBean);
		setGenericKey(KEY_PROFILE_XML, profileXml);        	

		log.info("Update Profile in database... done\n");
	}

	/** Manipulate JS in Controls */
	private void updateJavaScript(ProfileBean profileBean) {
		// tags for marking newly added javascript code (for later removal)
		String startTag = "\n// START 3.3.0 update\n";
		String endTag = "// 3.3.0 END\n";

		//------------- 'Sachdaten/Attributinformationen'
		log.info("'Sachdaten/Attributinformationen'(uiElement5070): add Validator to avoid null entries (Klasse 1)");
    	Controls control = MdekProfileUtils.findControl(profileBean, "uiElement5070");
		String jsCode = startTag +
"// avoid empty entries\n" +
"var validateFunction = dojo.partial(emptyRowValidation, \"ref1Data\");\n" +
"UtilGrid.getTable(\"ref1Data\").validate = validateFunction;\n" +
endTag;
		MdekProfileUtils.addToScriptedProperties(control, jsCode);
	}
}
