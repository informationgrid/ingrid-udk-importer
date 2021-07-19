/*
 * **************************************************-
 * InGrid UDK-IGC Importer (IGC Updater)
 * ==================================================
 * Copyright (C) 2014 - 2021 wemove digital solutions GmbH
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
package de.ingrid.importer.udk.strategy;

import junit.framework.TestCase;
import junit.framework.Assert;

public class IDCStrategyHelperTest extends TestCase {

	public void testTransDateTime() {
		assertEquals(IDCStrategyHelper.transDateTime("20071212153212"), "20071212153212000");
		assertEquals(IDCStrategyHelper.transDateTime("20072312153212"), null);
		String dateTime = null;
		assertEquals(IDCStrategyHelper.transDateTime(dateTime), null);
		assertEquals(IDCStrategyHelper.transDateTime(""), null);
	}
	
	public void testTransformNativeKey2TopicId() {
		assertEquals(IDCStrategyHelper.transformNativeKey2TopicId("12345678"), "GEMEINDE1234500678");
		assertEquals(IDCStrategyHelper.transformNativeKey2TopicId("12345"), "KREIS1234500000");
		assertEquals(IDCStrategyHelper.transformNativeKey2TopicId("12"), "BUNDESLAND12");
		assertEquals(IDCStrategyHelper.transformNativeKey2TopicId(null), "");
		assertEquals(IDCStrategyHelper.transformNativeKey2TopicId("123"), "");
	}
	
	public void testTransformNativeKey2FullAgs() {
		assertEquals(IDCStrategyHelper.transformNativeKey2FullAgs("12345678"), "12345678");
		assertEquals(IDCStrategyHelper.transformNativeKey2FullAgs("12345"), "12345000");
		assertEquals(IDCStrategyHelper.transformNativeKey2FullAgs("123"), "12300000");
		assertEquals(IDCStrategyHelper.transformNativeKey2FullAgs("12"), "12000000");
		assertEquals(IDCStrategyHelper.transformNativeKey2FullAgs(null), "");
		assertEquals(IDCStrategyHelper.transformNativeKey2FullAgs("1234"), "");
	}

    public void testReleaseStrategyExists() {
        // property is only set if udk importer release is in progress
        if (System.getProperty("udkReleaseVersion") != null) {
            String version = System.getProperty("udkReleaseVersion");
            String strategyFolder = "v" + version.replaceAll("\\.", ""); // e.g. v590
            String strategyName = "IDCStrategy" + version.replaceAll("\\.", "_") + "_RELEASE"; // e.g. IDCStrategy5_9_0_RELEASE
            String strategyClassname = "de.ingrid.importer.udk.strategy." + strategyFolder + "." + strategyName;
            try {
                Class.forName(strategyClassname);
            } catch (ClassNotFoundException e) {
                Assert.fail("should have a class called " + strategyClassname);
            }
        }
    }
}
