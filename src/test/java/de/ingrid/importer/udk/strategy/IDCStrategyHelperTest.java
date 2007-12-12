package de.ingrid.importer.udk.strategy;

import junit.framework.TestCase;

public class IDCStrategyHelperTest extends TestCase {

	public void testTransDateTime() {
		assertEquals(IDCStrategyHelper.transDateTime("20071212153212"), "20071212153212000");
		assertEquals(IDCStrategyHelper.transDateTime("20071312153212"), "");
		assertEquals(IDCStrategyHelper.transDateTime(null), "");
		assertEquals(IDCStrategyHelper.transDateTime(""), "");
	}

}
