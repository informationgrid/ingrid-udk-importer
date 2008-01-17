package de.ingrid.importer.udk.strategy;

import junit.framework.TestCase;

public class IDCStrategyHelperTest extends TestCase {

	public void testTransDateTime() {
		assertEquals(IDCStrategyHelper.transDateTime("20071212153212"), "20071212153212000");
		assertEquals(IDCStrategyHelper.transDateTime("20072312153212"), "");
		assertEquals(IDCStrategyHelper.transDateTime(null), "");
		assertEquals(IDCStrategyHelper.transDateTime(""), "");
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
}
