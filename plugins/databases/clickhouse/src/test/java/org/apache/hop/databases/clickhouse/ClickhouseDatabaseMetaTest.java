/*! *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.databases.clickhouse;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClickhouseDatabaseMetaTest {
	@ClassRule
	public static RestoreHopEnvironment env = new RestoreHopEnvironment();

	ClickhouseDatabaseMeta nativeMeta;

	@Before
	public void setupOnce() throws Exception {
		nativeMeta = new ClickhouseDatabaseMeta();
		nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);

		HopClientEnvironment.init();
	}

	@Test
	public void testAccessType() throws Exception {
		int[] aTypes = new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
		assertArrayEquals(aTypes, nativeMeta.getAccessTypeList());
	}

	
	@Test
	public void testUrl() throws Exception {
		assertEquals("cc.blynk.clickhouse.ClickHouseDriver", nativeMeta.getDriverClass());

		assertEquals("jdbc:clickhouse://localhost:8123/sampledb",
				nativeMeta.getURL("localhost", "8123", "sampledb"));
		try {
			assertEquals("jdbc:clickhouse://localhost:8123/sampledb",
					nativeMeta.getURL("", "8123", "sampledb"));
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException dummy) {
			// expected if host is null or empty
		}
	}
	
	@Test
	public void testSupport() throws Exception {
		assertFalse(nativeMeta.supportsSchemas());
		assertTrue(nativeMeta.supportsViews());
		assertFalse(nativeMeta.supportsSequences());
		assertTrue(nativeMeta.supportsErrorHandlingOnBatchUpdates());
		assertFalse(nativeMeta.supportsBooleanDataType());
		assertTrue(nativeMeta.supportsBitmapIndex());
		assertFalse(nativeMeta.supportsTransactions());
		assertFalse(nativeMeta.supportsTimeStampToDateConversion());
		assertTrue(nativeMeta.supportsSynonyms());
	}
}
