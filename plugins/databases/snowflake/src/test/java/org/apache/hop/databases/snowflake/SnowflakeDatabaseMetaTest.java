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

package org.apache.hop.databases.snowflake;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.*;

public class SnowflakeDatabaseMetaTest {
	@ClassRule
	public static RestoreHopEnvironment env = new RestoreHopEnvironment();

	SnowflakeDatabaseMeta nativeMeta;

	@Before
	public void setupOnce() throws Exception {
		nativeMeta = new SnowflakeDatabaseMeta();
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
		assertEquals("net.snowflake.client.jdbc.SnowflakeDriver", nativeMeta.getDriverClass());

		assertEquals("jdbc:snowflake://account.snowflakecomputing.com:1500/?db=bar",
				nativeMeta.getURL("account.snowflakecomputing.com", "1500", "bar"));
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com:1500/?db=bar",
				nativeMeta.getURL("account", "1500", "bar"));
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com/?db=bar",
				nativeMeta.getURL("account", "", "bar"));
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com", nativeMeta.getURL("account", "", ""));

		nativeMeta.setWarehouse("wh");
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com:1500/?warehouse=wh&db=bar",
				nativeMeta.getURL("account.snowflakecomputing.com", "1500", "bar"));
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com:1500/?warehouse=wh&db=bar",
				nativeMeta.getURL("account", "1500", "bar"));
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com:1500/?warehouse=wh",
				nativeMeta.getURL("account", "1500", ""));
		assertEquals("jdbc:snowflake://account.snowflakecomputing.com/?warehouse=wh&db=bar",
				nativeMeta.getURL("account", "", "bar"));

		try {
			assertEquals("jdbc:snowflake://account.snowflakecomputing.com:1500/?db=bar",
					nativeMeta.getURL("", "1500", "bar"));
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException dummy) {
			// expected if host is null or empty
		}
	}
	
	@Test
	public void testSupport() throws Exception {
		assertTrue(nativeMeta.supportsSchemas());
		assertTrue(nativeMeta.supportsViews());
		assertTrue(nativeMeta.supportsSequences());
		assertTrue(nativeMeta.supportsErrorHandlingOnBatchUpdates());
		assertTrue(nativeMeta.supportsBooleanDataType());
		assertFalse(nativeMeta.supportsBitmapIndex());
		assertFalse(nativeMeta.supportsTransactions());
		assertFalse(nativeMeta.supportsSynonyms());
	}
}
