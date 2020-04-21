/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
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

package org.apache.hop.databases.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class MySQLValueMetaBaseTest {
	protected static final String TEST_NAME = "TEST_NAME";
	protected static final String LOG_FIELD = "LOG_FIELD";

	@ClassRule
	public static RestoreHopEnvironment env = new RestoreHopEnvironment();
;
	private PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
	private ResultSet resultSet;
	private DatabaseMeta databaseMeta;
	private IValueMeta valueMetaBase;

	@BeforeClass
	public static void setUpBeforeClass() throws HopException {
		PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
		PluginRegistry.addPluginType(DatabasePluginType.getInstance());
		PluginRegistry.init();
		// HopLogStore.init();
	}
	
	@Before
	public void setUp() throws HopPluginException {
		valueMetaBase = ValueMetaFactory.createValueMeta( IValueMeta.TYPE_NONE);
		databaseMeta = spy(new DatabaseMeta());
		resultSet = mock(ResultSet.class);
	}

	protected void initValueMeta(BaseDatabaseMeta dbMeta, int length, Object data) throws HopDatabaseException {
		IValueMeta valueMetaString = new ValueMetaString(LOG_FIELD, length, 0);
		databaseMeta.setIDatabase(dbMeta);
		valueMetaString.setPreparedStatementValue(databaseMeta, preparedStatementMock, 0, data);
	}

	@Test
	public void test_Pdi_17126_mysql() throws Exception {
		String data = StringUtils.repeat("*", 10);
		initValueMeta(new MySQLDatabaseMeta(), DatabaseMeta.CLOB_LENGTH, data);

		verify(preparedStatementMock, times(1)).setString(0, data);
	}

	// PDI-14721 ESR-5021
	@Test
	public void testGetValueFromSQLTypeBinaryMysql() throws Exception {

		final int binaryColumnIndex = 1;
		ValueMetaBase valueMetaBase = new ValueMetaBase();
		DatabaseMeta dbMeta = spy(new DatabaseMeta());
		IDatabase iDatabase = new MySQLDatabaseMeta();
		dbMeta.setIDatabase(iDatabase);

		ResultSetMetaData metaData = mock(ResultSetMetaData.class);

		when(resultSet.getMetaData()).thenReturn(metaData);
		when(metaData.getColumnType(binaryColumnIndex)).thenReturn(Types.LONGVARBINARY);

		IValueMeta binaryValueMeta = valueMetaBase.getValueFromSQLType(dbMeta, TEST_NAME, metaData,
				binaryColumnIndex, false, false);
		assertEquals( IValueMeta.TYPE_BINARY, binaryValueMeta.getType());
		assertTrue(binaryValueMeta.isBinary());
	}

	@Test
	public void testMetdataPreviewSqlDoubleWithPrecisionGreaterThanLengthUsingMySQLVariant()
			throws SQLException, HopDatabaseException {
		doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
		doReturn(4).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
		doReturn(5).when(resultSet).getInt("DECIMAL_DIGITS");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getIDatabase();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isNumber());
		assertEquals(-1, valueMeta.getPrecision());
		assertEquals(-1, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlTimeToHopIntegerUsingMySQLVariant() throws SQLException, HopDatabaseException {
		doReturn(Types.TIME).when(resultSet).getInt("DATA_TYPE");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getIDatabase();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		doReturn(mock(Properties.class)).when(databaseMeta).getConnectionProperties();
		when(databaseMeta.getConnectionProperties().getProperty("yearIsDateType")).thenReturn("false");
		doReturn("YEAR").when(resultSet).getString("TYPE_NAME");
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isInteger());
		assertEquals(0, valueMeta.getPrecision());
		assertEquals(4, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlVarBinaryToHopBinaryUsingMySQLVariant()
			throws SQLException, HopDatabaseException {
		doReturn(Types.VARBINARY).when(resultSet).getInt("DATA_TYPE");
		doReturn(16).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getIDatabase();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isBinary());
		assertEquals(-1, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlDoubleToHopNumberUsingMySQL() throws SQLException, HopDatabaseException {
		doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
		doReturn(22).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getIDatabase();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isNumber());
		assertEquals(-1, valueMeta.getLength());
	}

}
