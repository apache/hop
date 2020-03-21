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
import org.apache.hop.core.database.DatabaseInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.mysql.MySQLDatabaseMeta;
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
	private ValueMetaInterface valueMetaBase;

	@BeforeClass
	public static void setUpBeforeClass() throws HopException {
		PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
		PluginRegistry.addPluginType(DatabasePluginType.getInstance());
		PluginRegistry.init();
		// HopLogStore.init();
	}
	
	@Before
	public void setUp() throws HopPluginException {
		valueMetaBase = ValueMetaFactory.createValueMeta(ValueMetaInterface.TYPE_NONE);
		databaseMeta = spy(new DatabaseMeta());
		resultSet = mock(ResultSet.class);
	}

	protected void initValueMeta(BaseDatabaseMeta dbMeta, int length, Object data) throws HopDatabaseException {
		ValueMetaInterface valueMetaString = new ValueMetaString(LOG_FIELD, length, 0);
		databaseMeta.setDatabaseInterface(dbMeta);
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
		DatabaseInterface databaseInterface = new MySQLDatabaseMeta();
		dbMeta.setDatabaseInterface(databaseInterface);

		ResultSetMetaData metaData = mock(ResultSetMetaData.class);

		when(resultSet.getMetaData()).thenReturn(metaData);
		when(metaData.getColumnType(binaryColumnIndex)).thenReturn(Types.LONGVARBINARY);

		ValueMetaInterface binaryValueMeta = valueMetaBase.getValueFromSQLType(dbMeta, TEST_NAME, metaData,
				binaryColumnIndex, false, false);
		assertEquals(ValueMetaInterface.TYPE_BINARY, binaryValueMeta.getType());
		assertTrue(binaryValueMeta.isBinary());
	}

	@Test
	public void testMetdataPreviewSqlDoubleWithPrecisionGreaterThanLengthUsingMySQLVariant()
			throws SQLException, HopDatabaseException {
		doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
		doReturn(4).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
		doReturn(5).when(resultSet).getInt("DECIMAL_DIGITS");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isNumber());
		assertEquals(-1, valueMeta.getPrecision());
		assertEquals(-1, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlTimeToPentahoIntegerUsingMySQLVariant() throws SQLException, HopDatabaseException {
		doReturn(Types.TIME).when(resultSet).getInt("DATA_TYPE");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		doReturn(mock(Properties.class)).when(databaseMeta).getConnectionProperties();
		when(databaseMeta.getConnectionProperties().getProperty("yearIsDateType")).thenReturn("false");
		doReturn("YEAR").when(resultSet).getString("TYPE_NAME");
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isInteger());
		assertEquals(0, valueMeta.getPrecision());
		assertEquals(4, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlVarBinaryToPentahoBinaryUsingMySQLVariant()
			throws SQLException, HopDatabaseException {
		doReturn(Types.VARBINARY).when(resultSet).getInt("DATA_TYPE");
		doReturn(16).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isBinary());
		assertEquals(-1, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlDoubleToPentahoNumberUsingMySQL() throws SQLException, HopDatabaseException {
		doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
		doReturn(22).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(MySQLDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		doReturn(true).when(databaseMeta).isMySQLVariant();
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isNumber());
		assertEquals(-1, valueMeta.getLength());
	}

}
