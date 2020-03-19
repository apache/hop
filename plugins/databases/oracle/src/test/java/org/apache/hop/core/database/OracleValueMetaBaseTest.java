package org.apache.hop.core.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;


public class OracleValueMetaBaseTest {

	@ClassRule
	public static RestoreHopEnvironment env = new RestoreHopEnvironment();

	private ResultSet resultSet;
	private DatabaseMeta databaseMeta;
	private ValueMetaInterface valueMetaBase;
	
	@BeforeClass
	public static void setUpBeforeClass() throws HopException {
		PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
		PluginRegistry.addPluginType(DatabasePluginType.getInstance());
		PluginRegistry.init();	
	}

	@Before
	public void setUp() throws HopException {	
		valueMetaBase = ValueMetaFactory.createValueMeta(ValueMetaInterface.TYPE_NONE);
		databaseMeta = spy(new DatabaseMeta());
		resultSet = mock(ResultSet.class);
	}

	@Test
	public void testMetdataPreviewSqlVarBinaryToString() throws SQLException, HopDatabaseException {
		doReturn(Types.VARBINARY).when(resultSet).getInt("DATA_TYPE");
		doReturn(16).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(OracleDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isString());
		assertEquals(16, valueMeta.getLength());
	}

	@Test
	public void testMetdataPreviewSqlLongVarBinaryToString() throws SQLException, HopDatabaseException {
		doReturn(Types.LONGVARBINARY).when(resultSet).getInt("DATA_TYPE");
		doReturn(mock(OracleDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isString());
	}

	@Test
	public void testMetdataPreviewSqlNumericWithStrictBigNumberInterpretation()
			throws SQLException, HopDatabaseException {
		doReturn(Types.NUMERIC).when(resultSet).getInt("DATA_TYPE");
		doReturn(38).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
		doReturn(0).when(resultSet).getInt("DECIMAL_DIGITS");
		doReturn(mock(OracleDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		when(databaseMeta.isStrictBigNumberInterpretation()).thenReturn(true);
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isBigNumber());
	}

	@Test
	public void testMetdataPreviewSqlNumericWithoutStrictBigNumberInterpretation()
			throws SQLException, HopDatabaseException {
		doReturn(Types.NUMERIC).when(resultSet).getInt("DATA_TYPE");
		doReturn(38).when(resultSet).getInt("COLUMN_SIZE");
		doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
		doReturn(0).when(resultSet).getInt("DECIMAL_DIGITS");
		doReturn(mock(OracleDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		when(databaseMeta.isStrictBigNumberInterpretation()).thenReturn(false);
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isInteger());
	}

	@Test
	public void testMetdataPreviewSqlTimestampToDate() throws SQLException, HopDatabaseException {
		doReturn(Types.TIMESTAMP).when(resultSet).getInt("DATA_TYPE");
		doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
		doReturn(19).when(resultSet).getInt("DECIMAL_DIGITS");
		doReturn(mock(OracleDatabaseMeta.class)).when(databaseMeta).getDatabaseInterface();
		doReturn(true).when(databaseMeta).supportsTimestampDataType();
		ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isDate());
		assertEquals(-1, valueMeta.getPrecision());
		assertEquals(19, valueMeta.getLength());
	}
}
