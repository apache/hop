package org.apache.hop.databases.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
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
	
	private DatabaseMeta databaseMeta;
	private IValueMeta valueMetaBase;
	private ResultSet resultSet;
	
	@BeforeClass
	public static void setUpBeforeClass() throws HopException {
		PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
		PluginRegistry.addPluginType(DatabasePluginType.getInstance());
		PluginRegistry.init();	
	}

	@Before
	public void setUp() throws HopException {	
		valueMetaBase = ValueMetaFactory.createValueMeta( IValueMeta.TYPE_NONE);
	    databaseMeta = spy(DatabaseMeta.class);
	    databaseMeta.setIDatabase(spy(OracleDatabaseMeta.class));
		resultSet = mock(ResultSet.class);
	}

	@Test
	public void testMetadataPreviewSqlVarBinaryToString() throws SQLException, HopDatabaseException {
		when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.VARBINARY);		
		when(resultSet.getInt("COLUMN_SIZE")).thenReturn(16);
		
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isString());
		assertEquals(16, valueMeta.getLength());
	}	
	
	@Test
	public void testMetadataPreviewSqlLongVarBinaryToString() throws SQLException, HopDatabaseException {
		when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.LONGVARBINARY);

		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isString());
	}
	
	@Test
	public void testMetadataPreviewSqlNumericWithStrictBigNumberInterpretation()
			throws SQLException, HopDatabaseException {
		
		when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.NUMERIC);		
		when(resultSet.getInt("COLUMN_SIZE")).thenReturn(38);
		when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(0);
		when(databaseMeta.getIDatabase().isStrictBigNumberInterpretation()).thenReturn(true);
		
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isBigNumber());
	}

	@Test
	public void testMetadataPreviewSqlNumericWithoutStrictBigNumberInterpretation()
			throws SQLException, HopDatabaseException {
		when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.NUMERIC);		
		when(resultSet.getInt("COLUMN_SIZE")).thenReturn(38);
		when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(0);
		when(databaseMeta.getIDatabase().isStrictBigNumberInterpretation()).thenReturn(false);
		
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isInteger());
	}

	@Test
	public void testMetadataPreviewSqlTimestampToDate() throws SQLException, HopDatabaseException {
		when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.TIMESTAMP);		
		when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(19);
		when(resultSet.getObject("DECIMAL_DIGITS")).thenReturn(mock(Object.class));
		when(databaseMeta.supportsTimestampDataType()).thenReturn(true);
		
		IValueMeta valueMeta = valueMetaBase.getMetadataPreview(databaseMeta, resultSet);
		assertTrue(valueMeta.isDate());
		assertEquals(-1, valueMeta.getPrecision());
		assertEquals(19, valueMeta.getLength());
	}
}
