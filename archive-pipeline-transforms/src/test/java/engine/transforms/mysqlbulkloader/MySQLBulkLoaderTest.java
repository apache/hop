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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.MySQLDatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MySQLBulkLoaderTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  MySQLBulkLoaderMeta lmeta;
  MySQLBulkLoaderData ldata;
  MySQLBulkLoader lder;
  TransformMeta smeta;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "MysqlBulkLoader" );

    Map<String, String> vars = new HashMap<>();
    vars.put( "delim", "," );
    vars.put( "enclos", "'" );
    vars.put( "charset", "UTF8" );
    vars.put( "tbl", "sometable" );
    vars.put( "schema", "someschema" );
    pipelineMeta.injectVariables( vars );
    MySQLDatabaseMeta mysql = new MySQLDatabaseMeta();
    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setIDatabase( mysql );
    dbMeta.setQuoteAllFields( true );
    lmeta = new MySQLBulkLoaderMeta();
    lmeta.setDelimiter( "${delim}" );
    lmeta.setEnclosure( "${enclos}" );
    lmeta.setEncoding( "${charset}" );
    lmeta.setTableName( "${tbl}" );
    lmeta.setSchemaName( "${schema}" );
    lmeta.setDatabaseMeta( dbMeta );
    ldata = new MySQLBulkLoaderData();
    PluginRegistry plugReg = PluginRegistry.getInstance();
    String mblPid = plugReg.getPluginId( TransformPluginType.class, lmeta );
    smeta = new TransformMeta( mblPid, "MySqlBulkLoader", lmeta );
    Pipeline pipeline = new Pipeline( pipelineMeta );
    pipelineMeta.addTransform( smeta );
    lder = new MySQLBulkLoader( smeta, ldata, 1, pipelineMeta, pipeline );
    lder.copyVariablesFrom( pipelineMeta );
  }

  @Test
  public void testFieldFormatType() throws HopXmlException {
    MySQLBulkLoaderMeta lm = new MySQLBulkLoaderMeta();
    Document document = XmlHandler.loadXmlFile( this.getClass().getResourceAsStream( "transform.xml" ) );
    IMetaStore metastore = null;
    Node transformNode = (Node) document.getDocumentElement();
    lm.loadXml( transformNode, metastore );
    int[] codes = lm.getFieldFormatType();
    assertEquals( 3, codes[ 0 ] );
    assertEquals( 4, codes[ 1 ] );
  }

  @Test
  public void testVariableSubstitution() throws HopException {
    lder.init();
    String is = null;
    is = new String( ldata.quote );
    assertEquals( "'", is );
    is = new String( ldata.separator );
    assertEquals( ",", is );
    assertEquals( "UTF8", ldata.bulkTimestampMeta.getStringEncoding() );
    assertEquals( "UTF8", ldata.bulkDateMeta.getStringEncoding() );
    assertEquals( "UTF8", ldata.bulkNumberMeta.getStringEncoding() );
    assertEquals( "`someschema`.`sometable`", ldata.schemaTable );
  }

  /**
   * [PDI-17481] Testing the ability that if no connection is specified, we will mark it as a fail and log the
   * appropriate reason to the user by throwing a HopException.
   */
  @Test
  public void testNoDatabaseConnection() {
    lmeta.setDatabaseMeta( null );
    assertFalse( lder.init();
    try {
      // Verify that the database connection being set to null throws a HopException with the following message.
      lder.verifyDatabaseConnection();
      // If the method does not throw a Hop Exception, then the DB was set and not null for this test. Fail it.
      fail( "Database Connection is not null, this fails the test." );
    } catch ( HopException aHopException ) {
      assertTrue( aHopException.getMessage().contains( "There is no connection defined in this transform" ) );
    }
  }

  @Test
  public void testEscapeCharacters() throws HopException, IOException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );

    MySQLBulkLoader loader;
    MySQLBulkLoaderData ld = new MySQLBulkLoaderData();
    MySQLBulkLoaderMeta lm = new MySQLBulkLoaderMeta();

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "loader" );

    PluginRegistry plugReg = PluginRegistry.getInstance();

    String loaderPid = plugReg.getPluginId( TransformPluginType.class, lm );
    TransformMeta transformMeta = new TransformMeta( loaderPid, "loader", lm );
    Pipeline pipeline = new Pipeline( pipelineMeta );
    pipelineMeta.addTransform( transformMeta );
    pipeline.setRunning( true );

    loader = spy( new MySQLBulkLoader( transformMeta, ld, 1, pipelineMeta, pipeline ) );

    RowMeta rm = new RowMeta();
    ValueMetaString vm = new ValueMetaString( "I don't want NPE!" );
    rm.addValueMeta( vm );
    RowMeta spyRowMeta = spy( new RowMeta() );
    when( spyRowMeta.getValueMeta( anyInt() ) ).thenReturn( vm );
    loader.setInputRowMeta( spyRowMeta );

    MySQLBulkLoaderMeta smi = new MySQLBulkLoaderMeta();
    smi.setFieldStream( new String[] { "Test" } );
    smi.setFieldFormatType( new int[] { MySQLBulkLoaderMeta.FIELD_FORMAT_TYPE_STRING_ESCAPE } );
    smi.setEscapeChar( "\\" );
    smi.setEnclosure( "\"" );
    smi.setDatabaseMeta( mock( DatabaseMeta.class ) );

    MySQLBulkLoaderData sdi = new MySQLBulkLoaderData();
    sdi.keynrs = new int[ 1 ];
    sdi.keynrs[ 0 ] = 0;
    sdi.fifoStream = mock( OutputStream.class );
    loader.init();
    loader.first = false;

    when( loader.getRow() ).thenReturn( new String[] { "test\"Escape\\" } );
    loader.processRow();
    verify( sdi.fifoStream, times( 1 ) ).write( "test\\\"Escape\\\\".getBytes() );
  }

  /**
   * Default conversion mask for Number column type should be calculated according to length and precision.
   * For example, for type NUMBER(6,3) conversion mask should be: " #000.000;-#000.000"
   */
  @Test
  public void testNumberFormatting() throws HopException, IOException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );

    MySQLBulkLoader loader;
    MySQLBulkLoaderData ld = new MySQLBulkLoaderData();
    MySQLBulkLoaderMeta lm = new MySQLBulkLoaderMeta();

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "loader" );

    PluginRegistry plugReg = PluginRegistry.getInstance();

    String loaderPid = plugReg.getPluginId( TransformPluginType.class, lm );
    TransformMeta transformMeta = new TransformMeta( loaderPid, "loader", lm );
    Pipeline pipeline = new Pipeline( pipelineMeta );
    pipelineMeta.addTransform( transformMeta );
    pipeline.setRunning( true );

    loader = spy( new MySQLBulkLoader( transformMeta, ld, 1, pipelineMeta, pipeline ) );

    RowMeta rm = new RowMeta();
    ValueMetaNumber vm = new ValueMetaNumber( "Test" );
    rm.addValueMeta( vm );
    RowMeta spyRowMeta = spy( new RowMeta() );
    when( spyRowMeta.getValueMeta( anyInt() ) ).thenReturn( vm );
    loader.setInputRowMeta( spyRowMeta );

    MySQLBulkLoaderMeta smi = new MySQLBulkLoaderMeta();
    smi.setFieldStream( new String[] { "Test" } );
    smi.setFieldFormatType( new int[] { MySQLBulkLoaderMeta.FIELD_FORMAT_TYPE_OK } );
    smi.setDatabaseMeta( mock( DatabaseMeta.class ) );

    ValueMetaNumber vmn = new ValueMetaNumber( "Test" );
    vmn.setLength( 6, 3 );

    MySQLBulkLoaderData sdi = new MySQLBulkLoaderData();
    sdi.keynrs = new int[ 1 ];
    sdi.keynrs[ 0 ] = 0;
    sdi.fifoStream = mock( OutputStream.class );
    sdi.bulkFormatMeta = new IValueMeta[] { vmn };

    loader.init();
    loader.first = false;

    when( loader.getRow() ).thenReturn( new Double[] { 1.023 } );
    loader.processRow();
    verify( sdi.fifoStream, times( 1 ) ).write( " 001.023".getBytes() );
    assertEquals( " #000.000;-#000.000", vmn.getDecimalFormat().toPattern() );
  }
}
