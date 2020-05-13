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
package org.apache.hop.pipeline.transforms.mappinginput;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MappingInputFieldsTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static Plugin p1;
  private static Plugin p2;

  MappingInput transform;
  MappingInputMeta meta;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();

    // PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.getInstance().registerPluginType( ValueMetaPluginType.class );

    Map<Class<?>, String> classes = new HashMap<Class<?>, String>();
    classes.put( IValueMeta.class, "org.apache.hop.core.row.value.ValueMetaString" );
    p1 =
      new Plugin( new String[] { "2" }, ValueMetaPluginType.class, IValueMeta.class, "", "", "", "", false,
        true, classes, null, null, null );

    classes = new HashMap<Class<?>, String>();
    classes.put( IValueMeta.class, "org.apache.hop.core.row.value.ValueMetaInteger" );
    p2 =
      new Plugin( new String[] { "5" }, ValueMetaPluginType.class, IValueMeta.class, "", "", "", "", false,
        true, classes, null, null, null );

    PluginRegistry.getInstance().registerPlugin( ValueMetaPluginType.class, p1 );
    PluginRegistry.getInstance().registerPlugin( ValueMetaPluginType.class, p2 );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if ( p1 != null ) {
      PluginRegistry.getInstance().removePlugin( ValueMetaPluginType.class, p1 );
    }
    if ( p2 != null ) {
      PluginRegistry.getInstance().removePlugin( ValueMetaPluginType.class, p2 );
    }
  }

  @Before
  public void setUp() throws Exception {
    meta = new MappingInputMeta();
    meta.setFieldName( new String[] { "n2", "n4" } );
    meta.setFieldType( new int[] { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_INTEGER } );
    meta.setFieldLength( new int[] { 0, 0 } );
    meta.setFieldPrecision( new int[] { 0, 0 } );

    TransformMeta sm = new TransformMeta( "MappingInput", "SubPipeline", meta );
    PipelineMeta tm = new PipelineMeta();
    tm.addTransform( sm );
    LoggingObjectInterface loi = new SimpleLoggingObject( "lo", LoggingObjectType.TRANSFORM, null );
    Pipeline tr = new LocalPipelineEngine( tm, loi );

    transform = new MappingInput( sm, null, 0, tm, tr );
    transform.getPipeline().setRunning( true );
  }

  /**
   * verifies: If SelectingAndSortingUnspecifiedFields checkbox is checked, then
   * <ol>
   * <li>all fields throw to the next transform;
   * <li>fields are resorted: mapped fields, then alphabetical sorted not mapped fields.
   * </ol>
   */
  @Test
  public void testSelectingAndSortingUnspecifiedFields() throws Exception {
    meta.setSelectingAndSortingUnspecifiedFields( true );

    MappingInputData sdi = new MappingInputData();

    sdi.linked = true;
    sdi.valueRenames = new ArrayList<MappingValueRename>();
    sdi.valueRenames.add( new MappingValueRename( "number2", "n2" ) );
    sdi.valueRenames.add( new MappingValueRename( "number4", "n4" ) );

    BlockingRowSet in = new BlockingRowSet( 10 );
    BlockingRowSet out = new BlockingRowSet( 10 );

    RowMeta rm = new RowMeta();

    rm.addValueMeta( new ValueMetaString( "string" ) );
    rm.addValueMeta( new ValueMetaInteger( "number1" ) );
    rm.addValueMeta( new ValueMetaInteger( "number2" ) );
    rm.addValueMeta( new ValueMetaInteger( "number3" ) );
    rm.addValueMeta( new ValueMetaInteger( "number" ) );
    rm.addValueMeta( new ValueMetaInteger( "number4" ) );
    rm.addValueMeta( new ValueMetaInteger( "number5" ) );

    in.putRow( rm, new Object[] { "str", new Integer( 100501 ), new Integer( 100502 ), new Integer( 100503 ),
      new Integer( 100500 ), new Integer( 100504 ), new Integer( 100505 ) } );
    in.putRow( rm, new Object[] { "str_1", new Integer( 200501 ), new Integer( 200502 ), new Integer( 200503 ),
      new Integer( 200500 ), new Integer( 200504 ), new Integer( 200505 ) } );

    transform.addRowSetToInputRowSets( in );
    transform.addRowSetToOutputRowSets( out );

    assertTrue( transform.init() );

    assertTrue( transform.processRow();

    Object[] outRowData = out.getRow();

    IRowMeta outMeta = out.getRowMeta();

    assertEquals( "All fields are expected.", 7, outMeta.size() );

    int i = 0;

    // Check if row-meta is formed according to the transform specification
    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n2", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n4", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number1", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number3", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number5", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_STRING, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "string", outMeta.getValueMeta( i++ ).getName() );

    // Check if row-data corresponds to the row-meta
    assertEquals( "the field value mismatch.", new Integer( 100502 ), outRowData[ 0 ] );
    assertEquals( "the field value mismatch.", new Integer( 100504 ), outRowData[ 1 ] );
    assertEquals( "the field value mismatch.", new Integer( 100500 ), outRowData[ 2 ] );
    assertEquals( "the field value mismatch.", new Integer( 100501 ), outRowData[ 3 ] );
    assertEquals( "the field value mismatch.", new Integer( 100503 ), outRowData[ 4 ] );
    assertEquals( "the field value mismatch.", new Integer( 100505 ), outRowData[ 5 ] );
    assertEquals( "the field value mismatch.", "str", outRowData[ 6 ] );

    assertTrue( transform.processRow();

    outRowData = out.getRow();

    outMeta = out.getRowMeta();

    assertEquals( "All fields are expected.", 7, outMeta.size() );

    i = 0;

    // Check if row-meta is formed according to the transform specification
    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n2", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n4", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number1", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number3", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number5", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_STRING, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "string", outMeta.getValueMeta( i++ ).getName() );

    // Check if row-data corresponds to the row-meta
    assertEquals( "the field value mismatch.", new Integer( 200502 ), outRowData[ 0 ] );
    assertEquals( "the field value mismatch.", new Integer( 200504 ), outRowData[ 1 ] );
    assertEquals( "the field value mismatch.", new Integer( 200500 ), outRowData[ 2 ] );
    assertEquals( "the field value mismatch.", new Integer( 200501 ), outRowData[ 3 ] );
    assertEquals( "the field value mismatch.", new Integer( 200503 ), outRowData[ 4 ] );
    assertEquals( "the field value mismatch.", new Integer( 200505 ), outRowData[ 5 ] );
    assertEquals( "the field value mismatch.", "str_1", outRowData[ 6 ] );

  }

  /*
   * verifies: If SelectingAndSortingUnspecifiedFields checkbox is not checked, then 1)all fields throw to the next transform;
   * 2)fields are not resorted;
   */
  @Test
  public void testOnlySpecifiedFields() throws Exception {
    meta.setSelectingAndSortingUnspecifiedFields( false );

    MappingInputData sdi = new MappingInputData();

    sdi.linked = true;
    sdi.valueRenames = new ArrayList<MappingValueRename>();
    sdi.valueRenames.add( new MappingValueRename( "number2", "n2" ) );
    sdi.valueRenames.add( new MappingValueRename( "number4", "n4" ) );

    BlockingRowSet in = new BlockingRowSet( 10 );
    BlockingRowSet out = new BlockingRowSet( 10 );

    RowMeta rm = new RowMeta();

    rm.addValueMeta( new ValueMetaString( "string" ) );
    rm.addValueMeta( new ValueMetaInteger( "number1" ) );
    rm.addValueMeta( new ValueMetaInteger( "number2" ) );
    rm.addValueMeta( new ValueMetaInteger( "number3" ) );
    rm.addValueMeta( new ValueMetaInteger( "number" ) );
    rm.addValueMeta( new ValueMetaInteger( "number4" ) );
    rm.addValueMeta( new ValueMetaInteger( "number5" ) );

    in.putRow( rm, new Object[] { "str", new Integer( 100501 ), new Integer( 100502 ), new Integer( 100503 ),
      new Integer( 100500 ), new Integer( 100504 ), new Integer( 100505 ) } );
    in.putRow( rm, new Object[] { "str_1", new Integer( 200501 ), new Integer( 200502 ), new Integer( 200503 ),
      new Integer( 200500 ), new Integer( 200504 ), new Integer( 200505 ) } );

    transform.addRowSetToInputRowSets( in );
    transform.addRowSetToOutputRowSets( out );

    assertTrue( transform.init() );

    assertTrue( transform.processRow();

    Object[] outRowData = out.getRow();

    IRowMeta outMeta = out.getRowMeta();

    assertEquals( "All fields are expected.", 7, outMeta.size() );

    int i = 0;

    // Check if row-meta is formed according to the transform specification
    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_STRING, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "string", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number1", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n2", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number3", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n4", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number5", outMeta.getValueMeta( i++ ).getName() );

    // Check if row-data corresponds to the row-meta
    assertEquals( "the field value mismatch.", "str", outRowData[ 0 ] );
    assertEquals( "the field value mismatch.", new Integer( 100501 ), outRowData[ 1 ] );
    assertEquals( "the field value mismatch.", new Integer( 100502 ), outRowData[ 2 ] );
    assertEquals( "the field value mismatch.", new Integer( 100503 ), outRowData[ 3 ] );
    assertEquals( "the field value mismatch.", new Integer( 100500 ), outRowData[ 4 ] );
    assertEquals( "the field value mismatch.", new Integer( 100504 ), outRowData[ 5 ] );
    assertEquals( "the field value mismatch.", new Integer( 100505 ), outRowData[ 6 ] );

    assertTrue( transform.processRow();

    outRowData = out.getRow();

    outMeta = out.getRowMeta();

    assertEquals( "All fields are expected.", 7, outMeta.size() );

    i = 0;

    // Check if row-meta is formed according to the transform specification
    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_STRING, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "string", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number1", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n2", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number3", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "n4", outMeta.getValueMeta( i++ ).getName() );

    assertEquals( "the field type-meta mismatch.", IValueMeta.TYPE_INTEGER, outMeta.getValueMeta( i ).getType() );
    assertEquals( "the field name-meta mismatch.", "number5", outMeta.getValueMeta( i++ ).getName() );

    // Check if row-data corresponds to the row-meta
    assertEquals( "the field value mismatch.", "str_1", outRowData[ 0 ] );
    assertEquals( "the field value mismatch.", new Integer( 200501 ), outRowData[ 1 ] );
    assertEquals( "the field value mismatch.", new Integer( 200502 ), outRowData[ 2 ] );
    assertEquals( "the field value mismatch.", new Integer( 200503 ), outRowData[ 3 ] );
    assertEquals( "the field value mismatch.", new Integer( 200500 ), outRowData[ 4 ] );
    assertEquals( "the field value mismatch.", new Integer( 200504 ), outRowData[ 5 ] );
    assertEquals( "the field value mismatch.", new Integer( 200505 ), outRowData[ 6 ] );

  }
}
