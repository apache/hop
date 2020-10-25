/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.memgroupby;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.TreeBasedTable;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.*;
import org.mockito.ArgumentCaptor;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

public class MemoryGroupByAggregationTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private Variables variables;
  private Map<String, Integer> aggregates;

  public static final String TRANSFORM_NAME = "testTransform";
  private static final ImmutableMap<String, Integer> default_aggregates;

  static {
    default_aggregates = ImmutableMap.<String, Integer>builder()
      .put( "min", MemoryGroupByMeta.TYPE_GROUP_MIN )
      .put( "max", MemoryGroupByMeta.TYPE_GROUP_MAX )
      .put( "sum", MemoryGroupByMeta.TYPE_GROUP_SUM )
      .put( "ave", MemoryGroupByMeta.TYPE_GROUP_AVERAGE )
      .put( "count", MemoryGroupByMeta.TYPE_GROUP_COUNT_ALL )
      .put( "count_any", MemoryGroupByMeta.TYPE_GROUP_COUNT_ANY )
      .put( "count_distinct", MemoryGroupByMeta.TYPE_GROUP_COUNT_DISTINCT )
      .build();
  }

  private RowMeta rowMeta;
  private TreeBasedTable<Integer, Integer, Optional<Object>> data;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {
    rowMeta = new RowMeta();
    data = TreeBasedTable.create();
    variables = new Variables();
    aggregates = Maps.newHashMap( default_aggregates );
  }

  @Test
  @Ignore
  public void testDefault() throws Exception {
    addColumn( new ValueMetaInteger( "intg" ), 0L, 1L, 1L, 10L );
    addColumn( new ValueMetaInteger( "nul" ) );
    addColumn( new ValueMetaInteger( "mix1" ), -1L, 2L );
    addColumn( new ValueMetaInteger( "mix2" ), null, 7L );
    addColumn( new ValueMetaNumber( "mix3" ), -1.0, 2.5 );
    addColumn( new ValueMetaDate( "date1" ), new Date( 1L ), new Date( 2L ) );

    RowMetaAndData output = runTransform();

    assertThat( output.getInteger( "intg_min" ), is( 0L ) );
    assertThat( output.getInteger( "intg_max" ), is( 10L ) );
    assertThat( output.getInteger( "intg_sum" ), is( 12L ) );
    assertThat( output.getInteger( "intg_ave" ), is( 3L ) );
    assertThat( output.getInteger( "intg_count" ), is( 4L ) );
    assertThat( output.getInteger( "intg_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "intg_count_distinct" ), is( 3L ) );

    assertThat( output.getInteger( "nul_min" ), nullValue() );
    assertThat( output.getInteger( "nul_max" ), nullValue() );
    assertThat( output.getInteger( "nul_sum" ), nullValue() );
    assertThat( output.getInteger( "nul_ave" ), nullValue() );
    assertThat( output.getInteger( "nul_count" ), is( 0L ) );
    assertThat( output.getInteger( "nul_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "nul_count_distinct" ), is( 0L ) );

    assertThat( output.getInteger( "mix1_max" ), is( 2L ) );
    assertThat( output.getInteger( "mix1_min" ), is( -1L ) );
    assertThat( output.getInteger( "mix1_sum" ), is( 1L ) );
    assertThat( output.getInteger( "mix1_ave" ), is( 0L ) );
    assertThat( output.getInteger( "mix1_count" ), is( 2L ) );
    assertThat( output.getInteger( "mix1_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "mix1_count_distinct" ), is( 2L ) );

    assertThat( output.getInteger( "mix2_max" ), is( 7L ) );
    assertThat( output.getInteger( "mix2_min" ), is( 7L ) );
    assertThat( output.getInteger( "mix2_sum" ), is( 7L ) );
    assertThat( output.getNumber( "mix2_ave", Double.NaN ), is( 7.0 ) );
    assertThat( output.getInteger( "mix2_count" ), is( 1L ) );
    assertThat( output.getInteger( "mix2_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "mix2_count_distinct" ), is( 1L ) );

    assertThat( output.getNumber( "mix3_max", Double.NaN ), is( 2.5 ) );
    assertThat( output.getNumber( "mix3_min", Double.NaN ), is( -1.0 ) );
    assertThat( output.getNumber( "mix3_sum", Double.NaN ), is( 1.5 ) );
    assertThat( output.getNumber( "mix3_ave", Double.NaN ), is( 0.75 ) );
    assertThat( output.getInteger( "mix3_count" ), is( 2L ) );
    assertThat( output.getInteger( "mix3_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "mix3_count_distinct" ), is( 2L ) );

    assertThat( output.getNumber( "date1_min", Double.NaN ), is( 1.0 ) );
    assertThat( output.getNumber( "date1_max", Double.NaN ), is( 2.0 ) );
    assertThat( output.getNumber( "date1_sum", Double.NaN ), is( 3.0 ) );
    assertThat( output.getNumber( "date1_ave", Double.NaN ), is( 1.5 ) );
    assertThat( output.getInteger( "date1_count" ), is( 2L ) );
    assertThat( output.getInteger( "date1_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "date1_count_distinct" ), is( 2L ) );
  }

  @Test
  @Ignore
  public void testCompatibility() throws HopException {
    variables.setVariable( Const.HOP_COMPATIBILITY_MEMORY_GROUP_BY_SUM_AVERAGE_RETURN_NUMBER_TYPE, "Y" );

    addColumn( new ValueMetaInteger( "intg" ), 0L, 1L, 1L, 10L );
    addColumn( new ValueMetaInteger( "nul" ) );
    addColumn( new ValueMetaInteger( "mix1" ), -1L, 2L );
    addColumn( new ValueMetaInteger( "mix2" ), null, 7L );
    addColumn( new ValueMetaNumber( "mix3" ), -1.0, 2.5 );

    RowMetaAndData output = runTransform();

    assertThat( output.getInteger( "intg_min" ), is( 0L ) );
    assertThat( output.getInteger( "intg_max" ), is( 10L ) );
    assertThat( output.getInteger( "intg_sum" ), is( 12L ) );
    assertThat( output.getInteger( "intg_ave" ), is( 3L ) );
    assertThat( output.getInteger( "intg_count" ), is( 4L ) );
    assertThat( output.getInteger( "intg_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "intg_count_distinct" ), is( 3L ) );

    assertThat( output.getInteger( "nul_min" ), nullValue() );
    assertThat( output.getInteger( "nul_max" ), nullValue() );
    assertThat( output.getInteger( "nul_sum" ), nullValue() );
    assertThat( output.getInteger( "nul_ave" ), nullValue() );
    assertThat( output.getInteger( "nul_count" ), is( 0L ) );
    assertThat( output.getInteger( "nul_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "nul_count_distinct" ), is( 0L ) );

    assertThat( output.getInteger( "mix1_max" ), is( 2L ) );
    assertThat( output.getInteger( "mix1_min" ), is( -1L ) );
    assertThat( output.getInteger( "mix1_sum" ), is( 1L ) );
    assertThat( output.getNumber( "mix1_ave", Double.NaN ), is( 0.5 ) );
    assertThat( output.getInteger( "mix1_count" ), is( 2L ) );
    assertThat( output.getInteger( "mix1_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "mix1_count_distinct" ), is( 2L ) );

    assertThat( output.getInteger( "mix2_max" ), is( 7L ) );
    assertThat( output.getInteger( "mix2_min" ), is( 7L ) );
    assertThat( output.getInteger( "mix2_sum" ), is( 7L ) );
    assertThat( output.getNumber( "mix2_ave", Double.NaN ), is( 7.0 ) );
    assertThat( output.getInteger( "mix2_count" ), is( 1L ) );
    assertThat( output.getInteger( "mix2_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "mix2_count_distinct" ), is( 1L ) );

    assertThat( output.getNumber( "mix3_max", Double.NaN ), is( 2.5 ) );
    assertThat( output.getNumber( "mix3_min", Double.NaN ), is( -1.0 ) );
    assertThat( output.getNumber( "mix3_sum", Double.NaN ), is( 1.5 ) );
    assertThat( output.getNumber( "mix3_ave", Double.NaN ), is( 0.75 ) );
    assertThat( output.getInteger( "mix3_count" ), is( 2L ) );
    assertThat( output.getInteger( "mix3_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "mix3_count_distinct" ), is( 2L ) );
  }

  @Test
  @Ignore
  public void testNullMin() throws Exception {
    variables.setVariable( Const.HOP_AGGREGATION_MIN_NULL_IS_VALUED, "Y" );

    addColumn( new ValueMetaInteger( "intg" ), null, 0L, 1L, -1L );
    addColumn( new ValueMetaString( "str" ), "A", null, "B", null );

    aggregates = Maps.toMap( ImmutableList.of( "min", "max" ), Functions.forMap( default_aggregates ) );

    RowMetaAndData output = runTransform();

    assertThat( output.getInteger( "intg_min" ), nullValue() );
    assertThat( output.getInteger( "intg_max" ), is( 1L ) );

    assertThat( output.getString( "str_min", null ), nullValue() );
    assertThat( output.getString( "str_max", "invalid" ), is( "B" ) );
  }

  @Test
  @Ignore
  public void testNullsAreZeroCompatible() throws Exception {
    variables.setVariable( Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "Y" );
    variables.setVariable( Const.HOP_COMPATIBILITY_MEMORY_GROUP_BY_SUM_AVERAGE_RETURN_NUMBER_TYPE, "Y" );

    addColumn( new ValueMetaInteger( "nul" ) );
    addColumn( new ValueMetaInteger( "both" ), -2L, 0L, null, 10L );

    RowMetaAndData output = runTransform();

    assertThat( output.getInteger( "nul_min" ), is( 0L ) );
    assertThat( output.getInteger( "nul_max" ), is( 0L ) );
    assertThat( output.getInteger( "nul_sum" ), is( 0L ) );
    assertThat( output.getInteger( "nul_ave" ), is( 0L ) );
    assertThat( output.getInteger( "nul_count" ), is( 0L ) );
    assertThat( output.getInteger( "nul_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "nul_count_distinct" ), is( 0L ) );

    assertThat( output.getInteger( "both_max" ), is( 10L ) );
    assertThat( output.getInteger( "both_min" ), is( -2L ) );
    assertThat( output.getInteger( "both_sum" ), is( 8L ) );
    assertThat( output.getInteger( "both_ave" ), is( 3L ) );
    assertThat( output.getInteger( "both_count" ), is( 3L ) );
    assertThat( output.getInteger( "both_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "both_count_distinct" ), is( 3L ) );
  }

  @Test
  @Ignore
  public void testNullsAreZeroDefault() throws Exception {
    variables.setVariable( Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "Y" );

    addColumn( new ValueMetaInteger( "nul" ) );
    addColumn( new ValueMetaInteger( "both" ), -2L, 0L, null, 10L );
    addColumn( new ValueMetaNumber( "both_num" ), -2.0, 0.0, null, 10.0 );

    RowMetaAndData output = runTransform();

    assertThat( output.getInteger( "nul_min" ), is( 0L ) );
    assertThat( output.getInteger( "nul_max" ), is( 0L ) );
    assertThat( output.getInteger( "nul_sum" ), is( 0L ) );
    assertThat( output.getInteger( "nul_ave" ), is( 0L ) );
    assertThat( output.getInteger( "nul_count" ), is( 0L ) );
    assertThat( output.getInteger( "nul_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "nul_count_distinct" ), is( 0L ) );

    assertThat( output.getInteger( "both_max" ), is( 10L ) );
    assertThat( output.getInteger( "both_min" ), is( -2L ) );
    assertThat( output.getInteger( "both_sum" ), is( 8L ) );
    assertThat( output.getInteger( "both_ave" ), is( 2L ) );
    assertThat( output.getInteger( "both_count" ), is( 3L ) );
    assertThat( output.getInteger( "both_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "both_count_distinct" ), is( 3L ) );

    assertThat( output.getNumber( "both_num_max", Double.NaN ), is( 10.0 ) );
    assertThat( output.getNumber( "both_num_min", Double.NaN ), is( -2.0 ) );
    assertThat( output.getNumber( "both_num_sum", Double.NaN ), is( 8.0 ) );
    assertEquals( 2.666666, output.getNumber( "both_num_ave", Double.NaN ), 0.000001 /* delta */ );
    assertThat( output.getInteger( "both_num_count" ), is( 3L ) );
    assertThat( output.getInteger( "both_num_count_any" ), is( 4L ) );
    assertThat( output.getInteger( "both_num_count_distinct" ), is( 3L ) );
  }

  @Test
  @Ignore
  public void testSqlCompatible() throws Exception {
    addColumn( new ValueMetaInteger( "value" ), null, -2L, null, 0L, null, 10L, null, null, 0L, null );

    RowMetaAndData output = runTransform();

    assertThat( output.getInteger( "value_max" ), is( 10L ) );
    assertThat( output.getInteger( "value_min" ), is( -2L ) );
    assertThat( output.getInteger( "value_sum" ), is( 8L ) );
    assertThat( output.getInteger( "value_ave" ), is( 2L ) );
    assertThat( output.getInteger( "value_count" ), is( 4L ) );
    assertThat( output.getInteger( "value_count_any" ), is( 10L ) );
    assertThat( output.getInteger( "value_count_distinct" ), is( 3L ) );
  }

  private RowMetaAndData runTransform() throws HopException {
    // Allocate meta
    List<String> aggKeys = ImmutableList.copyOf( aggregates.keySet() );
    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.allocate( 0, rowMeta.size() * aggKeys.size() );
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      String name = rowMeta.getValueMeta( i ).getName();
      for ( int j = 0; j < aggKeys.size(); j++ ) {
        String aggKey = aggKeys.get( j );
        int index = i * aggKeys.size() + j;

        meta.getAggregateField()[ index ] = name + "_" + aggKey;
        meta.getSubjectField()[ index ] = name;
        meta.getAggregateType()[ index ] = aggregates.get( aggKey );
      }
    }

    MemoryGroupByData data = new MemoryGroupByData();
    data.map = Maps.newHashMap();

    // Add to pipeline
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    TransformMeta transformMeta = new TransformMeta( TRANSFORM_NAME, meta );
    when( pipelineMeta.findTransform( TRANSFORM_NAME ) ).thenReturn( transformMeta );

    // Spy on transform, regrettable but we need to easily inject rows
    MemoryGroupBy transform = spy( new MemoryGroupBy( transformMeta, meta, data, 0, pipelineMeta, mock( Pipeline.class ) ) );
    transform.copyVariablesFrom( variables );
    doNothing().when( transform ).putRow( (IRowMeta) any(), (Object[]) any() );
    doNothing().when( transform ).setOutputDone();

    // Process rows
    doReturn( rowMeta ).when( transform ).getInputRowMeta();
    for ( Object[] row : getRows() ) {
      doReturn( row ).when( transform ).getRow();
//      assertThat(transform.processRow(), is(true));
      while(transform.processRow()){
      }
    }
    verify( transform, never() ).putRow( (IRowMeta) any(), (Object[]) any() );

    // Mark stop
    doReturn( null ).when( transform ).getRow();
//    assertThat(transform.processRow(), is(true)) ;
    while(transform.processRow()){
    };
    verify( transform ).setOutputDone();

    // Collect output
    ArgumentCaptor<IRowMeta> rowMetaCaptor = ArgumentCaptor.forClass( IRowMeta.class );
    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass( Object[].class );
    verify( transform ).putRow( rowMetaCaptor.capture(), rowCaptor.capture() );

    return new RowMetaAndData( rowMetaCaptor.getValue(), rowCaptor.getValue() );
  }

  private void addColumn( IValueMeta meta, Object... values ) {
    int column = rowMeta.size();

    rowMeta.addValueMeta( meta );
    for ( int row = 0; row < values.length; row++ ) {
      data.put( row, column, Optional.fromNullable( values[ row ] ) );
    }
  }

  private Iterable<Object[]> getRows() {
    if ( data.isEmpty() ) {
      return ImmutableSet.of();
    }

    Range<Integer> rows = Range.closed( 0, data.rowMap().lastKey() );

    return FluentIterable.from( ContiguousSet.create( rows, DiscreteDomain.integers() ) )
      .transform( Functions.forMap( data.rowMap(), ImmutableMap.<Integer, Optional<Object>>of() ) )
      .transform( input -> {
        Object[] row = new Object[ rowMeta.size() ];
        for ( Map.Entry<Integer, Optional<Object>> entry : input.entrySet() ) {
          row[ entry.getKey() ] = entry.getValue().orNull();
        }
        return row;
      } );
  }
}
