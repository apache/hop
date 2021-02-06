/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.groupby.Aggregation;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;

/**
 * Helper class to generate profiling pipelines...
 *
 */
public class PipelineProfileFactory {
  public static final String RESULT_TRANSFORM_NAME = "calc stats";

  private final IVariables variables;
  private final DatabaseMeta databaseMeta;
  private final String schemaTable;

  private IRowMeta tableLayout;

  /**
   * @param databaseMeta
   * @param schemaTable  the properly quoted schema-table combination
   */
  public PipelineProfileFactory( IVariables variables, DatabaseMeta databaseMeta, String schemaTable ) {
    this.variables = variables;
    this.databaseMeta = databaseMeta;
    this.schemaTable = schemaTable;
  }

  public PipelineMeta generatePipeline( ILoggingObject parentLoggingInterface ) throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();

    // Get the list of fields from the table...
    //
    tableLayout = getTableFields( parentLoggingInterface );

    // Now start building the pipeline...
    //
    PipelineMeta pipelineMeta = new PipelineMeta();

    // Create a transform to read the content of the table
    // Read the data from the database table...
    // For now we read it all, later we add options to only read the first X rows
    //
    TableInputMeta readMeta = new TableInputMeta();
    readMeta.setSql( "SELECT * FROM " + schemaTable );
    readMeta.setDatabaseMeta( databaseMeta );
    TransformMeta read = new TransformMeta( registry.getPluginId( TransformPluginType.class, readMeta ), "Read data", readMeta );
    read.setLocation( 50, 50 );
    pipelineMeta.addTransform( read );

    // Grab the data types too
    //

    // Now calculate the requested statistics for all fields...
    // TODO: create configuration possibility
    // For now, just do : min, max, sum, count, avg, std dev. (7)
    //
    int[] numericCalculations =
      new int[] {
        GroupByMeta.TYPE_GROUP_MIN, GroupByMeta.TYPE_GROUP_MAX, GroupByMeta.TYPE_GROUP_SUM,
        GroupByMeta.TYPE_GROUP_COUNT_ALL, GroupByMeta.TYPE_GROUP_AVERAGE,
        GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION, };

    int[] stringCalculations =
      new int[] { GroupByMeta.TYPE_GROUP_MIN, GroupByMeta.TYPE_GROUP_MAX, GroupByMeta.TYPE_GROUP_COUNT_ALL, };

    int[] dateCalculations =
      new int[] { GroupByMeta.TYPE_GROUP_MIN, GroupByMeta.TYPE_GROUP_MAX, GroupByMeta.TYPE_GROUP_COUNT_ALL, };

    int[] booleanCalculations =
      new int[] { GroupByMeta.TYPE_GROUP_MIN, GroupByMeta.TYPE_GROUP_MAX, GroupByMeta.TYPE_GROUP_COUNT_ALL, };

    // Run it through the "group by" transform without a grouping.
    // Later, we can use the UnivariateStats plugin/transform perhaps.
    //
    GroupByMeta statsMeta = new GroupByMeta();
    int nrNumeric = 0;
    int nrDates = 0;
    int nrStrings = 0;
    int nrBooleans = 0;
    for ( IValueMeta valueMeta : tableLayout.getValueMetaList() ) {
      if ( valueMeta.isNumeric() ) {
        nrNumeric++;
      }
      if ( valueMeta.isDate() ) {
        nrDates++;
      }
      if ( valueMeta.isString() ) {
        nrStrings++;
      }
      if ( valueMeta.isBoolean() ) {
        nrBooleans++;
      }
    }

    statsMeta.allocate( 0 );
    int calcIndex = 0;
    for ( int i = 0; i < tableLayout.size(); i++ ) {
      IValueMeta valueMeta = tableLayout.getValueMeta( i );
      // Numeric data...
      //
      if ( valueMeta.isNumeric() ) {
        for ( int c = 0; c < numericCalculations.length; c++ ) {
          String aggField =  valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( numericCalculations[ c ] ) + ")";
          String aggSubject = valueMeta.getName();
          int aggType = numericCalculations[ c ];
          statsMeta.getAggregations().add(new Aggregation(aggField, aggSubject, aggType, null) );
        }
      }

      // String data
      //
      if ( valueMeta.isString() ) {
        for ( int c = 0; c < stringCalculations.length; c++ ) {
          String aggField = valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( stringCalculations[ c ] ) + ")";
          String aggSubject = valueMeta.getName();
          int aggType = stringCalculations[ c ];
          statsMeta.getAggregations().add(new Aggregation(aggField, aggSubject, aggType, null) );
        }
      }

      // Date data
      //
      if ( valueMeta.isDate() ) {
        for ( int c = 0; c < dateCalculations.length; c++ ) {
          String aggField = valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( dateCalculations[ c ] ) + ")";
          String aggSubject = valueMeta.getName();
          int aggType = dateCalculations[ c ];
          statsMeta.getAggregations().add(new Aggregation(aggField, aggSubject, aggType, null) );
        }
      }

      // Boolean data
      //
      if ( valueMeta.isBoolean() ) {
        for ( int c = 0; c < booleanCalculations.length; c++ ) {
          String aggField = valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( booleanCalculations[ c ] ) + ")";
          String aggSubject = valueMeta.getName();
          int aggType = booleanCalculations[ c ];
          statsMeta.getAggregations().add(new Aggregation(aggField, aggSubject, aggType, null) );
        }
      }
    }
    TransformMeta calc = new TransformMeta( registry.getPluginId( TransformPluginType.class, statsMeta ), "Calc", statsMeta );
    calc.setLocation( 250, 50 );
    pipelineMeta.addTransform( calc );

    PipelineHopMeta hop = new PipelineHopMeta( read, calc );
    pipelineMeta.addPipelineHop( hop );

    DummyMeta dummyMeta = new DummyMeta();
    TransformMeta result =
      new TransformMeta( registry.getPluginId( TransformPluginType.class, dummyMeta ), RESULT_TRANSFORM_NAME, dummyMeta );
    result.setLocation( 450, 50 );
    pipelineMeta.addTransform( result );

    PipelineHopMeta hop2 = new PipelineHopMeta( calc, result );
    pipelineMeta.addPipelineHop( hop2 );

    return pipelineMeta;
  }

  private IRowMeta getTableFields( ILoggingObject parentLoggingObject ) throws HopDatabaseException {
    Database database = new Database( parentLoggingObject, variables, databaseMeta );
    try {
      database.connect();
      return database.getTableFields( schemaTable );
    } finally {
      database.disconnect();
    }

  }

  /**
   * @return the tableLayout
   */
  public IRowMeta getTableLayout() {
    return tableLayout;
  }
}
