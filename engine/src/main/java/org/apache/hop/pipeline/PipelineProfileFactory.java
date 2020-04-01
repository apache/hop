/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.steps.dummy.DummyMeta;
import org.apache.hop.pipeline.steps.groupby.GroupByMeta;
import org.apache.hop.pipeline.steps.tableinput.TableInputMeta;

/**
 * Helper class to generate profiling pipelines...
 *
 * @author Matt Casters (mcasters@pentaho.org)
 */
public class PipelineProfileFactory {
  public static final String RESULT_STEP_NAME = "calc stats";

  private DatabaseMeta databaseMeta;
  private String schemaTable;

  private RowMetaInterface tableLayout;

  /**
   * @param databaseMeta
   * @param schemaTable  the properly quoted schema-table combination
   */
  public PipelineProfileFactory( DatabaseMeta databaseMeta, String schemaTable ) {
    this.databaseMeta = databaseMeta;
    this.schemaTable = schemaTable;
  }

  public PipelineMeta generatePipeline( LoggingObjectInterface parentLoggingInterface ) throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();

    // Get the list of fields from the table...
    //
    tableLayout = getTableFields( parentLoggingInterface );

    // Now start building the pipeline...
    //
    PipelineMeta pipelineMeta = new PipelineMeta( databaseMeta );

    // Create a step to read the content of the table
    // Read the data from the database table...
    // For now we read it all, later we add options to only read the first X rows
    //
    TableInputMeta readMeta = new TableInputMeta();
    readMeta.setSQL( "SELECT * FROM " + schemaTable );
    readMeta.setDatabaseMeta( databaseMeta );
    StepMeta read = new StepMeta( registry.getPluginId( StepPluginType.class, readMeta ), "Read data", readMeta );
    read.setLocation( 50, 50 );
    pipelineMeta.addStep( read );

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

    // Run it through the "group by" step without a grouping.
    // Later, we can use the UnivariateStats plugin/step perhaps.
    //
    GroupByMeta statsMeta = new GroupByMeta();
    int nrNumeric = 0;
    int nrDates = 0;
    int nrStrings = 0;
    int nrBooleans = 0;
    for ( ValueMetaInterface valueMeta : tableLayout.getValueMetaList() ) {
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
    int nrCalculations =
      nrNumeric
        * numericCalculations.length + nrDates * dateCalculations.length + nrStrings
        * stringCalculations.length + nrBooleans * booleanCalculations.length;

    statsMeta.allocate( 0, nrCalculations );
    int calcIndex = 0;
    for ( int i = 0; i < tableLayout.size(); i++ ) {
      ValueMetaInterface valueMeta = tableLayout.getValueMeta( i );
      // Numeric data...
      //
      if ( valueMeta.isNumeric() ) {
        //CHECKSTYLE:Indentation:OFF
        //CHECKSTYLE:LineLength:OFF
        for ( int c = 0; c < numericCalculations.length; c++ ) {
          statsMeta.getAggregateField()[ calcIndex ] = valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( numericCalculations[ c ] ) + ")";
          statsMeta.getSubjectField()[ calcIndex ] = valueMeta.getName();
          statsMeta.getAggregateType()[ calcIndex ] = numericCalculations[ c ];
          calcIndex++;
        }
      }

      // String data
      //
      if ( valueMeta.isString() ) {
        //CHECKSTYLE:Indentation:OFF
        //CHECKSTYLE:LineLength:OFF
        for ( int c = 0; c < stringCalculations.length; c++ ) {
          statsMeta.getAggregateField()[ calcIndex ] = valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( stringCalculations[ c ] ) + ")";
          statsMeta.getSubjectField()[ calcIndex ] = valueMeta.getName();
          statsMeta.getAggregateType()[ calcIndex ] = stringCalculations[ c ];
          calcIndex++;
        }
      }

      // Date data
      //
      if ( valueMeta.isDate() ) {
        for ( int c = 0; c < dateCalculations.length; c++ ) {
          statsMeta.getAggregateField()[ calcIndex ] =
            valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( dateCalculations[ c ] ) + ")";
          statsMeta.getSubjectField()[ calcIndex ] = valueMeta.getName();
          statsMeta.getAggregateType()[ calcIndex ] = dateCalculations[ c ];
          calcIndex++;
        }
      }

      // Boolean data
      //
      if ( valueMeta.isBoolean() ) {
        for ( int c = 0; c < booleanCalculations.length; c++ ) {
          statsMeta.getAggregateField()[ calcIndex ] =
            valueMeta.getName() + "(" + GroupByMeta.getTypeDesc( booleanCalculations[ c ] ) + ")";
          statsMeta.getSubjectField()[ calcIndex ] = valueMeta.getName();
          statsMeta.getAggregateType()[ calcIndex ] = booleanCalculations[ c ];
          calcIndex++;
        }
      }
    }
    StepMeta calc = new StepMeta( registry.getPluginId( StepPluginType.class, statsMeta ), "Calc", statsMeta );
    calc.setLocation( 250, 50 );
    pipelineMeta.addStep( calc );

    PipelineHopMeta hop = new PipelineHopMeta( read, calc );
    pipelineMeta.addPipelineHop( hop );

    DummyMeta dummyMeta = new DummyMeta();
    StepMeta result =
      new StepMeta( registry.getPluginId( StepPluginType.class, dummyMeta ), RESULT_STEP_NAME, dummyMeta );
    result.setLocation( 450, 50 );
    pipelineMeta.addStep( result );

    PipelineHopMeta hop2 = new PipelineHopMeta( calc, result );
    pipelineMeta.addPipelineHop( hop2 );

    return pipelineMeta;
  }

  private RowMetaInterface getTableFields( LoggingObjectInterface parentLoggingObject ) throws HopDatabaseException {
    Database database = new Database( parentLoggingObject, databaseMeta );
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
  public RowMetaInterface getTableLayout() {
    return tableLayout;
  }
}
