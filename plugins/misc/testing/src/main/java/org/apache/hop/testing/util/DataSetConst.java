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

package org.apache.hop.testing.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.PipelineTweak;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.TestType;
import org.apache.hop.testing.PipelineUnitTestFieldMapping;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.testing.xp.RowCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataSetConst {
  private static final Class<?> PKG = DataSetConst.class; // For Translator

  public static final String DATABASE_FACTORY_KEY = "DatabaseMetaFactory";
  public static final String GROUP_FACTORY_KEY = "DataSetGroupFactory";
  public static final String SET_FACTORY_KEY = "DataSetFactory";

  // Variables during execution to indicate the selected test to run
  //
  public static final String VAR_RUN_UNIT_TEST = "__UnitTest_Run__";
  public static final String VAR_UNIT_TEST_NAME = "__UnitTest_Name__";
  public static final String VAR_WRITE_TO_DATASET = "__UnitTest_WriteDataSet__";
  public static final String VAR_DO_NOT_SHOW_UNIT_TEST_ERRORS = "__UnitTest_DontShowUnitTestErrors__";

  public static final String AREA_DRAWN_UNIT_TEST_ICON = "Drawn_UnitTestIcon";
  public static final String AREA_DRAWN_UNIT_TEST_NAME = "Drawn_UnitTestName";
  public static final String AREA_DRAWN_INPUT_DATA_SET = "Input_DataSet";
  public static final String AREA_DRAWN_GOLDEN_DATA_SET = "Golden_DataSet";


  public static final String ROW_COLLECTION_MAP = "RowCollectionMap";
  public static final String UNIT_TEST_RESULTS = "UnitTestResults";

  public static final String VARIABLE_HOP_UNIT_TESTS_FOLDER = "HOP_UNIT_TESTS_FOLDER";

  private static final String[] tweakDesc = new String[] {
    BaseMessages.getString( PKG, "DataSetConst.Tweak.NONE.Desc" ),
    BaseMessages.getString( PKG, "DataSetConst.Tweak.BYPASS_TRANSFORM.Desc" ),
    BaseMessages.getString( PKG, "DataSetConst.Tweak.REMOVE_TRANSFORM.Desc" ),
  };

  private static final String[] testTypeDesc = new String[] {
    BaseMessages.getString( PKG, "DataSetConst.TestType.DEVELOPMENT.Desc" ),
    BaseMessages.getString( PKG, "DataSetConst.TestType.UNIT_TEST.Desc" ),
  };

  /**
   * Validate the execution results of a pipeline against the golden data sets of a unit test.
   *
   * @param pipeline     The pipeline after execution
   * @param unitTest  The unit test
   * @param metadataProvider The MetaStore to use
   * @param results   The results list to add comments to
   * @return The nr of errors, 0 if no errors found
   * @throws HopException In case there was an error loading data or metadata.
   */
  public static final int validateTransResultAgainstUnitTest( IPipelineEngine<PipelineMeta> pipeline, PipelineUnitTest unitTest, IHopMetadataProvider metadataProvider, List<UnitTestResult> results ) throws HopException {
    int nrErrors = 0;

    ILogChannel log = pipeline.getLogChannel();

    @SuppressWarnings( "unchecked" )
    Map<String, RowCollection> collectionMap = (Map<String, RowCollection>) pipeline.getExtensionDataMap().get( DataSetConst.ROW_COLLECTION_MAP );
    if ( collectionMap == null ) {

      String comment = "No transform output result data found to validate against";
      results.add( new UnitTestResult( pipeline.getPipelineMeta().getName(), unitTest.getName(), null, null, false, comment ) );
      return nrErrors;
    }

    for ( PipelineUnitTestSetLocation location : unitTest.getGoldenDataSets() ) {

      // Sometimes we deleted a transform and it's still in the list:
      // Simply skip that one
      //
      if ( pipeline.getPipelineMeta().findTransform( location.getTransformName() ) == null ) {
        continue;
      }

      int nrLocationErrors = 0;
      RowCollection resultCollection = collectionMap.get( location.getTransformName() );
      if ( resultCollection == null || resultCollection.getRows() == null || resultCollection.getRowMeta() == null ) {
        // error occurred somewhere, we don't have results, provide dummy values to avoid exceptions, flag error
        //
        resultCollection = new RowCollection();
        resultCollection.setRowMeta( new RowMeta() );
        resultCollection.setRows( new ArrayList<>() );

        String comment = "WARNING: no test results found for transform '" + location.getTransformName() + "' : check disabled hops, input and so on.";
        results.add( new UnitTestResult(
          pipeline.getPipelineMeta().getName(), unitTest.getName(), location.getDataSetName(), location.getTransformName(),
          false, comment ) );
      }
      final IRowMeta resultRowMeta = resultCollection.getRowMeta();

      log.logDetailed( "Found " + resultCollection.getRows().size() + " results for data comparing in transform '" + location.getTransformName() + "', fields: " + resultRowMeta.toString() );

      DataSet goldenDataSet = unitTest.getGoldenDataSet( log, metadataProvider, location );
      List<Object[]> goldenRows = goldenDataSet.getAllRows( pipeline, log, location );
      IRowMeta goldenRowMeta = goldenDataSet.getMappedDataSetFieldsRowMeta( location );

      log.logDetailed( "Found " + goldenRows.size() + " golden rows '" + location.getTransformName() + "', fields: " + goldenRowMeta );

      List<Object[]> resultRows = resultCollection.getRows();

      if ( resultRows.size() != goldenRows.size() ) {
        String comment =
          "Incorrect number of rows received from transform, golden data set '" + location.getDataSetName() + "' has " + goldenRows.size() + " rows in it and we received " + resultRows.size();
        results.add( new UnitTestResult(
          pipeline.getPipelineMeta().getName(), unitTest.getName(), location.getDataSetName(), location.getTransformName(),
          true, comment ) );
        nrLocationErrors++;
      } else {

        // To compare the 2 data sets they need to be explicitly sorted on the same keys
        // The added problem is that the user provided a field mapping.
        // So for every "location field order" we need to find the transform source field
        //
        // Sort transform result rows
        //
        final int[] resultFieldIndexes = new int[ location.getFieldOrder().size() ];
        for ( int i = 0; i < resultFieldIndexes.length; i++ ) {
          String dataSetOrderField = location.getFieldOrder().get( i );
          String transformOrderField = location.findTransformField( dataSetOrderField );
          if ( transformOrderField == null ) {
            throw new HopException( "There is no transform field provided in the mappings so I don't know which field to use to sort '" + dataSetOrderField + "'" );
          }
          resultFieldIndexes[ i ] = resultRowMeta.indexOfValue( transformOrderField );
          if ( resultFieldIndexes[ i ] < 0 ) {
            throw new HopException( "Unable to find sort field '" + transformOrderField + "' in transform results : " + Arrays.toString( resultRowMeta.getFieldNames() ) );
          }
        }
        try {
          log.logDetailed( "Sorting result rows collection on fields: " + location.getFieldOrder() );
          resultCollection.getRows().sort( ( row1, row2 ) -> {
            try {
              return resultRowMeta.compare( row1, row2, resultFieldIndexes );
            } catch ( HopValueException e ) {
              throw new RuntimeException( "Error comparing golden data result rows", e );
            }
          } );
        } catch ( RuntimeException e ) {
          throw new HopException( "Error sorting result rows for golden data set '" + location.getDataSetName() + "'", e );
        }

        // Print the first 10 result rows
        //
        if ( log.isDebug() ) {
          for ( int i = 0; i < 10 && i < resultCollection.getRows().size(); i++ ) {
            log.logDetailed( "Result row #" + ( i + 1 ) + " : " + resultRowMeta.getString( resultCollection.getRows().get( i ) ) );
          }
        }


        // Golden rows
        //
        final int[] goldenFieldIndexes = new int[ location.getFieldOrder().size() ];
        for ( int i = 0; i < goldenFieldIndexes.length; i++ ) {
          goldenFieldIndexes[ i ] = goldenRowMeta.indexOfValue( location.getFieldOrder().get( i ) );
          if ( goldenFieldIndexes[ i ] < 0 ) {
            throw new HopException( "Unable to find sort field '" + location.getFieldOrder().get( i ) + "' in golden rows : " + Arrays.toString( goldenRowMeta.getFieldNames() ) );
          }
        }
        try {
          log.logDetailed( "Sorting golden rows collection on fields: " + location.getFieldOrder() );

          goldenRows.sort( ( row1, row2 ) -> {
            try {
              return goldenRowMeta.compare( row1, row2, goldenFieldIndexes );
            } catch ( HopValueException e ) {
              throw new RuntimeException( "Error comparing golden data set rows", e );
            }
          } );
        } catch ( RuntimeException e ) {
          throw new HopException( "Error sorting golden data rows for golden data set '" + location.getDataSetName() + "'", e );
        }

        // Print the first 10 golden rows
        //
        if ( log.isDebug() ) {
          for ( int i = 0; i < 10 && i < goldenRows.size(); i++ ) {
            log.logDetailed( "Golden row #" + ( i + 1 ) + " : " + goldenRowMeta.getString( goldenRows.get( i ) ) );
          }
        }

        if ( nrLocationErrors == 0 ) {
          final int[] transformFieldIndices = new int[ location.getFieldMappings().size() ];
          final int[] goldenIndices = new int[ location.getFieldMappings().size() ];
          for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
            PipelineUnitTestFieldMapping fieldMapping = location.getFieldMappings().get( i );

            transformFieldIndices[ i ] = resultRowMeta.indexOfValue( fieldMapping.getTransformFieldName() );
            if (transformFieldIndices[i]<0) {
              throw new HopException( "Unable to find output field '" + fieldMapping.getTransformFieldName() + "' while testing output of transform '" + location.getTransformName()+"'" );
            }
            goldenIndices[ i ] = goldenRowMeta.indexOfValue( fieldMapping.getDataSetFieldName() );
            if (goldenIndices[i]<0) {
              throw new HopException( "Unable to find golden data set field '" + fieldMapping.getDataSetFieldName() + "' while testing output of transform '" + location.getTransformName()+"'" );
            }
            log.logDetailed( "Field to compare #" + i + " found on transform index : " + transformFieldIndices[ i ] + ", golden index : " + goldenIndices[ i ] );
          }

          for ( int rowNumber = 0; rowNumber < resultRows.size(); rowNumber++ ) {
            Object[] resultRow = resultRows.get( rowNumber );
            Object[] goldenRow = goldenRows.get( rowNumber );

            // Now compare the input to the golden row
            //
            for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
              IValueMeta transformValueMeta = resultCollection.getRowMeta().getValueMeta( transformFieldIndices[ i ] );
              Object transformValue = resultRow[ transformFieldIndices[ i ] ];

              IValueMeta goldenValueMeta = goldenRowMeta.getValueMeta( goldenIndices[ i ] );
              Object goldenValue = goldenRow[ goldenIndices[ i ] ];

              if ( log.isDetailed() ) {
                log.logDebug( "Comparing Meta '" + transformValueMeta.toString() + "' with '" + goldenValueMeta.toString() + "'" );
                log.logDebug( "Comparing Value '" + transformValue + "' with '" + goldenValue + "'" );
              }

              Object goldenValueConverted;

              // sometimes there are data conversion issues because of the the database...
              //
              if ( goldenValueMeta.getType() == transformValueMeta.getType() ) {
                goldenValueConverted = goldenValue;
              } else {
                goldenValueConverted = transformValueMeta.convertData( goldenValueMeta, goldenValue );
              }

              try {
                int cmp = transformValueMeta.compare( transformValue, transformValueMeta, goldenValueConverted );
                if ( cmp != 0 ) {
                  if ( log.isDebug() ) {
                    log.logDebug( "Unit test failure: '" + transformValue + "' <> '" + goldenValue + "'" );
                  }
                  String comment = "Validation againt golden data failed for row number " + ( rowNumber + 1 )
                    + ": transform value [" + transformValueMeta.getString( transformValue )
                    + "] does not correspond to data set value [" + goldenValueMeta.getString( goldenValue ) + "]";
                  results.add( new UnitTestResult(
                    pipeline.getPipelineMeta().getName(), unitTest.getName(), location.getDataSetName(), location.getTransformName(),
                    true, comment ) );
                  nrLocationErrors++;
                }
              } catch ( HopValueException e ) {
                throw new HopException( "Unable to compare transform data against golden data set '" + location.getDataSetName() + "'", e );
              }
            }
          }
        }

        if ( nrLocationErrors == 0 ) {
          String comment = "Test passed succesfully against golden data set";
          results.add( new UnitTestResult(
            pipeline.getPipelineMeta().getName(), unitTest.getName(), location.getDataSetName(), location.getTransformName(),
            false, comment ) );
        } else {
          nrErrors += nrLocationErrors;
        }
      }
    }

    if ( nrErrors == 0 ) {
      String comment = "Test passed succesfully against unit test";
      results.add( new UnitTestResult(
        pipeline.getPipelineMeta().getName(), unitTest.getName(), null, null,
        false, comment ) );

    }
    return nrErrors;
  }

  public static final String getDirectoryFromPath( String path ) {
    int lastSlashIndex = path.lastIndexOf( '/' );
    if ( lastSlashIndex >= 0 ) {
      return path.substring( 0, lastSlashIndex );
    } else {
      return "/";
    }
  }

  public static final String getNameFromPath( String path ) {
    int lastSlashIndex = path.lastIndexOf( '/' );
    if ( lastSlashIndex >= 0 ) {
      return path.substring( lastSlashIndex + 1 );
    } else {
      return path;
    }
  }

  public static IRowMeta getTransformOutputFields( DataSet dataSet, PipelineUnitTestSetLocation inputLocation ) throws HopException {
    IRowMeta dataSetRowMeta = dataSet.getSetRowMeta( );
    IRowMeta outputRowMeta = new RowMeta();

    for ( int i = 0; i < inputLocation.getFieldMappings().size(); i++ ) {
      PipelineUnitTestFieldMapping fieldMapping = inputLocation.getFieldMappings().get( i );
      IValueMeta injectValueMeta = dataSetRowMeta.searchValueMeta( fieldMapping.getDataSetFieldName() );
      if ( injectValueMeta == null ) {
        throw new HopException( "Unable to find mapped field '" + fieldMapping.getDataSetFieldName() + "' in data set '" + dataSet.getName() + "'" );
      }
      // Rename to the transform output names though...
      //
      injectValueMeta.setName( fieldMapping.getTransformFieldName() );
      outputRowMeta.addValueMeta( injectValueMeta );
    }

    return outputRowMeta;
  }

  /**
   * Get the PipelineTweak for a tweak description (from the dialog)
   *
   * @param tweakDescription The description to look for
   * @return the tweak or NONE if nothing matched
   */
  public PipelineTweak getTweakForDescription( String tweakDescription ) {
    if ( StringUtils.isEmpty( tweakDescription ) ) {
      return PipelineTweak.NONE;
    }
    int index = Const.indexOfString( tweakDescription, tweakDesc );
    if ( index < 0 ) {
      return PipelineTweak.NONE;
    }
    return PipelineTweak.values()[ index ];
  }

  public static final String getTestTypeDescription( TestType testType ) {
    int index = 0; // DEVELOPMENT
    if ( testType != null ) {
      TestType[] testTypes = TestType.values();
      for ( int i = 0; i < testTypes.length; i++ ) {
        if ( testTypes[ i ] == testType ) {
          index = i;
          break;
        }
      }
    }

    return testTypeDesc[ index ];
  }

  /**
   * Get the TestType for a tweak description (from the dialog)
   *
   * @param testTypeDescription The description to look for
   * @return the test type or NONE if nothing matched
   */
  public static final TestType getTestTypeForDescription( String testTypeDescription ) {
    if ( StringUtils.isEmpty( testTypeDescription ) ) {
      return TestType.DEVELOPMENT;
    }
    int index = Const.indexOfString( testTypeDescription, testTypeDesc );
    if ( index < 0 ) {
      return TestType.DEVELOPMENT;
    }
    return TestType.values()[ index ];
  }

  public static final String[] getTestTypeDescriptions() {
    return testTypeDesc;
  }

}
