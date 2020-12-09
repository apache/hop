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

package org.apache.hop.testing.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestFieldMapping;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.util.DataSetConst;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author matt
 */
@ExtensionPoint(
  extensionPointId = "PipelineStartThreads",
  id = "InjectDataSetIntoTransformExtensionPoint",
  description = "Inject a bunch of rows into a transform during preview"
)
public class InjectDataSetIntoTransformExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, final IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    if (!(pipeline instanceof LocalPipelineEngine)) {
      throw new HopPluginException( "Unit tests can only run using a local pipeline engine type" );
    }

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    boolean dataSetEnabled = "Y".equalsIgnoreCase( pipeline.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
    if ( log.isDetailed() ) {
      log.logDetailed( "Data Set enabled? " + dataSetEnabled );
    }
    if ( !dataSetEnabled ) {
      return;
    }

    String unitTestName = pipeline.getVariable( DataSetConst.VAR_UNIT_TEST_NAME );
    if ( log.isDetailed() ) {
      log.logDetailed( "Unit test name: " + unitTestName );
    }
    try {
      IHopMetadataProvider metadataProvider = pipelineMeta.getMetadataProvider();

      // If the pipeline has a variable set with the unit test in it, we're dealing with a unit test situation.
      //
      if ( StringUtil.isEmpty( unitTestName ) ) {
        return;
      }
      PipelineUnitTest unitTest = metadataProvider.getSerializer( PipelineUnitTest.class).load( unitTestName );
      if ( unitTest == null ) {
        if ( log.isDetailed() ) {
          log.logDetailed( "Unit test '" + unitTestName + "' could not be found" );
        }
        return;
      }

      // Replace all transforms with input data sets with Injector transforms.
      // Replace all transforms with a golden data set, attached to a unit test, with a Dummy
      // Apply tweaks
      //
      for ( final TransformMeta transformMeta : pipeline.getPipelineMeta().getTransforms() ) {
        String transformName = transformMeta.getName();
        PipelineUnitTestSetLocation inputLocation = unitTest.findInputLocation( transformName );
        if ( inputLocation != null && StringUtils.isNotEmpty( inputLocation.getDataSetName() ) ) {
          String inputDataSetName = inputLocation.getDataSetName();
          log.logDetailed( "Data Set location found for transform '" + transformName + "' and data set  " + inputDataSetName );

          // We need to inject data from the data set with the specified name into the transform
          //
          injectDataSetIntoTransform( (LocalPipelineEngine)pipeline, inputDataSetName, metadataProvider, transformMeta, inputLocation );
        }

        // How about capturing rows for golden data review?
        //
        PipelineUnitTestSetLocation goldenLocation = unitTest.findGoldenLocation( transformName );
        if ( goldenLocation != null ) {
          String goldenDataSetName = goldenLocation.getDataSetName();
          if ( !StringUtil.isEmpty( goldenDataSetName ) ) {

            log.logDetailed( "Capturing rows for validation at pipeline end, transform='" + transformMeta.getName() + "', golden set '" + goldenDataSetName );

            final RowCollection rowCollection = new RowCollection();

            // Create a row collection map if it's missing...
            //
            @SuppressWarnings( "unchecked" )
            Map<String, RowCollection> collectionMap = (Map<String, RowCollection>) pipeline.getExtensionDataMap().get( DataSetConst.ROW_COLLECTION_MAP );
            if ( collectionMap == null ) {
              collectionMap = new HashMap<>();
              pipeline.getExtensionDataMap().put( DataSetConst.ROW_COLLECTION_MAP, collectionMap );
            }

            // Keep the map for safe keeping...
            //
            collectionMap.put( transformMeta.getName(), rowCollection );

            // We'll capture the rows from this one and then evaluate them after execution...
            //
            IEngineComponent component = pipeline.findComponent( transformMeta.getName(), 0 );
            component.addRowListener( new RowAdapter() {
              @Override
              public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                if ( rowCollection.getRowMeta() == null ) {
                  rowCollection.setRowMeta( rowMeta );
                }
                rowCollection.getRows().add( row );
              }
            } );
          }
        }

      }
    } catch ( Throwable e ) {
      throw new HopException( "Unable to inject data set rows", e );
    }

  }

  private void injectDataSetIntoTransform( final LocalPipelineEngine pipeline, final String dataSetName,
                                           final IHopMetadataProvider metadataProvider, final TransformMeta transformMeta,
                                           PipelineUnitTestSetLocation inputLocation ) throws HopException, HopException {

    final DataSet dataSet = metadataProvider.getSerializer( DataSet.class).load( dataSetName );
    if (dataSet==null) {
      throw new HopException("Unable to find data set '"+dataSetName+"'");
    }

    final ILogChannel log = pipeline.getLogChannel();
    final RowProducer rowProducer = pipeline.addRowProducer( transformMeta.getName(), 0 );

    // Look for the transform into which we'll inject rows...
    //
    TransformMetaDataCombi combi = null;
    for ( TransformMetaDataCombi transform : pipeline.getTransforms() ) {
      if ( transform.transformName.equals( transformMeta.getName() ) ) {
        combi = transform;
        break;
      }
    }

    if ( combi != null ) {

      // Get the rows of the mapped values in the mapped order sorted as asked
      //
      final List<Object[]> dataSetRows = dataSet.getAllRows( pipeline, log, inputLocation );
      IRowMeta dataSetRowMeta = dataSet.getMappedDataSetFieldsRowMeta( inputLocation );

      // The rows to inject are always driven by the dataset, NOT the transform it replaces (!) for simplicity
      //
      IRowMeta injectRowMeta = new RowMeta();

      // Figure out which fields to pass
      // Only inject those mentioned in the field mappings...
      //
      int[] fieldIndexes = new int[ inputLocation.getFieldMappings().size() ];
      for ( int i = 0; i < inputLocation.getFieldMappings().size(); i++ ) {
        PipelineUnitTestFieldMapping fieldMapping = inputLocation.getFieldMappings().get( i );
        fieldIndexes[ i ] = dataSetRowMeta.indexOfValue( fieldMapping.getDataSetFieldName() );
        if ( fieldIndexes[ i ] < 0 ) {
          throw new HopException( "Unable to find mapped field '" + fieldMapping.getDataSetFieldName() + "' in data set '" + dataSet.getName() + "'" );
        }
        IValueMeta injectValueMeta = dataSetRowMeta.getValueMeta( fieldIndexes[ i ] ).clone();
        // Rename to the transform output names though...
        //
        injectValueMeta.setName( fieldMapping.getTransformFieldName() );
        injectRowMeta.addValueMeta( injectValueMeta );
      }

      log.logDetailed( "Injecting data set '" + dataSetName + "' into transform '" + transformMeta.getName() + "', fields: " + Arrays.toString( injectRowMeta.getFieldNames() ) );

      // Pass rows
      //
      Runnable runnable = () -> {
        try {

          for ( Object[] dataSetRow : dataSetRows ) {
            // pass the row with the external names, in the right order and with the selected columns from the data set
            //
            Object[] row = RowDataUtil.allocateRowData( injectRowMeta.size() );
            for ( int i = 0; i < fieldIndexes.length; i++ ) {
              row[ i ] = dataSetRow[ fieldIndexes[ i ] ];
            }
            rowProducer.putRow( injectRowMeta, row );
          }
          rowProducer.finished();

        } catch ( Exception e ) {
          throw new RuntimeException( "Problem injecting data set '" + dataSetName + "' row into transform '" + transformMeta.getName() + "'", e );
        }
      };
      Thread thread = new Thread( runnable );
      thread.start();


    }
  }
}
