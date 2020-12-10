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

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestDatabaseReplacement;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.PipelineUnitTestTweak;
import org.apache.hop.testing.util.DataSetConst;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class PipelineMetaModifier {

  private final IVariables variables;
  private final PipelineMeta pipelineMeta;
  private final PipelineUnitTest unitTest;

  public PipelineMetaModifier( IVariables variables, PipelineMeta pipelineMeta, PipelineUnitTest unitTest ) {
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.unitTest = unitTest;
  }

  public PipelineMeta getTestPipeline( ILogChannel log, IVariables variables, IHopMetadataProvider metadataProvider ) throws HopException {
    // OK, so now replace an input transform with a data set attached with an Injector transform...
    // However, we don't want to have the user see this so we need to copy pipeline.pipelineMeta first...
    //
    // Clone seems to has problems so we'll take the long (XML) way around...
    //
    InputStream stream;
    try {
      stream = new ByteArrayInputStream( pipelineMeta.getXml().getBytes( Const.XML_ENCODING ) );
    } catch ( UnsupportedEncodingException e ) {
      throw new HopException( "Encoding error", e );
    }
    PipelineMeta copyPipelineMeta = new PipelineMeta( stream, metadataProvider, true, variables );

    // Pass the metadata references...
    //
    copyPipelineMeta.setMetadataProvider( pipelineMeta.getMetadataProvider() );

    // Replace certain connections with another
    //
    for ( PipelineUnitTestDatabaseReplacement dbReplacement : unitTest.getDatabaseReplacements() ) {
      String sourceDatabaseName = variables.resolve( dbReplacement.getOriginalDatabaseName() );
      String replacementDatabaseName = variables.resolve( dbReplacement.getReplacementDatabaseName() );

      DatabaseMeta sourceDatabaseMeta = copyPipelineMeta.findDatabase( sourceDatabaseName );
      DatabaseMeta replacementDatabaseMeta = copyPipelineMeta.findDatabase( replacementDatabaseName );
      if ( sourceDatabaseMeta == null ) {
        throw new HopException( "Unable to find source database connection '" + sourceDatabaseName + "', can not be replaced" );
      }
      if ( replacementDatabaseMeta == null ) {
        throw new HopException( "Unable to find replacement database connection '" + replacementDatabaseName + "', can not be used to replace" );
      }

      if ( log.isDetailed() ) {
        log.logDetailed( "Replaced database connection '" + sourceDatabaseName + "' with connection '" + replacementDatabaseName + "'" );
      }
      sourceDatabaseMeta.replaceMeta( replacementDatabaseMeta );
    }

    // Replace all transforms with an Input Data Set marker with an Injector
    // Replace all transforms with a Golden Data Set marker with a Dummy
    // Apply the tweaks to the transforms:
    //   - Bypass : replace with Dummy
    //   - Remove : remove transform and all connected hops.
    //
    // Loop over the original pipeline to allow us to safely modify the copy
    //
    List<TransformMeta> transforms = pipelineMeta.getTransforms();
    for ( TransformMeta transform : transforms ) {
      TransformMeta transformMeta = copyPipelineMeta.findTransform( transform.getName() );
      PipelineUnitTestSetLocation inputLocation = unitTest.findInputLocation( transformMeta.getName() );
      PipelineUnitTestSetLocation goldenLocation = unitTest.findGoldenLocation( transformMeta.getName() );
      PipelineUnitTestTweak transformTweak = unitTest.findTweak( transformMeta.getName() );

      // See if there's a unit test if the transform isn't flagged...
      //
      if ( inputLocation != null ) {
        handleInputDataSet( log, inputLocation, unitTest, pipelineMeta, transformMeta, metadataProvider );
      }

      // Capture golden data in a dummy transform instead of the regular one?
      //
      if ( goldenLocation != null ) {
        handleGoldenDataSet( log, goldenLocation, transformMeta, metadataProvider );
      }

      if ( transformTweak != null && transformTweak.getTweak() != null ) {
        switch ( transformTweak.getTweak() ) {
          case NONE:
            break;
          case REMOVE_TRANSFORM:
            handleTweakRemoveTransform( log, copyPipelineMeta, transformMeta );
            break;
          case BYPASS_TRANSFORM:
            handleTweakBypassTransform( log, transformMeta );
            break;
          default:
            break;
        }
      }
    }

    return copyPipelineMeta;
  }

  private void handleInputDataSet( ILogChannel log, PipelineUnitTestSetLocation inputLocation, PipelineUnitTest unitTest,
                                   PipelineMeta pipelineMeta, TransformMeta transformMeta, IHopMetadataProvider metadataProvider ) throws HopException {

    String inputSetName = inputLocation.getDataSetName();

    if ( log.isDetailed() ) {
      log.logDetailed( "Replacing transform '" + transformMeta.getName() + "' with an Injector for dataset '" + inputSetName + "'" );
    }

    DataSet dataSet;
    try {
      dataSet = metadataProvider.getSerializer( DataSet.class).load( inputSetName );
    } catch ( HopException e ) {
      throw new HopException( "Unable to load data set '" + inputSetName + "'" );
    }

    // OK, this transform needs to be replaced by an Injector transform...
    // Which fields do we need to use?
    //
    final IRowMeta transformFields = DataSetConst.getTransformOutputFields( dataSet, inputLocation );

    if ( log.isDetailed() ) {
      log.logDetailed( "Input Data Set '" + inputSetName + "' Injector fields : '" + transformFields.toString() );
    }

    InjectorMeta injectorMeta = new InjectorMeta();
    injectorMeta.allocate( transformFields.size() );
    for ( int x = 0; x < transformFields.size(); x++ ) {
      injectorMeta.getFieldname()[ x ] = transformFields.getValueMeta( x ).getName();
      injectorMeta.getType()[ x ] = transformFields.getValueMeta( x ).getType();
      injectorMeta.getLength()[ x ] = transformFields.getValueMeta( x ).getLength();
      injectorMeta.getPrecision()[ x ] = transformFields.getValueMeta( x ).getPrecision();

      // Only the transform metadata, type...
      //
      transformMeta.setTransform( injectorMeta );
      transformMeta.setTransformPluginId( PluginRegistry.getInstance().getPluginId( TransformPluginType.class, injectorMeta ) );
    }
  }

  private void handleGoldenDataSet( ILogChannel log, PipelineUnitTestSetLocation goldenSetName, TransformMeta transformMeta, IHopMetadataProvider metadataProvider ) {

    if ( log.isDetailed() ) {
      log.logDetailed( "Replacing transform '" + transformMeta.getName() + "' with an Dummy for golden dataset '" + goldenSetName + "'" );
    }

    replaceTransformWithDummy( log, transformMeta );
  }

  private void replaceTransformWithDummy( ILogChannel log, TransformMeta transformMeta ) {
    DummyMeta dummyTransformMeta = new DummyMeta();
    transformMeta.setTransform( dummyTransformMeta );
    transformMeta.setTransformPluginId( PluginRegistry.getInstance().getPluginId( TransformPluginType.class, dummyTransformMeta ) );
  }

  private void handleTweakBypassTransform( ILogChannel log, TransformMeta transformMeta ) {
    if ( log.isDetailed() ) {
      log.logDetailed( "Replacing transform '" + transformMeta.getName() + "' with an Dummy for Bypass transform tweak" );
    }

    replaceTransformWithDummy( log, transformMeta );
  }

  private void handleTweakRemoveTransform( ILogChannel log, PipelineMeta copyPipelineMeta, TransformMeta transformMeta ) {
    if ( log.isDetailed() ) {
      log.logDetailed( "Removing transform '" + transformMeta.getName() + "' for Remove transform tweak" );
    }

    // Remove all hops connecting to the transform to be removed...
    //
    List<TransformMeta> prevTransforms = copyPipelineMeta.findPreviousTransforms( transformMeta );
    for ( TransformMeta prevTransform : prevTransforms ) {
      PipelineHopMeta hop = copyPipelineMeta.findPipelineHop( prevTransform, transformMeta );
      if ( hop != null ) {
        int hopIndex = copyPipelineMeta.indexOfPipelineHop( hop );
        copyPipelineMeta.removePipelineHop( hopIndex );
      }
    }
    List<TransformMeta> nextTransforms = copyPipelineMeta.findNextTransforms( transformMeta );
    for ( TransformMeta nextTransform : nextTransforms ) {
      PipelineHopMeta hop = copyPipelineMeta.findPipelineHop( transformMeta, nextTransform );
      if ( hop != null ) {
        int hopIndex = copyPipelineMeta.indexOfPipelineHop( hop );
        copyPipelineMeta.removePipelineHop( hopIndex );
      }
    }

    int idx = copyPipelineMeta.indexOfTransform( transformMeta );
    if ( idx >= 0 ) {
      copyPipelineMeta.removeTransform( idx );
    }
  }


  /**
   * Gets pipelineMeta
   *
   * @return value of pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }


  /**
   * Gets unitTest
   *
   * @return value of unitTest
   */
  public PipelineUnitTest getUnitTest() {
    return unitTest;
  }
}
