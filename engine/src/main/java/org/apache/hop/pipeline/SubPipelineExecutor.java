/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.PipelineTransformUtil;
import org.apache.hop.pipeline.transforms.pipelineexecutor.PipelineExecutorParameters;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Will run the given sub- pipeline with the rows passed to execute
 */
public class SubPipelineExecutor {
  private static final Class<?> PKG = SubPipelineExecutor.class;
  private final Map<String, TransformStatus> statuses;
  private final String subPipelineName;
  private Pipeline parentPipeline;
  private PipelineMeta subPipelineMeta;
  private boolean shareVariables;
  private PipelineExecutorParameters parameters;
  private String subTransform;
  private boolean stopped;
  Set<Pipeline> running;

  public SubPipelineExecutor( String subPipelineName, Pipeline parentPipeline, PipelineMeta subPipelineMeta, boolean shareVariables,
                              PipelineExecutorParameters parameters, String subTransform ) {
    this.subPipelineName = subPipelineName;
    this.parentPipeline = parentPipeline;
    this.subPipelineMeta = subPipelineMeta;
    this.shareVariables = shareVariables;
    this.parameters = parameters;
    this.subTransform = subTransform;
    this.statuses = new LinkedHashMap<>();
    this.running = new ConcurrentHashSet<>();
  }

  public Optional<Result> execute( List<RowMetaAndData> rows ) throws HopException {
    if ( rows.isEmpty() || stopped ) {
      return Optional.empty();
    }

    Pipeline subPipeline = this.createSubPipeline();
    running.add( subPipeline );
    parentPipeline.addActiveSubPipeline( subPipelineName, subPipeline );

    // Pass parameter values
    passParametersToPipeline( subPipeline, rows.get( 0 ) );

    Result result = new Result();
    result.setRows( rows );
    subPipeline.setPreviousResult( result );

    subPipeline.prepareExecution();
    List<RowMetaAndData> rowMetaAndData = new ArrayList<>();
    subPipeline.getTransforms().stream()
      .filter( c -> c.transform.getTransformName().equalsIgnoreCase( subTransform ) )
      .findFirst()
      .ifPresent( c -> c.transform.addRowListener( new RowAdapter() {
        @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) {
          rowMetaAndData.add( new RowMetaAndData( rowMeta, row ) );
        }
      } ) );
    subPipeline.startThreads();

    subPipeline.waitUntilFinished();
    updateStatuses( subPipeline );
    running.remove( subPipeline );

    Result subpipelineResult = subPipeline.getResult();
    subpipelineResult.setRows( rowMetaAndData );
    return Optional.of( subpipelineResult );
  }

  private synchronized void updateStatuses( Pipeline subPipeline ) {
    List<TransformMetaDataCombi<TransformInterface, TransformMetaInterface, TransformDataInterface>> transforms =
      subPipeline.getTransforms();
    for ( TransformMetaDataCombi combi : transforms ) {
      TransformStatus transformStatus;
      if ( statuses.containsKey( combi.transformName ) ) {
        transformStatus = statuses.get( combi.transformName );
        transformStatus.updateAll( combi.transform );
      } else {
        transformStatus = new TransformStatus( combi.transform );
        statuses.put( combi.transformName, transformStatus );
      }

      transformStatus.setStatusDescription( BaseTransformData.TransformExecutionStatus.STATUS_RUNNING.getDescription() );
    }
  }

  private Pipeline createSubPipeline() {
    Pipeline subPipeline = new Pipeline( this.subPipelineMeta, this.parentPipeline );
    subPipeline.setParentPipeline( this.parentPipeline );
    subPipeline.setLogLevel( this.parentPipeline.getLogLevel() );
    if ( this.shareVariables ) {
      subPipeline.shareVariablesWith( this.parentPipeline );
    }

    subPipeline.setInternalHopVariables( this.parentPipeline );
    subPipeline.copyParametersFrom( this.subPipelineMeta );
    subPipeline.setPreview( this.parentPipeline.isPreview() );
    PipelineTransformUtil.initServletConfig( this.parentPipeline, subPipeline );
    return subPipeline;
  }

  private void passParametersToPipeline( Pipeline internalPipeline, RowMetaAndData rowMetaAndData ) throws HopException {
    internalPipeline.clearParameters();
    String[] parameterNames = internalPipeline.listParameters();

    for ( int i = 0; i < this.parameters.getVariable().length; ++i ) {
      String variable = this.parameters.getVariable()[ i ];
      String fieldName = this.parameters.getField()[ i ];
      String inputValue = this.parameters.getInput()[ i ];
      String value;
      if ( !Utils.isEmpty( fieldName ) ) {
        int idx = rowMetaAndData.getRowMeta().indexOfValue( fieldName );
        if ( idx < 0 ) {
          throw new HopException(
            BaseMessages.getString( PKG, "PipelineExecutor.Exception.UnableToFindField", fieldName ) );
        }

        value = rowMetaAndData.getString( idx, "" );
      } else {
        value = this.parentPipeline.environmentSubstitute( inputValue );
      }

      if ( Const.indexOfString( variable, parameterNames ) < 0 ) {
        internalPipeline.setVariable( variable, Const.NVL( value, "" ) );
      } else {
        internalPipeline.setParameterValue( variable, Const.NVL( value, "" ) );
      }
    }

    internalPipeline.activateParameters();
  }

  public void stop() {
    stopped = true;
    for ( Pipeline subPipeline : running ) {
      subPipeline.stopAll();
    }
    running.clear();
    for ( Map.Entry<String, TransformStatus> entry : statuses.entrySet() ) {
      entry.getValue().setStatusDescription( BaseTransformData.TransformExecutionStatus.STATUS_STOPPED.getDescription() );
    }
  }

  public Map<String, TransformStatus> getStatuses() {
    return statuses;
  }

  public Pipeline getParentPipeline() {
    return parentPipeline;
  }
}
