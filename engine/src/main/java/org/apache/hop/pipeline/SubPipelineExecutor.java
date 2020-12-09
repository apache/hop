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

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformStatus;
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
  private IPipelineEngine<PipelineMeta> parentPipeline;
  private PipelineMeta subPipelineMeta;
  private boolean shareVariables;
  private PipelineExecutorParameters parameters;
  private String subTransform;
  private boolean stopped;
  private Set<IPipelineEngine<PipelineMeta>> running;
  private PipelineRunConfiguration runConfiguration;

  public SubPipelineExecutor( String subPipelineName, IPipelineEngine<PipelineMeta> parentPipeline, PipelineMeta subPipelineMeta, boolean shareVariables,
                              PipelineExecutorParameters parameters, String subTransform, PipelineRunConfiguration runConfiguration ) {
    this.subPipelineName = subPipelineName;
    this.parentPipeline = parentPipeline;
    this.subPipelineMeta = subPipelineMeta;
    this.shareVariables = shareVariables;
    this.parameters = parameters;
    this.subTransform = subTransform;
    this.statuses = new LinkedHashMap<>();
    this.running = new ConcurrentHashSet<>();
    this.runConfiguration = runConfiguration;
  }

  public Optional<Result> execute( List<RowMetaAndData> rows ) throws HopException {
    if ( rows.isEmpty() || stopped ) {
      return Optional.empty();
    }

    IPipelineEngine<PipelineMeta> subPipeline = this.createSubPipeline();
    running.add( subPipeline );
    parentPipeline.addActiveSubPipeline( subPipelineName, subPipeline );

    // Pass parameter values
    passParametersToPipeline( subPipeline, rows.get( 0 ) );

    Result result = new Result();
    result.setRows( rows );
    subPipeline.setPreviousResult( result );

    subPipeline.prepareExecution();
    List<RowMetaAndData> rowMetaAndData = new ArrayList<>();
    subPipeline.getComponents().stream()
      .filter( c -> c.getName().equalsIgnoreCase( subTransform ) )
      .findFirst()
      .ifPresent( c -> c.addRowListener( new RowAdapter() {
        @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) {
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

  private synchronized void updateStatuses( IPipelineEngine<PipelineMeta> subPipeline ) {
    List<IEngineComponent> components = subPipeline.getComponents();
    for ( IEngineComponent component : components ) {

      TransformStatus transformStatus;
      if ( statuses.containsKey( component.getName() ) ) {
        transformStatus = statuses.get( component.getName() );
        transformStatus.updateAll( component );
      } else {
        transformStatus = new TransformStatus( component );
        statuses.put( component.getName(), transformStatus );
      }

      transformStatus.setStatusDescription( EngineComponent.ComponentExecutionStatus.STATUS_RUNNING.getDescription() );
    }
  }

  private IPipelineEngine<PipelineMeta> createSubPipeline() throws HopException {
    IPipelineEngine<PipelineMeta> subPipeline = PipelineEngineFactory.createPipelineEngine( runConfiguration, this.subPipelineMeta );
    subPipeline.setParentPipeline( this.parentPipeline );
    subPipeline.setLogLevel( this.parentPipeline.getLogLevel() );
    if ( this.shareVariables ) {
      subPipeline.shareWith( this.parentPipeline );
    }

    subPipeline.setInternalHopVariables( this.parentPipeline );
    subPipeline.setPreview( this.parentPipeline.isPreview() );
    PipelineTransformUtil.initServletConfig( this.parentPipeline, subPipeline );
    return subPipeline;
  }

  private void passParametersToPipeline( IPipelineEngine<PipelineMeta> internalPipeline, RowMetaAndData rowMetaAndData ) throws HopException {
    internalPipeline.clearParameterValues();
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
        value = this.parentPipeline.resolve( inputValue );
      }

      if ( Const.indexOfString( variable, parameterNames ) < 0 ) {
        internalPipeline.setVariable( variable, Const.NVL( value, "" ) );
      } else {
        internalPipeline.setParameterValue( variable, Const.NVL( value, "" ) );
      }
    }

    internalPipeline.activateParameters(internalPipeline);
  }

  public void stop() {
    stopped = true;
    for ( IPipelineEngine<PipelineMeta> subPipeline : running ) {
      subPipeline.stopAll();
    }
    running.clear();
    for ( Map.Entry<String, TransformStatus> entry : statuses.entrySet() ) {
      entry.getValue().setStatusDescription( EngineComponent.ComponentExecutionStatus.STATUS_STOPPED.getDescription() );
    }
  }

  public Map<String, TransformStatus> getStatuses() {
    return statuses;
  }

  public IPipelineEngine<PipelineMeta> getParentPipeline() {
    return parentPipeline;
  }

  /**
   * Gets running
   *
   * @return value of running
   */
  public Set<IPipelineEngine<PipelineMeta>> getRunning() {
    return running;
  }

  /**
   * @param running The running to set
   */
  public void setRunning( Set<IPipelineEngine<PipelineMeta>> running ) {
    this.running = running;
  }
}
