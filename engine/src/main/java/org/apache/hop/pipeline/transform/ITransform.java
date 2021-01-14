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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The interface that any pipeline transform or plugin needs to implement.
 * <p>
 * Created on 12-AUG-2004
 *
 * @author Matt
 */

public interface ITransform<Meta extends ITransformMeta, Data extends ITransformData>
  extends IVariables, IHasLogChannel, IEngineComponent {

  /**
   * @return the pipeline that is executing this transform
   */
  IPipelineEngine<PipelineMeta> getPipeline();

  /**
   * Perform the equivalent of processing one row. Typically this means reading a row from input (getRow()) and passing
   * a row to output (putRow)).
   *
   * @return false if no more rows can be processed or an error occurred.
   * @throws HopException
   */
  boolean processRow() throws HopException;

  /**
   * This method checks if the transform is capable of processing at least one row.
   * <p>
   * For example, if a transform has no input records but needs at least one to function, it will return false.
   *
   * @return true if the transform can process a row.
   */
  boolean canProcessOneRow();

  /**
   * Initialize and do work where other transforms need to wait for...
   *
   */
  boolean init();

  /**
   * Dispose of this transform: close files, empty logs, etc.
   *
   */
  void dispose();

  /**
   * Mark the start time of the transform.
   */
  void markStart();

  /**
   * Mark the end time of the transform.
   */
  void markStop();

  /**
   * Stop running operations...
   *
   */
  void stopRunning() throws HopException;

  /**
   * @return true if the transform is running after having been initialized
   */
  boolean isRunning();

  /**
   * Flag the transform as running or not
   *
   * @param running the running flag to set
   */
  void setRunning( boolean running );

  /**
   * @return True if the transform is marked as stopped. Execution should stop immediate.
   */
  boolean isStopped();

  /**
   * @param stopped true if the transform needs to be stopped
   */
  void setStopped( boolean stopped );

  /**
   * @param stopped true if the transform needs to be safe stopped
   */
  default void setSafeStopped( boolean stopped ) {
  }

  /**
   * @return true if transform is safe stopped.
   */
  default boolean isSafeStopped() {
    return false;
  }

  /**
   * @return True if the transform is paused
   */
  boolean isPaused();

  /**
   * Flags all rowsets as stopped/completed/finished.
   */
  void stopAll();

  /**
   * Pause a running transform
   */
  void pauseRunning();

  /**
   * Resume a running transform
   */
  void resumeRunning();

  /**
   * Get the name of the transform.
   *
   * @return the name of the transform
   */
  String getTransformName();

  /**
   * @return The transforms copy number (default 0)
   */
  int getCopy();

  /**
   * @return the type ID of the transform...
   */
  String getTransformPluginId();

  /**
   * Get the number of errors
   *
   * @return the number of errors
   */
  long getErrors();

  /**
   * Sets the number of errors
   *
   * @param errors the number of errors to set
   */
  void setErrors( long errors );

  /**
   * @return Returns the linesInput.
   */
  long getLinesInput();

  /**
   * @return Returns the linesOutput.
   */
  long getLinesOutput();

  /**
   * @return Returns the linesRead.
   */
  long getLinesRead();

  /**
   * @return Returns the linesWritten.
   */
  long getLinesWritten();

  /**
   * @return Returns the linesUpdated.
   */
  long getLinesUpdated();

  /**
   * @param linesRejected transforms the lines rejected by error handling.
   */
  void setLinesRejected( long linesRejected );

  /**
   * @return Returns the lines rejected by error handling.
   */
  long getLinesRejected();

  /**
   * Put a row on the destination rowsets.
   *
   * @param row The row to send to the destinations transforms
   */
  void putRow( IRowMeta row, Object[] data ) throws HopException;

  /**
   * @return a row from the source transform(s).
   */
  Object[] getRow() throws HopException;

  /**
   * Signal output done to destination transforms
   */
  void setOutputDone();

  /**
   * Add a rowlistener to the transform allowing you to inspect (or manipulate, be careful) the rows coming in or exiting the
   * transform.
   *
   * @param rowListener the rowlistener to add
   */
  void addRowListener( IRowListener rowListener );

  /**
   * Remove a rowlistener from this transform.
   *
   * @param rowListener the rowlistener to remove
   */
  void removeRowListener( IRowListener rowListener );

  /**
   * @return a list of the installed RowListeners
   */
  List<IRowListener> getRowListeners();

  /**
   * @return The list of active input rowsets for the transform
   */
  List<IRowSet> getInputRowSets();

  /**
   * @return The list of active output rowsets for the transform
   */
  List<IRowSet> getOutputRowSets();

  /**
   * @return true if the transform is running partitioned
   */
  boolean isPartitioned();

  /**
   * @param partitionId the partitionID to set
   */
  void setPartitionId( String partitionId );

  /**
   * @return the transforms partition ID
   */
  String getPartitionId();

  /**
   * Cleanup any left-over resources for this transform.
   */
  void cleanup();

  /**
   * This method is executed by Pipeline right before the threads start and right after initialization.<br>
   * <br>
   * <b>!!! A plugin implementing this method should make sure to also call <i>super.initBeforeStart();</i> !!!</b>
   *
   * @throws HopTransformException In case there is an error
   */
  void initBeforeStart() throws HopTransformException;

  /**
   * Attach a transform listener to be notified when a transform finishes
   *
   * @param transformListener The listener to add to the transform
   */
  void addTransformFinishedListener( ITransformFinishedListener transformListener );

  /**
   * Attach a transform listener to be notified when a transform starts
   *
   * @param transformListener The listener to add to the transform
   */
  void addTransformStartedListener( ITransformStartedListener transformListener );

  /**
   * @return true if the thread is a special mapping transform
   */
  boolean isMapping();

  /**
   * @return The metadata for this transform
   */
  TransformMeta getTransformMeta();

  /**
   * @return the logging channel for this transform
   */
  @Override ILogChannel getLogChannel();

  /**
   * @return The total amount of rows in the input buffers
   */
  int rowsetInputSize();

  /**
   * @return The total amount of rows in the output buffers
   */
  int rowsetOutputSize();

  /**
   * @return The number of "processed" lines of a transform. Well, a representable metric for that anyway.
   */
  long getProcessed();

  /**
   * @return The result files for this transform
   */
  Map<String, ResultFile> getResultFiles();

  /**
   * @return the description as in {@link ITransformData}
   */
  ComponentExecutionStatus getStatus();

  /**
   * @return The number of ms that this transform has been running
   */
  long getExecutionDuration();

  /**
   * To be used to flag an error output channel of a transform prior to execution for performance reasons.
   */
  void identifyErrorOutput();

  /**
   * @param partitioned true if this transform is partitioned
   */
  void setPartitioned( boolean partitioned );

  /**
   * @param partitioningMethod The repartitioning method
   */
  void setRepartitioning( int partitioningMethod );

  /**
   * Calling this method will alert the transform that we finished passing a batch of records to the transform. Specifically for
   * transforms like "Sort Rows" it means that the buffered rows can be sorted and passed on.
   *
   * @throws HopException In case an error occurs during the processing of the batch of rows.
   */
  void batchComplete() throws HopException;

  /**
   * Pass along the metadata to use when loading external elements at runtime.
   *
   * @param metadataProvider The metadata to use
   */
  void setMetadataProvider( IHopMetadataProvider metadataProvider );

  /**
   * @return The metadata that the transform uses to load external elements from.
   */
  IHopMetadataProvider getMetadataProvider();

  /**
   * @return the index of the active (current) output row set
   */
  int getCurrentOutputRowSetNr();

  /**
   * @param index Sets the index of the active (current) output row set to use.
   */
  void setCurrentOutputRowSetNr( int index );

  /**
   * @return the index of the active (current) input row set
   */
  int getCurrentInputRowSetNr();

  /**
   * @param index Sets the index of the active (current) input row set to use.
   */
  void setCurrentInputRowSetNr( int index );

  default Collection<TransformStatus> subStatuses() {
    return Collections.emptyList();
  }

  default void addRowSetToInputRowSets( IRowSet rowSet ) {
    getInputRowSets().add( rowSet );
  }

  default void addRowSetToOutputRowSets( IRowSet rowSet ) {
    getOutputRowSets().add( rowSet );
  }

  /**
   * @return Returns the transform specific metadata.
   */
  Meta getMeta();

  /**
   * @param meta The transform specific metadata.
   */
  void setMeta( Meta meta );

  /**
   * Get the transform data
   *
   * @return the tansform data
   */
  Data getData();

  /**
   * @param data The transform data to set
   */
  void setData( Data data );



  /**
   * Gets initStartDate
   *
   * @return value of initStartDate
   */
  Date getInitStartDate();

  /**
   * @param initStartDate The initStartDate to set
   */
  void setInitStartDate( Date initStartDate );

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  Date getExecutionStartDate();

  /**
   * @param executionStartDate The executionStartDate to set
   */
  void setExecutionStartDate( Date executionStartDate );

  /**
   * Gets firstRowReadDate
   *
   * @return value of firstRowReadDate
   */
  Date getFirstRowReadDate();

  /**
   * @param firstRowReadDate The firstRowReadDate to set
   */
  void setFirstRowReadDate( Date firstRowReadDate );

  /**
   * Gets lastRowWrittenDate
   *
   * @return value of lastRowWrittenDate
   */
  Date getLastRowWrittenDate();

  /**
   * @param lastRowWrittenDate The lastRowWrittenDate to set
   */
  void setLastRowWrittenDate( Date lastRowWrittenDate );

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  Date getExecutionEndDate();

  /**
   * @param executionEndDate The executionEndDate to set
   */
  void setExecutionEndDate( Date executionEndDate );

}
