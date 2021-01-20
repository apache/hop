// CHECKSTYLE:FileLength:OFF
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.BasePartitioner;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import java.io.Closeable;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class can be extended for the actual row processing of the implemented transform.
 * <p>
 * The implementing class can rely mostly on the base class, and has only three important methods it implements itself.
 * The three methods implement the transform lifecycle during pipeline execution: initialization, row processing, and
 * clean-up.
 * <ul>
 * <li>Transform Initialization<br/>
 * The init() method is called when a pipeline is preparing to start execution.
 * <p>
 * <pre>
 * <a href="#init(org.apache.hop.pipeline.transform.ITransformMeta,
 * org.apache.hop.pipeline.transform.ITransformData)">public boolean init(...)</a>
 * </pre>
 * <p>
 * Every transform is given the opportunity to do one-time initialization tasks like opening files or establishing database
 * connections. For any transforms derived from BaseTransform it is mandatory that super.init() is called to ensure correct
 * behavior. The method must return true in case the transform initialized correctly, it must returned false if there was an
 * initialization error. PDI will abort the execution of a pipeline in case any transform returns false upon
 * initialization.
 * <p></li>
 * <p>
 * <li>Row Processing<br/>
 * Once the pipeline starts execution it enters a tight loop calling processRow() on each transform until the method
 * returns false. Each transform typically reads a single row from the input stream, alters the row structure and fields and
 * passes the row on to next transforms.
 * <p>
 * <pre>
 * <a href="#processRow(org.apache.hop.pipeline.transform.ITransformMeta,
 * org.apache.hop.pipeline.transform.ITransformData)">public boolean processRow(...)</a>
 * </pre>
 * <p>
 * A typical implementation queries for incoming input rows by calling getRow(), which blocks and returns a row object
 * or null in case there is no more input. If there was an input row, the transform does the necessary row processing and
 * calls putRow() to pass the row on to the next transform. If there are no more rows, the transform must call setOutputDone() and
 * return false.
 * <p>
 * Formally the method must conform to the following rules:
 * <ul>
 * <li>If the transform is done processing all rows, the method must call setOutputDone() and return false</li>
 * <li>If the transform is not done processing all rows, the method must return true. Hop will call processRow() again in
 * this case.</li>
 * </ul>
 * </li>
 * <p>
 * <li>Transform Clean-Up<br/>
 * Once the pipeline is complete, Hop calls dispose() on all transforms.
 * <p>
 * <pre>
 * <a href="#dispose(org.apache.hop.pipeline.transform.ITransformMeta,
 * org.apache.hop.pipeline.transform.ITransformData)">public void dispose(...)</a>
 * </pre>
 * <p>
 * Transforms are required to deallocate resources allocated during init() or subsequent row processing. This typically means
 * to clear all fields of the ITransformData object, and to ensure that all open files or connections are properly
 * closed. For any transforms derived from BaseTransform it is mandatory that super.dispose() is called to ensure correct
 * deallocation.
 * </ul>
 */
public class BaseTransform<Meta extends ITransformMeta, Data extends ITransformData>
  implements ITransform<Meta, Data>,
  IVariables, ILoggingObject, IExtensionData, IEngineComponent {

  private static final Class<?> PKG = BaseTransform.class; // For Translator

  protected IVariables variables = new Variables();

  private PipelineMeta pipelineMeta;

  private TransformMeta transformMeta;

  private String transformName;

  protected ILogChannel log;

  private String containerObjectId;

  private IPipelineEngine<PipelineMeta> pipeline;

  private final Object statusCountersLock = new Object();

  protected Date initStartDate;
  protected Date executionStartDate;
  protected Date firstRowReadDate;
  protected Date lastRowWrittenDate;
  protected Date executionEndDate;

  /**
   * Number of lines read from previous transform(s)
   */
  private long linesRead;

  /**
   * Number of lines written to next transform(s)
   */
  private long linesWritten;

  /**
   * Number of lines read from file or database 
   */
  private long linesInput;

  /**
   * Number of lines written to file or database  
   */
  private long linesOutput;

  /**
   * Number of updates in a database table or file
   */
  private long linesUpdated;

  /**
   * Number of lines skipped
   */
  private long linesSkipped;

  /**
   * Number of lines rejected to an error handling transform
   */
  private long linesRejected;

  private boolean distributed;

  private IRowDistribution rowDistribution;

  private long errors;

  private TransformMeta[] nextTransforms;

  private TransformMeta[] prevTransforms;

  private int currentInputRowSetNr, currentOutputRowSetNr;

  /**
   * The rowsets on the input, size() == nr of source transforms
   */
  private List<IRowSet> inputRowSets;

  private final ReentrantReadWriteLock inputRowSetsLock = new ReentrantReadWriteLock();

  /**
   * the rowsets on the output, size() == nr of target transforms
   */
  private List<IRowSet> outputRowSets;

  private final ReadWriteLock outputRowSetsLock = new ReentrantReadWriteLock();

  /**
   * the rowset for the error rows
   */
  private IRowSet errorRowSet;

  private AtomicBoolean running;

  private AtomicBoolean stopped;

  protected AtomicBoolean safeStopped;

  private AtomicBoolean paused;

  private boolean init;

  /**
   * the copy number of this thread
   */
  private int copyNr;

  private Date startTime;
  
  private Date stopTime;

  /**
   * if true then the row being processed is the first row
   */
  public boolean first;

  /**
   *
   */
  public boolean terminator;

  public List<Object[]> terminatorRows;

  protected Meta meta;
  protected Data data;

  /**
   * The list of IRowListener interfaces
   */
  protected List<IRowListener> rowListeners;

  /**
   * Map of files that are generated or used by this transform. After execution, these can be added to result. The entry to
   * the map is the filename
   */
  private final Map<String, ResultFile> resultFiles;
  private final ReentrantReadWriteLock resultFilesLock;

  /**
   * This contains the first row received and will be the reference row. We used it to perform extra checking: see if we
   * don't get rows with "mixed" contents.
   */
  private IRowMeta inputReferenceRow;

  /**
   * This field tells the putRow() method that we are in partitioned mode
   */
  private boolean partitioned;

  /**
   * The partition ID at which this transform copy runs, or null if this transform is not running partitioned.
   */
  private String partitionId;

  /**
   * This field tells the putRow() method to re-partition the incoming data, See also
   * TransformPartitioningMeta.PARTITIONING_METHOD_*
   */
  private int repartitioning;

  /**
   * The partition ID to rowset mapping
   */
  private Map<String, BlockingRowSet> partitionTargets;

  private IRowMeta inputRowMeta;

  /**
   * transform partitioning information of the NEXT transform
   */
  private TransformPartitioningMeta nextTransformPartitioningMeta;

  /**
   * The metadata information of the error output row. There is only one per transform so we cache it
   */
  private IRowMeta errorRowMeta = null;

  private IRowMeta previewRowMeta;

  private boolean checkPipelineRunning;

  private static final int NR_OF_ROWS_IN_BLOCK = 500;

  private int blockPointer;

  private List<ITransformFinishedListener> transformFinishedListeners;
  private List<ITransformStartedListener> transformStartedListeners;

  /**
   * The upper buffer size boundary after which we manage the thread priority a little bit to prevent excessive locking
   */
  private int upperBufferBoundary;

  /**
   * The lower buffer size boundary after which we manage the thread priority a little bit to prevent excessive locking
   */
  private int lowerBufferBoundary;

  /**
   * maximum number of errors to allow
   */
  private Long maxErrors = -1L;

  /**
   * maximum percent of errors to allow
   */
  private int maxPercentErrors = -1;

  /**
   * minumum number of rows to process before using maxPercentErrors in calculation
   */
  private long minRowsForMaxErrorPercent = -1L;

  /**
   * set this flag to true to allow empty field names and types to output
   */
  private boolean allowEmptyFieldNamesAndTypes = false;

  /**
   * Keeps track of the number of rows read for input deadlock verification.
   */
  protected long deadLockCounter;

  /**
   * The metadata that the transform uses to load external elements from
   */
  protected IHopMetadataProvider metadataProvider;

  protected Map<String, Object> extensionDataMap;

  /**
   * rowHandler handles getting/putting rows and putting errors.
   * Default implementation defers to corresponding methods in this class.
   */
  private IRowHandler rowHandler;

  private AtomicBoolean markStopped;

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this class to implement your own
   * transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param data          the data object to store temporary data, database connections, caches, result sets,
   *                      hashtables etc.
   * @param copyNr        The copynumber for this transform.
   * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
   */
  public BaseTransform( TransformMeta transformMeta, Meta meta, Data data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    this.transformMeta = transformMeta;
    this.meta = meta;
    this.data = data;
    this.copyNr = copyNr;
    this.pipelineMeta = pipelineMeta;
    this.pipeline = pipeline;
    this.transformName = transformMeta.getName();

    // Sanity check
    //
    if ( transformMeta.getName() == null ) {
      throw new RuntimeException( "A transform in pipeline [" + pipelineMeta.toString() + "] doesn't have a name.  A transform should always have a name to identify it by." );
    }

    if (pipeline != null) {
      // We need a full copy of the variables to take into account internal variables
      // Like Internal.Transform.CopyNr, Internal.Transform.Partition.ID, ...
      //
      initializeFrom(pipeline);
    }

    log = HopLogStore.getLogChannelFactory().create( this, pipeline );

    first = true;

    running = new AtomicBoolean( false );
    stopped = new AtomicBoolean( false );
    safeStopped = new AtomicBoolean( false );
    paused = new AtomicBoolean( false );

    init = false;

    synchronized ( statusCountersLock ) {
      linesRead = 0L;
      linesWritten = 0L;
      linesUpdated = 0L;
      linesSkipped = 0L;
      linesRejected = 0L;
      linesInput = 0L;
      linesOutput = 0L;
    }

    inputRowSets = null;
    outputRowSets = null;
    nextTransforms = null;

    terminator = transformMeta.hasTerminator();
    if ( terminator ) {
      terminatorRows = new ArrayList<>();
    } else {
      terminatorRows = null;
    }

    startTime = null;
    stopTime = null;

    distributed = transformMeta.isDistributes();
    rowDistribution = transformMeta.getRowDistribution();

    rowListeners = new CopyOnWriteArrayList<>();
    resultFiles = new HashMap<>();
    resultFilesLock = new ReentrantReadWriteLock();

    repartitioning = TransformPartitioningMeta.PARTITIONING_METHOD_NONE;
    partitionTargets = new Hashtable<>();

    extensionDataMap = new HashMap<>();

    checkPipelineRunning = false;

    blockPointer = 0;

    transformFinishedListeners = Collections.synchronizedList( new ArrayList<>() );
    transformStartedListeners = Collections.synchronizedList( new ArrayList<>() );

    markStopped = new AtomicBoolean( false );

    dispatch();

    upperBufferBoundary = (int) ( pipeline.getRowSetSize() * 0.99 );
    lowerBufferBoundary = (int) ( pipeline.getRowSetSize() * 0.01 );
  }

  @Override public boolean init() {

    initStartDate = new Date();

    data.setStatus( ComponentExecutionStatus.STATUS_INIT );

    if ( transformMeta.isPartitioned() ) {
      // This is a locally partitioned transform...
      //
      int partitionNr = copyNr;
      String partitionNrString = new DecimalFormat( "000" ).format( partitionNr );
      setVariable( Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_NR, partitionNrString );
      final List<String> partitionIdList = transformMeta.getTransformPartitioningMeta().getPartitionSchema().calculatePartitionIds(this);

      if ( partitionIdList.size() > 0 ) {
        String partitionId = partitionIdList.get( partitionNr );
        setVariable( Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_ID, partitionId );
      } else {
        logError( BaseMessages.getString( PKG, "BaseTransform.Log.UnableToRetrievePartitionId",
          transformMeta.getTransformPartitioningMeta().getPartitionSchema().getName() ) );
        return false;
      }
    } else if ( !Utils.isEmpty( partitionId ) ) {
      setVariable( Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_ID, partitionId );
    }

    setVariable( Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, Integer.toString( copyNr ) );

    // BACKLOG-18004
    allowEmptyFieldNamesAndTypes = Boolean.parseBoolean( System.getProperties().getProperty( Const.HOP_ALLOW_EMPTY_FIELD_NAMES_AND_TYPES, "false" ) );


    // Getting ans setting the error handling values
    // first, get the transform meta
    TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
    if ( transformErrorMeta != null ) {

      // do an environment substitute for transformErrorMeta.getMaxErrors(), transformErrorMeta.getMinPercentRows()
      // and transformErrorMeta.getMaxPercentErrors()
      // Catch NumberFormatException since the user can enter anything in the dialog- the value
      // they enter must be a number or a variable set to a number

      // We will use a boolean to indicate failure so that we can log all errors - not just the first one caught
      boolean envSubFailed = false;
      try {
        maxErrors =
          ( !Utils.isEmpty( transformErrorMeta.getMaxErrors() ) ? Long.valueOf( pipeline.resolve( transformErrorMeta.getMaxErrors() ) ) : -1L );
      } catch ( NumberFormatException nfe ) {
        log.logError( BaseMessages.getString( PKG, "BaseTransform.Log.NumberFormatException", BaseMessages.getString(
          PKG, "BaseTransform.Property.MaxErrors.Name" ), this.transformName, ( transformErrorMeta.getMaxErrors() != null
          ? transformErrorMeta.getMaxErrors() : "" ) ) );
        envSubFailed = true;
      }

      try {
        minRowsForMaxErrorPercent =
          ( !Utils.isEmpty( transformErrorMeta.getMinPercentRows() ) ? Long.parseLong( pipeline
            .resolve( transformErrorMeta.getMinPercentRows() ) ) : -1L );
      } catch ( NumberFormatException nfe ) {
        log.logError( BaseMessages.getString( PKG, "BaseTransform.Log.NumberFormatException", BaseMessages.getString(
          PKG, "BaseTransform.Property.MinRowsForErrorsPercentCalc.Name" ), this.transformName, ( transformErrorMeta
          .getMinPercentRows() != null ? transformErrorMeta.getMinPercentRows() : "" ) ) );
        envSubFailed = true;
      }

      try {
        maxPercentErrors =
          ( !Utils.isEmpty( transformErrorMeta.getMaxPercentErrors() ) ? Integer.valueOf( pipeline
            .resolve( transformErrorMeta.getMaxPercentErrors() ) ) : -1 );
      } catch ( NumberFormatException nfe ) {
        log.logError( BaseMessages.getString(
          PKG, "BaseTransform.Log.NumberFormatException", BaseMessages.getString(
            PKG, "BaseTransform.Property.MaxPercentErrors.Name" ), this.transformName, ( transformErrorMeta
            .getMaxPercentErrors() != null ? transformErrorMeta.getMaxPercentErrors() : "" ) ) );
        envSubFailed = true;
      }

      // if we failed and environment subsutitue
      if ( envSubFailed ) {
        return false;
      }
    }

    return true;
  }

  @Override public void dispose() {
    data.setStatus( ComponentExecutionStatus.STATUS_DISPOSED );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#cleanup()
   */
  @Override
  public void cleanup() {
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getProcessed()
   */
  @Override
  public long getProcessed() {
    if ( getLinesRead() > getLinesWritten() ) {
      return getLinesRead();
    } else {
      return getLinesWritten();
    }
  }

  /**
   * Sets the copy.
   *
   * @param cop the new copy
   */
  public void setCopy( int cop ) {
    copyNr = cop;
  }

  /**
   * @return The transforms copy number (default 0)
   */
  @Override
  public int getCopy() {
    return copyNr;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getErrors()
   */
  @Override
  public long getErrors() {
    return errors;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#setErrors(long)
   */
  @Override
  public void setErrors( long e ) {
    errors = e;
  }

  /**
   * @return Returns the number of lines read from previous transforms
   * 
   * @see {@link #setLinesRead()}, {@link #incrementLinesRead()}, {@link #decrementLinesRead()}
   */
  @Override
  public long getLinesRead() {
    synchronized ( statusCountersLock ) {
      return linesRead;
    }
  }

  /**
   * Increments the number of lines read from previous transforms by one
   * @see {@link #getLinesRead()}, {@link #setLinesRead()}, {@link #decrementLinesRead()}
   * @return Returns the new value
   */
  public long incrementLinesRead() {
    synchronized ( statusCountersLock ) {
      return ++linesRead;
    }
  }

  /**
   * Decrements the number of lines read from previous transforms by one
   *
   * @return Returns the new value
   */
  public long decrementLinesRead() {
    synchronized ( statusCountersLock ) {
      return --linesRead;
    }
  }

  /**
   * @param newLinesReadValue the new number of lines read from previous transforms
   * 
   * @see {@link #getLinesRead()}, {@link #incrementLinesRead()}, {@link #decrementLinesRead()}
   */
  public void setLinesRead( long newLinesReadValue ) {
    synchronized ( statusCountersLock ) {
      linesRead = newLinesReadValue;
    }
  }

  /**
   * 
   * @see {@link #incrementLinesInput()}
   * @return Returns the number of lines read from an input source: database, file, socket, etc.
   */
  @Override
  public long getLinesInput() {
    synchronized ( statusCountersLock ) {
      return linesInput;
    }
  }

  /**
   * Increments the number of lines read from an input source: database, file, socket, etc.
   *
   * @see {@link #getLinesInput()}, {@link #setLinesInput()}
   * @return the new incremented value
   */
  public long incrementLinesInput() {
    synchronized ( statusCountersLock ) {
      return ++linesInput;
    }
  }

  /**
   * 
   * @param newLinesInputValue the new number of lines read from an input source: database, file, socket, etc.
   * 
   * @see {@link #getLinesInput()} or {@link #incrementLinesInput()}
   */
  public void setLinesInput( long newLinesInputValue ) {
    synchronized ( statusCountersLock ) {
      linesInput = newLinesInputValue;
    }
  }

  /**
   * @return Returns the number of lines written to an output target: database, file, socket, etc.
   * 
   * @see {@link #setLinesOutput()}, {@link #incrementLinesOutput()}
   */
  @Override
  public long getLinesOutput() {
    synchronized ( statusCountersLock ) {
      return linesOutput;
    }
  }

  /**
   * Increments the number of lines written to an output target: database, file, socket, etc.
   *
   * @return the new incremented value
   * 
   * @see {@link #getLinesOutput()}, {@link #setLinesOutput()}
   */
  public long incrementLinesOutput() {
    synchronized ( statusCountersLock ) {
      return ++linesOutput;
    }
  }

  /**
   * @param newLinesOutputValue the new number of lines written to an output target: database, file, socket, etc.
   * 
   * @see {@link #getLinesOutput()} or {@link #incrementLinesOutput()}
   */
  public void setLinesOutput( long newLinesOutputValue ) {
    synchronized ( statusCountersLock ) {
      linesOutput = newLinesOutputValue;
    }
  }

  /**
   * Get the number of lines written to next transforms
   * 
   * @see {@link #incrementLinesWritten()}, or {@link #decrementLinesWritten()}
   * @return Returns the linesWritten.
   */
  @Override
  public long getLinesWritten() {
    synchronized ( statusCountersLock ) {
      return linesWritten;
    }
  }

  /**
   * Increments the number of lines written to next transforms by one
   * 
   * @see {@link #getLinesWritten()}, {@link #decrementLinesWritten()}
   * @return Returns the new value
   */
  public long incrementLinesWritten() {
    synchronized ( statusCountersLock ) {
      return ++linesWritten;
    }
  }

  /**
   * Decrements the number of lines written to next transforms by one
   * 
   * @see {@link #getLinesWritten()}, {@link #incrementLinesWritten()}
   * @return Returns the new value
   */
  public long decrementLinesWritten() {
    synchronized ( statusCountersLock ) {
      return --linesWritten;
    }
  }

  /**
   * Set the number of lines written to next transforms
   * 
   * @param newLinesWrittenValue the new number of lines written to next transforms
   * 
   * @see {@link #getLinesWritten()}, {@link #incrementLinesWritten()}, or {@link #decrementLinesWritten()}
   */
  public void setLinesWritten( long newLinesWrittenValue ) {
    synchronized ( statusCountersLock ) {
      linesWritten = newLinesWrittenValue;
    }
  }

  /**
   * @return Returns the number of lines updated in an output target: database, file, socket, etc.
   * 
   * @see {@link #setLinesUpdated()}, {@link #incrementLinesUpdated()}
   */
  @Override
  public long getLinesUpdated() {
    synchronized ( statusCountersLock ) {
      return linesUpdated;
    }
  }

  /**
   * Increments the number of lines updated in an output target: database, file, socket, etc.
   *
   * @return the new incremented value
   * 
   * @see {@link #getLinesUpdated()}, {@link #setLinesUpdated()}
   */
  public long incrementLinesUpdated() {
    synchronized ( statusCountersLock ) {
      return ++linesUpdated;
    }
  }

  /**
   * @param newLinesUpdatedValue the new number of lines updated in an output target: database, file, socket, etc.
   * 
   * @see {@link #getLinesUpdated()}, {@link #incrementLinesUpdated()}
   */
  public void setLinesUpdated( long newLinesUpdatedValue ) {
    synchronized ( statusCountersLock ) {
      linesUpdated = newLinesUpdatedValue;
    }
  }

  /**
   * Get the number of lines rejected to an error handling transform
   * 
   * @see {@link #setLinesRejected()}, {@link #incrementLinesRejected()}
   * @return the number of lines rejected to an error handling transform
   */
  @Override
  public long getLinesRejected() {
    synchronized ( statusCountersLock ) {
      return linesRejected;
    }
  }

  /**
   * Increments the number of lines rejected to an error handling transform
   * 
   * @see {@link #getLinesRejected()}, {@link #setLinesRejected()}
   * @return the new incremented value
   */
  public long incrementLinesRejected() {
    synchronized ( statusCountersLock ) {
      return ++linesRejected;
    }
  }

  /**
   * @param newLinesRejectedValue lines number of lines rejected to an error handling transform
   * 
   * @see {@link #getLinesRejected()}, {@link #incrementLinesRejected()}
   */
  @Override
  public void setLinesRejected( long newLinesRejectedValue ) {
    synchronized ( statusCountersLock ) {
      linesRejected = newLinesRejectedValue;
    }
  }

  /**
   * @return the number of lines skipped
   * 
   * @see {@link #setLinesSkipped()}, {@link #incrementLinesSkipped()}
   */
  public long getLinesSkipped() {
    synchronized ( statusCountersLock ) {
      return linesSkipped;
    }
  }

  /**
   * Increments the number of lines skipped
   *
   * @return the new incremented value
   * 
   * @see {@link #getLinesSkipped()}, {@link #setLinesSkipped()}
   */
  public long incrementLinesSkipped() {
    synchronized ( statusCountersLock ) {
      return ++linesSkipped;
    }
  }

  /**
   * @param newLinesSkippedValue lines number of lines skipped
   * 
   * @see {@link #getLinesSkipped()}, {@link #incrementLinesSkipped()}
   */
  public void setLinesSkipped( long newLinesSkippedValue ) {
    synchronized ( statusCountersLock ) {
      linesSkipped = newLinesSkippedValue;
    }
  }

  @Override public boolean isSelected() {
    return transformMeta != null && transformMeta.isSelected();
  }

  @Override
  public String getTransformName() {
    return transformName;
  }

  /**
   * Sets the transformName.
   *
   * @param transformName the new transformName
   */
  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  /**
   * Gets the dispatcher.
   *
   * @return the dispatcher
   */
  public IPipelineEngine<PipelineMeta> getDispatcher() {
    return pipeline;
  }

  /**
   * Gets the status description.
   *
   * @return the status description
   */
  public String getStatusDescription() {
    return getStatus().getDescription();
  }

  /**
   * @return Returns the transformMetaInterface.
   */
  public Meta getMeta() {
    return meta;
  }

  /**
   * @param meta The transformMetaInterface to set.
   */
  public void setMeta( Meta meta ) {
    this.meta = meta;
  }

  /**
   * Gets data
   *
   * @return value of data
   */
  public Data getData() {
    return data;
  }

  /**
   * @param data The data to set
   */
  public void setData( Data data ) {
    this.data = data;
  }

  /**
   * @return Returns the transformMeta.
   */
  @Override
  public TransformMeta getTransformMeta() {
    return transformMeta;
  }

  /**
   * @param transformMeta The transformMeta to set.
   */
  public void setTransformMeta( TransformMeta transformMeta ) {
    this.transformMeta = transformMeta;
  }

  /**
   * @return Returns the pipelineMeta.
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta The pipelineMeta to set.
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * @return Returns the pipeline.
   */
  @Override
  public IPipelineEngine<PipelineMeta> getPipeline() {
    return pipeline;
  }


  /**
   * putRow is used to copy a row, to the alternate rowset(s) This should get priority over everything else!
   * (synchronized) If distribute is true, a row is copied only once to the output rowsets, otherwise copies are sent to
   * each rowset!
   *
   * @param row The row to put to the destination rowset(s).
   * @throws HopTransformException
   */
  @Override
  public void putRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
    if ( rowMeta != null ) {
      if ( !allowEmptyFieldNamesAndTypes ) {
        // check row meta for empty field name (BACKLOG-18004)
        for ( IValueMeta vmi : rowMeta.getValueMetaList() ) {
          if ( StringUtils.isBlank( vmi.getName() ) ) {
            throw new HopTransformException( "Please set a field name for all field(s) that have 'null'." );
          }
          if ( vmi.getType() <= 0 ) {
            throw new HopTransformException( "Please set a value for the missing field(s) type." );
          }
        }
      }
    }
    getRowHandler().putRow( rowMeta, row );

    // This transform is not reading data, only writing
    //
    if (firstRowReadDate==null) {
      firstRowReadDate = new Date();
    }
  }

  private void handlePutRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
    // Are we pausing the transform? If so, stall forever...
    //
    while ( paused.get() && !stopped.get() ) {
      try {
        Thread.sleep( 1 );
      } catch ( InterruptedException e ) {
        throw new HopTransformException( e );
      }
    }

    // Right after the pause loop we have to check if this thread is stopped or
    // not.
    //
    if ( stopped.get() && !safeStopped.get() ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "BaseTransform.Log.StopPuttingARow" ) );
      }
      stopAll();
      return;
    }

    // Have all threads started?
    // Are we running yet? If not, wait a bit until all threads have been
    // started.
    //
    if ( this.checkPipelineRunning == false ) {
      while ( !pipeline.isRunning() && !stopped.get() ) {
        try {
          Thread.sleep( 1 );
        } catch ( InterruptedException e ) {
          // Ignore
        }
      }
      this.checkPipelineRunning = true;
    }

    // call all row listeners...
    //
    for ( IRowListener listener : rowListeners ) {
      listener.rowWrittenEvent( rowMeta, row );
    }

    // Keep adding to terminator_rows buffer...
    //
    if ( terminator && terminatorRows != null ) {
      try {
        terminatorRows.add( rowMeta.cloneRow( row ) );
      } catch ( HopValueException e ) {
        throw new HopTransformException( "Unable to clone row while adding rows to the terminator rows.", e );
      }
    }

    outputRowSetsLock.readLock().lock();
    try {
      if ( outputRowSets.isEmpty() ) {
        // No more output rowsets!
        // Still update the nr of lines written.
        //
        incrementLinesWritten();

        return; // we're done here!
      }

      // Repartitioning happens when the current transform is not partitioned, but the next one is.
      // That means we need to look up the partitioning information in the next transform..
      // If there are multiple transforms, we need to look at the first (they should be all the same)
      //
      switch ( repartitioning ) {
        case TransformPartitioningMeta.PARTITIONING_METHOD_NONE:
          noPartitioning( rowMeta, row );
          break;

        case TransformPartitioningMeta.PARTITIONING_METHOD_SPECIAL:
          specialPartitioning( rowMeta, row );
          break;
        case TransformPartitioningMeta.PARTITIONING_METHOD_MIRROR:
          mirrorPartitioning( rowMeta, row );
          break;
        default:
          throw new HopTransformException( "Internal error: invalid repartitioning type: " + repartitioning );
      }
    } finally {
      outputRowSetsLock.readLock().unlock();
    }
  }

  /**
   * Copy always to all target transforms/copies
   */
  private void mirrorPartitioning( IRowMeta rowMeta, Object[] row ) {
    for ( IRowSet rowSet : outputRowSets ) {
      putRowToRowSet( rowSet, rowMeta, row );
    }
  }

  private void specialPartitioning( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
    if ( nextTransformPartitioningMeta == null ) {
      // Look up the partitioning of the next transform.
      // This is the case for non-clustered partitioning...
      //
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( transformMeta );
      if ( nextTransforms.size() > 0 ) {
        nextTransformPartitioningMeta = nextTransforms.get( 0 ).getTransformPartitioningMeta();
      }

      // TODO: throw exception if we're not partitioning yet.
      // For now it throws a NP Exception.
    }

    int partitionNr;
    try {
      partitionNr = nextTransformPartitioningMeta.getPartition( this, rowMeta, row );
    } catch ( HopException e ) {
      throw new HopTransformException(
        "Unable to convert a value to integer while calculating the partition number", e );
    }

    IRowSet selectedRowSet;


    // Local partitioning...
    // Put the row forward to the next transform according to the partition rule.
    //

    // Count of partitioned row at one transform
    int partCount = ( (BasePartitioner) nextTransformPartitioningMeta.getPartitioner() ).getNrPartitions();

    for ( int i = 0; i < nextTransforms.length; i++ ) {

      selectedRowSet = outputRowSets.get( partitionNr + i * partCount );

      if ( selectedRowSet == null ) {
        logBasic( BaseMessages.getString( PKG, "BaseTransform.TargetRowsetIsNotAvailable", partitionNr ) );
      } else {

        // Wait
        putRowToRowSet( selectedRowSet, rowMeta, row );
        incrementLinesWritten();

        if ( log.isRowLevel() ) {
          try {
            logRowlevel( BaseMessages.getString( PKG, "BaseTransform.PartitionedToRow", partitionNr,
              selectedRowSet, rowMeta.getString( row ) ) );
          } catch ( HopValueException e ) {
            throw new HopTransformException( e );
          }
        }
      }
    }

  }

  private void noPartitioning( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
    if ( distributed ) {
      if ( rowDistribution != null ) {
        // Plugin defined row distribution!
        //
        rowDistribution.distributeRow( rowMeta, row, this );
        incrementLinesWritten();
      } else {
        // ROUND ROBIN DISTRIBUTION:
        // --------------------------
        // Copy the row to the "next" output rowset.
        // We keep the next one in out_handling
        //
        IRowSet rs = outputRowSets.get( currentOutputRowSetNr );

        // To reduce stress on the locking system we are NOT going to allow
        // the buffer to grow to its full capacity.

        if ( !rs.isDone() && rs.size() >= upperBufferBoundary && !isStopped() ) {
          try {
            Thread.sleep( 0, 1 );
          } catch ( InterruptedException e ) {
            // Ignore sleep interruption exception
          }
        }

        // Loop until we find room in the target rowset
        //
        putRowToRowSet( rs, rowMeta, row );
        incrementLinesWritten();

        // Now determine the next output rowset!
        // Only if we have more then one output...
        //
        if ( outputRowSets.size() > 1 ) {
          currentOutputRowSetNr++;
          if ( currentOutputRowSetNr >= outputRowSets.size() ) {
            currentOutputRowSetNr = 0;
          }
        }
      }
    } else {

      // Copy the row to all output rowsets
      //

      // Copy to the row in the other output rowsets...
      for ( int i = 1; i < outputRowSets.size(); i++ ) { // start at 1

        IRowSet rs = outputRowSets.get( i );

        // To reduce stress on the locking system we are NOT going to allow
        // the buffer to grow to its full capacity.

        if ( !rs.isDone() && rs.size() >= upperBufferBoundary && !isStopped() ) {
          try {
            Thread.sleep( 0, 1 );
          } catch ( InterruptedException e ) {
            // Ignore sleep interruption exception
          }
        }

        try {
          // Loop until we find room in the target rowset
          //
          putRowToRowSet( rs, rowMeta, rowMeta.cloneRow( row ) );
          incrementLinesWritten();
        } catch ( HopValueException e ) {
          throw new HopTransformException( "Unable to clone row while copying rows to multiple target transforms", e );
        }
      }

      // set row in first output rowset
      //
      IRowSet rs = outputRowSets.get( 0 );
      putRowToRowSet( rs, rowMeta, row );
      incrementLinesWritten();
    }
  }

  private void putRowToRowSet( IRowSet rs, IRowMeta rowMeta, Object[] row ) {
    IRowMeta toBeSent;
    IRowMeta metaFromRs = rs.getRowMeta();
    if ( metaFromRs == null ) {
      // IRowSet is not initialised so far
      toBeSent = rowMeta.clone();
    } else {
      // use the existing
      toBeSent = metaFromRs;
    }

    while ( !rs.putRow( toBeSent, row ) ) {
      if ( isStopped() && !safeStopped.get() ) {
        return;
      }
    }
  }

  /**
   * putRowTo is used to put a row in a certain specific IRowSet.
   *
   * @param rowMeta The row meta-data to put to the destination IRowSet.
   * @param row     the data to put in the IRowSet
   * @param rowSet  the RoWset to put the row into.
   * @throws HopTransformException In case something unexpected goes wrong
   */
  public void putRowTo( IRowMeta rowMeta, Object[] row, IRowSet rowSet ) throws HopTransformException {
    getRowHandler().putRowTo( rowMeta, row, rowSet );

    // This transform is not reading data, only writing
    //
    if (firstRowReadDate==null) {
      firstRowReadDate = new Date();
    }
  }

  public void handlePutRowTo( IRowMeta rowMeta, Object[] row, IRowSet rowSet ) throws HopTransformException {

    // Are we pausing the transform? If so, stall forever...
    //
    while ( paused.get() && !stopped.get() ) {
      try {
        Thread.sleep( 1 );
      } catch ( InterruptedException e ) {
        throw new HopTransformException( e );
      }
    }

    // call all row listeners...
    //
    for ( IRowListener listener : rowListeners ) {
      listener.rowWrittenEvent( rowMeta, row );
    }

    // Keep adding to terminator_rows buffer...
    if ( terminator && terminatorRows != null ) {
      try {
        terminatorRows.add( rowMeta.cloneRow( row ) );
      } catch ( HopValueException e ) {
        throw new HopTransformException( "Unable to clone row while adding rows to the terminator buffer", e );
      }
    }

    if ( stopped.get() ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "BaseTransform.Log.StopPuttingARow" ) );
      }
      stopAll();
      return;
    }

    // Don't distribute or anything, only go to this rowset!
    //
    while ( !rowSet.putRow( rowMeta, row ) ) {
      if ( isStopped() ) {
        break;
      }
    }
    incrementLinesWritten();
  }

  /**
   * Put error.
   *
   * @param rowMeta           the row meta
   * @param row               the row
   * @param nrErrors          the nr errors
   * @param errorDescriptions the error descriptions
   * @param fieldNames        the field names
   * @param errorCodes        the error codes
   * @throws HopTransformException the hop transform exception
   */
  public void putError( IRowMeta rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                        String fieldNames, String errorCodes ) throws HopTransformException {
    getRowHandler().putError( rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes );
  }


  private void handlePutError( IVariables variables, IRowMeta rowMeta, Object[] row,
    long nrErrors, String errorDescriptions, String fieldNames, String errorCodes
  ) throws HopTransformException {
    if ( pipeline.isSafeModeEnabled() ) {
      if ( row==null || rowMeta.size() > row.length ) {
        throw new HopTransformException( BaseMessages.getString(
          PKG, "BaseTransform.Exception.MetadataDoesntMatchDataRowSize", Integer.toString( rowMeta.size() ), Integer.toString( row != null ? row.length : 0 ) ) );
      }
    }

    TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();

    if ( errorRowMeta == null ) {
      errorRowMeta = rowMeta.clone();

      IRowMeta add = transformErrorMeta.getErrorRowMeta( variables );
      errorRowMeta.addRowMeta( add );
    }

    Object[] errorRowData = RowDataUtil.allocateRowData( errorRowMeta.size() );
    if ( row != null ) {
      System.arraycopy( row, 0, errorRowData, 0, rowMeta.size() );
    }

    // Also add the error fields...
    transformErrorMeta.addErrorRowData( this, errorRowData, rowMeta.size(), nrErrors, errorDescriptions, fieldNames, errorCodes );

    // call all row listeners...
    for ( IRowListener listener : rowListeners ) {
      listener.errorRowWrittenEvent( rowMeta, row );
    }

    if ( errorRowSet != null ) {
      while ( !errorRowSet.putRow( errorRowMeta, errorRowData ) ) {
        if ( isStopped() ) {
          break;
        }
      }
      incrementLinesRejected();
    }

    verifyRejectionRates();
  }

  /**
   * Verify rejection rates.
   */
  private void verifyRejectionRates() {
    TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
    if ( transformErrorMeta == null ) {
      return; // nothing to verify.
    }

    // Was this one error too much?
    if ( maxErrors > 0 && getLinesRejected() > maxErrors ) {
      logError( BaseMessages.getString( PKG, "BaseTransform.Log.TooManyRejectedRows", Long.toString( maxErrors ), Long
        .toString( getLinesRejected() ) ) );
      setErrors( 1L );
      stopAll();
    }

    if ( maxPercentErrors > 0
      && getLinesRejected() > 0
      && ( minRowsForMaxErrorPercent <= 0 || getLinesRead() >= minRowsForMaxErrorPercent ) ) {
      int pct =
        (int) Math.ceil( 100 * (double) getLinesRejected() / getLinesRead() ); // additional conversion for PDI-10210
      if ( pct > maxPercentErrors ) {
        logError( BaseMessages.getString(
          PKG, "BaseTransform.Log.MaxPercentageRejectedReached", Integer.toString( pct ), Long
            .toString( getLinesRejected() ), Long.toString( getLinesRead() ) ) );
        setErrors( 1L );
        stopAll();
      }
    }
  }

  /**
   * Current input stream.
   *
   * @return the row set
   */
  @VisibleForTesting
  IRowSet currentInputStream() {
    inputRowSetsLock.readLock().lock();
    try {
      return inputRowSets.get( currentInputRowSetNr );
    } finally {
      inputRowSetsLock.readLock().unlock();
    }
  }

  /**
   * Find the next not-finished input-stream... in_handling says which one...
   */
  private void nextInputStream() {
    blockPointer = 0;

    int streams = inputRowSets.size();

    // No more streams left: exit!
    if ( streams == 0 ) {
      return;
    }

    // Just the one rowSet (common case)
    if ( streams == 1 ) {
      currentInputRowSetNr = 0;
    }

    // If we have some left: take the next!
    currentInputRowSetNr++;
    if ( currentInputRowSetNr >= streams ) {
      currentInputRowSetNr = 0;
    }
  }

  /**
   * Wait until the pipeline is completely running and all threads have been started.
   */
  protected void waitUntilPipelineIsStarted() {
    // Have all threads started?
    // Are we running yet? If not, wait a bit until all threads have been
    // started.
    //
    if ( this.checkPipelineRunning == false ) {
      while ( !pipeline.isRunning() && !stopped.get() ) {
        try {
          Thread.sleep( 1 );
        } catch ( InterruptedException e ) {
          // Ignore sleep interruption exception
        }
      }
      this.checkPipelineRunning = true;
    }
  }


  /**
   * In case of getRow, we receive data from previous transforms through the input rowset. In case we split the stream, we
   * have to copy the data to the alternate splits: rowsets 1 through n.
   */
  @Override
  public Object[] getRow() throws HopException {
    Object[] row = getRowHandler().getRow();

    if (firstRowReadDate==null) {
      firstRowReadDate = new Date();
    }

    return row;
  }


  private Object[] handleGetRow() throws HopException {

    // Are we pausing the transform? If so, stall forever...
    //
    while ( paused.get() && !stopped.get() ) {
      try {
        Thread.sleep( 100 );
      } catch ( InterruptedException e ) {
        throw new HopTransformException( e );
      }
    }

    if ( stopped.get() ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "BaseTransform.Log.StopLookingForMoreRows" ) );
      }
      stopAll();
      return null;
    }

    // Small startup check
    //
    waitUntilPipelineIsStarted();

    IRowSet inputRowSet = null;
    Object[] row = null;

    inputRowSetsLock.readLock().lock();
    try {

      // If everything is finished, we can stop immediately!
      //
      if ( inputRowSets.isEmpty() ) {
        return null;
      }

      // Do we need to switch to the next input stream?
      if ( blockPointer >= NR_OF_ROWS_IN_BLOCK ) {

        // Take a peek at the next input stream.
        // If there is no data, process another NR_OF_ROWS_IN_BLOCK on the next
        // input stream.
        //
        for ( int r = 0; r < inputRowSets.size() && row == null; r++ ) {
          nextInputStream();
          inputRowSet = currentInputStream();
          row = inputRowSet.getRowImmediate();
        }
        if ( row != null ) {
          incrementLinesRead();
        }
      } else {
        // What's the current input stream?
        inputRowSet = currentInputStream();
      }

      // To reduce stress on the locking system we are going to allow
      // The buffer to grow beyond "a few" entries.
      // We'll only do that if the previous transform has not ended...

      if ( !inputRowSet.isDone() && inputRowSet.size() <= lowerBufferBoundary && !isStopped() ) {
        try {
          Thread.sleep( 0, 1 );
        } catch ( InterruptedException e ) {
          // Ignore sleep interruption exception
        }
      }

      // See if this transform is receiving partitioned data...
      // In that case it might be the case that one input row set is receiving
      // all data and
      // the other rowsets nothing. (repartitioning on the same key would do
      // that)
      //
      // We never guaranteed that the input rows would be read one by one
      // alternatively.
      // So in THIS particular case it is safe to just read 100 rows from one
      // rowset, then switch to another etc.
      // We can use timeouts to switch from one to another...
      //
      while ( row == null && !isStopped() ) {
        // Get a row from the input in row set ...
        // Timeout immediately if nothing is there to read.
        // We will then switch to the next row set to read from...
        //
        row = inputRowSet.getRowWait( 1, TimeUnit.MILLISECONDS );
        if ( row != null ) {
          incrementLinesRead();
          blockPointer++;
        } else {
          // Try once more...
          // If row is still empty and the row set is done, we remove the row
          // set from
          // the input stream and move on to the next one...
          //
          if ( inputRowSet.isDone() ) {
            row = inputRowSet.getRowWait( 1, TimeUnit.MILLISECONDS );
            if ( row == null ) {

              // Must release the read lock before acquisition of the write lock to prevent deadlocks.
              inputRowSetsLock.readLock().unlock();

              // Another thread might acquire the write lock before we do,
              // and invalidate the data we have just read.
              //
              // This is actually fine, until we only want to remove the current rowSet - ArrayList ignores non-existing
              // elements when removing.
              inputRowSetsLock.writeLock().lock();
              try {
                inputRowSets.remove( inputRowSet );
                if ( inputRowSets.isEmpty() ) {
                  return null; // We're completely done.
                }
              } finally {
                inputRowSetsLock.readLock().lock(); // downgrade to read lock
                inputRowSetsLock.writeLock().unlock();
              }
            } else {
              incrementLinesRead();
            }
          }
          nextInputStream();
          inputRowSet = currentInputStream();
        }
      }

      // This rowSet is perhaps no longer giving back rows?
      //
      while ( row == null && !stopped.get() ) {
        // Try the next input row set(s) until we find a row set that still has
        // rows...
        // The getRowFrom() method removes row sets from the input row sets
        // list.
        //
        if ( inputRowSets.isEmpty() ) {
          return null; // We're done.
        }

        nextInputStream();
        inputRowSet = currentInputStream();
        row = getRowFrom( inputRowSet );
      }
    } finally {
      inputRowSetsLock.readLock().unlock();
    }

    // Also set the meta data on the first occurrence.
    // or if prevTransforms.length > 1 inputRowMeta can be changed
    if ( inputRowMeta == null || prevTransforms.length > 1 ) {
      inputRowMeta = inputRowSet.getRowMeta();
    }

    if ( row != null ) {
      // OK, before we return the row, let's see if we need to check on mixing
      // row compositions...
      //
      if ( pipeline.isSafeModeEnabled() ) {
        pipelineMeta.checkRowMixingStatically( this, transformMeta, null );
      }

      for ( IRowListener listener : rowListeners ) {
        listener.rowReadEvent( inputRowMeta, row );
      }
    }

    // Check the rejection rates etc. as well.
    verifyRejectionRates();

    return row;
  }

  /**
   * IRowHandler controls how getRow/putRow are handled.
   * The default IRowHandler will simply call
   * {@link #handleGetRow()} and {@link #handlePutRow(IRowMeta, Object[])}
   */
  public void setRowHandler( IRowHandler rowHandler ) {
    Preconditions.checkNotNull( rowHandler );
    this.rowHandler = rowHandler;
  }

  public IRowHandler getRowHandler() {
    if ( rowHandler == null ) {
      rowHandler = new DefaultRowHandler();
    }
    return this.rowHandler;
  }

  /**
   * Safe mode checking.
   *
   * @param row the row
   * @throws HopRowException the hop row exception
   */
  protected void safeModeChecking( IRowMeta row ) throws HopRowException {
    if ( row == null ) {
      return;
    }

    if ( inputReferenceRow == null ) {
      inputReferenceRow = row.clone(); // copy it!

      // Check for double field names.
      //
      String[] fieldnames = row.getFieldNames();
      Arrays.sort( fieldnames );
      for ( int i = 0; i < fieldnames.length - 1; i++ ) {
        if ( fieldnames[ i ].equals( fieldnames[ i + 1 ] ) ) {
          throw new HopRowException( BaseMessages.getString(
            PKG, "BaseTransform.SafeMode.Exception.DoubleFieldnames", fieldnames[ i ] ) );
        }
      }
    } else {
      safeModeChecking( inputReferenceRow, row );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#identifyErrorOutput()
   */
  @Override
  public void identifyErrorOutput() {
    if ( transformMeta.isDoingErrorHandling() ) {
      TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
      outputRowSetsLock.writeLock().lock();
      try {
        for ( int rowsetNr = 0; rowsetNr < outputRowSets.size(); rowsetNr++ ) {
          IRowSet outputRowSet = outputRowSets.get( rowsetNr );
          if ( outputRowSet.getDestinationTransformName().equalsIgnoreCase( transformErrorMeta.getTargetTransform().getName() ) ) {
            // This is the rowset to move!
            //
            errorRowSet = outputRowSet;
            outputRowSets.remove( rowsetNr );
            return;
          }
        }
      } finally {
        outputRowSetsLock.writeLock().unlock();
      }
    }
  }

  /**
   * Safe mode checking.
   *
   * @param referenceRowMeta the reference row meta
   * @param rowMeta          the row meta
   * @throws HopRowException the hop row exception
   */
  public static void safeModeChecking( IRowMeta referenceRowMeta, IRowMeta rowMeta )
    throws HopRowException {
    // See if the row we got has the same layout as the reference row.
    // First check the number of fields
    //
    if ( referenceRowMeta.size() != rowMeta.size() ) {
      throw new HopRowException( BaseMessages.getString( PKG, "BaseTransform.SafeMode.Exception.VaryingSize", ""
        + referenceRowMeta.size(), "" + rowMeta.size(), rowMeta.toString() ) );
    } else {
      // Check field by field for the position of the names...
      for ( int i = 0; i < referenceRowMeta.size(); i++ ) {
        IValueMeta referenceValue = referenceRowMeta.getValueMeta( i );
        IValueMeta compareValue = rowMeta.getValueMeta( i );

        if ( !referenceValue.getName().equalsIgnoreCase( compareValue.getName() ) ) {
          throw new HopRowException( BaseMessages.getString(
            PKG, "BaseTransform.SafeMode.Exception.MixingLayout", "" + ( i + 1 ), referenceValue.getName()
              + " " + referenceValue.toStringMeta(), compareValue.getName()
              + " " + compareValue.toStringMeta() ) );
        }

        if ( referenceValue.getType() != compareValue.getType() ) {
          throw new HopRowException( BaseMessages.getString( PKG, "BaseTransform.SafeMode.Exception.MixingTypes", ""
            + ( i + 1 ), referenceValue.getName() + " " + referenceValue.toStringMeta(), compareValue.getName()
            + " " + compareValue.toStringMeta() ) );
        }

        if ( referenceValue.getStorageType() != compareValue.getStorageType() ) {
          throw new HopRowException( BaseMessages.getString(
            PKG, "BaseTransform.SafeMode.Exception.MixingStorageTypes", "" + ( i + 1 ), referenceValue.getName()
              + " " + referenceValue.toStringMeta(), compareValue.getName()
              + " " + compareValue.toStringMeta() ) );
        }
      }
    }
  }

  /**
   * Gets the row from.
   *
   * @param rowSet the row set
   * @return the row from
   * @throws HopTransformException the hop transform exception
   */
  public Object[] getRowFrom( IRowSet rowSet ) throws HopTransformException {
    Object[] row = getRowHandler().getRowFrom( rowSet );
    if (firstRowReadDate==null) {
      firstRowReadDate = new Date();
    }
    return row;
  }

  public Object[] handleGetRowFrom( IRowSet rowSet ) throws HopTransformException {
    // Are we pausing the transform? If so, stall forever...
    //
    while ( paused.get() && !stopped.get() ) {
      try {
        Thread.sleep( 10 );
      } catch ( InterruptedException e ) {
        throw new HopTransformException( e );
      }
    }

    // Have all threads started?
    // Are we running yet? If not, wait a bit until all threads have been
    // started.
    if ( this.checkPipelineRunning == false ) {
      while ( !pipeline.isRunning() && !stopped.get() ) {
        try {
          Thread.sleep( 1 );
        } catch ( InterruptedException e ) {
          // Ignore sleep interruption exception
        }
      }
      this.checkPipelineRunning = true;
    }
    Object[] rowData = null;

    // To reduce stress on the locking system we are going to allow
    // The buffer to grow beyond "a few" entries.
    // We'll only do that if the previous transform has not ended...

    if ( !rowSet.isDone() && rowSet.size() <= lowerBufferBoundary && !isStopped() ) {
      try {
        Thread.sleep( 0, 1 );
      } catch ( InterruptedException e ) {
        // Ignore sleep interruption exception
      }
    }

    // Grab a row... If nothing received after a timeout, try again.
    //
    rowData = rowSet.getRow();
    while ( rowData == null && !rowSet.isDone() && !stopped.get() ) {
      rowData = rowSet.getRow();

      // Verify deadlocks!
      //
      /*
       * if (rowData==null) { if (getInputRowSets().size()>1 && getLinesRead()==deadLockCounter) {
       * verifyInputDeadLock(); } deadLockCounter=getLinesRead(); }
       */
    }

    // Still nothing: no more rows to be had?
    //
    if ( rowData == null && rowSet.isDone() ) {
      // Try one more time to get a row to make sure we don't get a
      // race-condition between the get and the isDone()
      //
      rowData = rowSet.getRow();
    }

    if ( stopped.get() ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "BaseTransform.Log.StopLookingForMoreRows" ) );
      }
      stopAll();
      return null;
    }

    if ( rowData == null && rowSet.isDone() ) {
      // Try one more time...
      //
      rowData = rowSet.getRow();
      if ( rowData == null ) {

        // Must release the read lock before acquisition of the write lock to prevent deadlocks.
        //
        // But #handleGetRowFrom() can be called either from outside or from handleGetRow().
        // So a current thread might hold the read lock (possibly reentrantly) and might not.
        // We therefore must release it conditionally.
        int holdCount = inputRowSetsLock.getReadHoldCount();
        for ( int i = 0; i < holdCount; i++ ) {
          inputRowSetsLock.readLock().unlock();
        }
        // Just like in handleGetRow() method, an another thread might acquire the write lock before we do.
        // Here this is also fine, until we only want to remove the given rowSet - ArrayList ignores non-existing
        // elements when removing.
        inputRowSetsLock.writeLock().lock();
        try {
          inputRowSets.remove( rowSet );

          // Downgrade to read lock by restoring to the previous state before releasing the write lock
          for ( int i = 0; i < holdCount; i++ ) {
            inputRowSetsLock.readLock().lock();
          }

          return null;
        } finally {
          inputRowSetsLock.writeLock().unlock();
        }
      }
    }
    incrementLinesRead();

    // call all rowlisteners...
    //
    for ( IRowListener listener : rowListeners ) {
      listener.rowReadEvent( rowSet.getRowMeta(), rowData );
    }

    return rowData;
  }

  /**
   * Find input row set.
   *
   * @param sourceTransformName the source transform
   * @return the row set
   * @throws HopTransformException the hop transform exception
   */
  public IRowSet findInputRowSet( String sourceTransformName ) throws HopTransformException {
    // Check to see that "sourceTransform" only runs in a single copy
    // Otherwise you'll see problems during execution.
    //
    TransformMeta sourceTransformMeta = pipelineMeta.findTransform( sourceTransformName );
    if ( sourceTransformMeta == null ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "BaseTransform.Exception.SourceTransformToReadFromDoesntExist", sourceTransformName ) );
    }

    // If this transform is partitioned but the source isn't, throw an error
    //
    if (transformMeta.isPartitioned() && !sourceTransformMeta.isPartitioned()) {
      throw new HopTransformException("When reading from info transforms and running partitioned the source transform needs to be partitioned in the same way");
    }

    // If the source transform is partitioned but this one isn't, throw an error
    //
    if (transformMeta.isPartitioned() && !sourceTransformMeta.isPartitioned()) {
      throw new HopTransformException("The info transform to read data from called ["+sourceTransformMeta.getName()+"] is partitioned when transform ["+getTransformName()+"] isn't.  Make sure both are partitioned or neither.");
    }

    // If both transforms are partitioned it's OK to run in parallel...
    //
    if (sourceTransformMeta.isPartitioned() && !sourceTransformMeta.isRepartitioning() &&
        transformMeta.isPartitioned()) {
      if (!sourceTransformMeta.getTransformPartitioningMeta().equals( transformMeta.getTransformPartitioningMeta() )) {
        throw new HopTransformException("When reading from info transforms and running partitioned the source transform needs to be partitioned in the same way");
      }
      return findInputRowSet( sourceTransformName, getCopy(), getTransformName(), getCopy() );
    }



    if ( sourceTransformMeta.getCopies(this) > 1 ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "BaseTransform.Exception.SourceTransformToReadFromCantRunInMultipleCopies", sourceTransformName, Integer
          .toString( sourceTransformMeta.getCopies(this) ) ) );
    }

    return findInputRowSet( sourceTransformName, 0, getTransformName(), getCopy() );
  }

  /**
   * Find input row set.
   *
   * @param from     the from
   * @param fromCopy the fromcopy
   * @param to       the to
   * @param toCopy   the tocopy
   * @return the row set
   */
  public IRowSet findInputRowSet( String from, int fromCopy, String to, int toCopy ) {
    inputRowSetsLock.readLock().lock();
    try {
      for ( IRowSet rs : inputRowSets ) {
        if ( rs.getOriginTransformName().equalsIgnoreCase( from )
          && rs.getDestinationTransformName().equalsIgnoreCase( to ) && rs.getOriginTransformCopy() == fromCopy
          && rs.getDestinationTransformCopy() == toCopy ) {
          return rs;
        }
      }
    } finally {
      inputRowSetsLock.readLock().unlock();
    }

    return null;
  }

  /**
   * Find output row set.
   *
   * @param targetTransform the target transform
   * @return the row set
   * @throws HopTransformException the hop transform exception
   */
  public IRowSet findOutputRowSet( String targetTransform ) throws HopTransformException {

    // Check to see that "targetTransform" only runs in a single copy
    // Otherwise you'll see problems during execution.
    //
    TransformMeta targetTransformMeta = pipelineMeta.findTransform( targetTransform );
    if ( targetTransformMeta == null ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "BaseTransform.Exception.TargetTransformToWriteToDoesntExist", targetTransform ) );
    }

    if ( targetTransformMeta.getCopies(this) > 1 ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "BaseTransform.Exception.TargetTransformToWriteToCantRunInMultipleCopies", targetTransform, Integer
          .toString( targetTransformMeta.getCopies(this) ) ) );
    }

    return findOutputRowSet( getTransformName(), getCopy(), targetTransform, 0 );
  }

  /**
   * Find an output rowset in a running pipeline. It will also look at the "to" transform to see if this is a mapping.
   * If it is, it will find the appropriate rowset in that pipeline.
   *
   * @param from
   * @param fromcopy
   * @param to
   * @param tocopy
   * @return The rowset or null if none is found.
   */
  public IRowSet findOutputRowSet( String from, int fromcopy, String to, int tocopy ) {
    outputRowSetsLock.readLock().lock();
    try {
      for ( IRowSet rs : outputRowSets ) {
        if ( rs.getOriginTransformName().equalsIgnoreCase( from )
          && rs.getDestinationTransformName().equalsIgnoreCase( to ) && rs.getOriginTransformCopy() == fromcopy
          && rs.getDestinationTransformCopy() == tocopy ) {
          return rs;
        }
      }
    } finally {
      outputRowSetsLock.readLock().unlock();
    }

    // Still nothing found!
    //
    return null;
  }

  //
  // We have to tell the next transform we're finished with
  // writing to output rowset(s)!
  //
  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#setOutputDone()
   */
  @Override
  public void setOutputDone() {
    outputRowSetsLock.readLock().lock();
    try {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "BaseTransform.Log.OutputDone", String.valueOf( outputRowSets.size() ) ) );
      }
      for ( IRowSet rs : outputRowSets ) {
        rs.setDone();
      }
      if ( errorRowSet != null ) {
        errorRowSet.setDone();
      }
    } finally {
      lastRowWrittenDate = new Date();
      outputRowSetsLock.readLock().unlock();
    }
  }

  /**
   * This method finds the surrounding transforms and rowsets for this base transform. This transforms keeps it's own list of rowsets
   * (etc.) to prevent it from having to search every time.
   * <p>
   * Note that all rowsets input and output is already created by pipeline itself. So
   * in this place we will look and choose which rowsets will be used by this particular transform.
   * <p>
   * We will collect all input rowsets and output rowsets so transform will be able to read input data,
   * and write to the output.
   * <p>
   * Transforms can run in multiple copies, on in partitioned fashion. For this case we should take
   * in account that in different cases we should take in account one to one, one to many and other cases
   * properly.
   */
  public void dispatch() {
    if ( pipelineMeta == null ) { // for preview reasons, no dispatching is done!
      return;
    }

    TransformMeta transformMeta = pipelineMeta.findTransform( transformName );

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.StartingBuffersAllocation" ) );
    }

    // How many next transforms are there? 0, 1 or more??
    // How many transforms do we send output to?
    List<TransformMeta> previousTransforms = pipelineMeta.findPreviousTransforms( transformMeta, true );
    List<TransformMeta> succeedingTransforms = pipelineMeta.findNextTransforms( transformMeta );

    int nrInput = previousTransforms.size();
    int nrOutput = succeedingTransforms.size();

    inputRowSetsLock.writeLock().lock();
    outputRowSetsLock.writeLock().lock();
    try {
      inputRowSets = new ArrayList<>();
      outputRowSets = new ArrayList<>();

      errorRowSet = null;
      prevTransforms = new TransformMeta[ nrInput ];
      nextTransforms = new TransformMeta[ nrOutput ];

      currentInputRowSetNr = 0; // we start with input[0];

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.TransformMeta", String.valueOf( nrInput ), String
          .valueOf( nrOutput ) ) );
      }
      // populate input rowsets.
      for ( int i = 0; i < previousTransforms.size(); i++ ) {
        prevTransforms[ i ] = previousTransforms.get( i );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "BaseTransform.Log.GotPreviousTransform", transformName, String.valueOf( i ), prevTransforms[ i ].getName() ) );
        }

        // Looking at the previous transform, you can have either 1 rowset to look at or more then one.
        int prevCopies = prevTransforms[ i ].getCopies(this);
        int nextCopies = transformMeta.getCopies(this);
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "BaseTransform.Log.InputRowInfo", String.valueOf( prevCopies ), String.valueOf( nextCopies ) ) );
        }

        int nrCopies;
        int dispatchType;
        boolean repartitioning;
        if ( prevTransforms[ i ].isPartitioned() ) {
          repartitioning = !prevTransforms[ i ].getTransformPartitioningMeta()
            .equals( transformMeta.getTransformPartitioningMeta() );
        } else {
          repartitioning = transformMeta.isPartitioned();
        }

        if ( prevCopies == 1 && nextCopies == 1 ) {
          // normal hop
          dispatchType = Pipeline.TYPE_DISP_1_1;
          nrCopies = 1;
        } else if ( prevCopies == 1 && nextCopies > 1 ) {
          // one to many hop
          dispatchType = Pipeline.TYPE_DISP_1_N;
          nrCopies = 1;
        } else if ( prevCopies > 1 && nextCopies == 1 ) {
          // from many to one hop
          dispatchType = Pipeline.TYPE_DISP_N_1;
          nrCopies = prevCopies;
        } else if ( prevCopies == nextCopies && !repartitioning ) {
          // this may be many-to-many or swim-lanes hop
          dispatchType = Pipeline.TYPE_DISP_N_N;
          nrCopies = 1;
        } else { // > 1!
          dispatchType = Pipeline.TYPE_DISP_N_M;
          nrCopies = prevCopies;
        }

        for ( int c = 0; c < nrCopies; c++ ) {
          IRowSet rowSet = null;
          switch ( dispatchType ) {
            case Pipeline.TYPE_DISP_1_1:
              rowSet = pipeline.findRowSet( prevTransforms[ i ].getName(), 0, transformName, 0 );
              break;
            case Pipeline.TYPE_DISP_1_N:
              rowSet = pipeline.findRowSet( prevTransforms[ i ].getName(), 0, transformName, getCopy() );
              break;
            case Pipeline.TYPE_DISP_N_1:
              rowSet = pipeline.findRowSet( prevTransforms[ i ].getName(), c, transformName, 0 );
              break;
            case Pipeline.TYPE_DISP_N_N:
              rowSet = pipeline.findRowSet( prevTransforms[ i ].getName(), getCopy(), transformName, getCopy() );
              break;
            case Pipeline.TYPE_DISP_N_M:
              rowSet = pipeline.findRowSet( prevTransforms[ i ].getName(), c, transformName, getCopy() );
              break;
            default:
              break;
          }
          if ( rowSet != null ) {
            inputRowSets.add( rowSet );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.FoundInputRowset", rowSet.getName() ) );
            }
          } else {
            if ( !prevTransforms[ i ].isMapping() && !transformMeta.isMapping() ) {
              logError( BaseMessages.getString( PKG, "BaseTransform.Log.UnableToFindInputRowset" ) );
              setErrors( 1 );
              stopAll();
              return;
            }
          }
        }
      }
      // And now the output part!
      for ( int i = 0; i < nrOutput; i++ ) {
        nextTransforms[ i ] = succeedingTransforms.get( i );

        int prevCopies = transformMeta.getCopies(this);
        int nextCopies = nextTransforms[ i ].getCopies(this);

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "BaseTransform.Log.OutputRowInfo", String.valueOf( prevCopies ), String.valueOf( nextCopies ) ) );
        }

        int nrCopies;
        int dispatchType;
        boolean repartitioning;
        if ( transformMeta.isPartitioned() ) {
          repartitioning = !transformMeta.getTransformPartitioningMeta()
            .equals( nextTransforms[ i ].getTransformPartitioningMeta() );
        } else {
          repartitioning = nextTransforms[ i ].isPartitioned();
        }

        if ( prevCopies == 1 && nextCopies == 1 ) {
          dispatchType = Pipeline.TYPE_DISP_1_1;
          nrCopies = 1;
        } else if ( prevCopies == 1 && nextCopies > 1 ) {
          dispatchType = Pipeline.TYPE_DISP_1_N;
          nrCopies = nextCopies;
        } else if ( prevCopies > 1 && nextCopies == 1 ) {
          dispatchType = Pipeline.TYPE_DISP_N_1;
          nrCopies = 1;
        } else if ( prevCopies == nextCopies && !repartitioning ) {
          dispatchType = Pipeline.TYPE_DISP_N_N;
          nrCopies = 1;
        } else { // > 1!
          dispatchType = Pipeline.TYPE_DISP_N_M;
          nrCopies = nextCopies;
        }

        for ( int c = 0; c < nrCopies; c++ ) {
          IRowSet rowSet = null;
          switch ( dispatchType ) {
            case Pipeline.TYPE_DISP_1_1:
              rowSet = pipeline.findRowSet( transformName, 0, nextTransforms[ i ].getName(), 0 );
              break;
            case Pipeline.TYPE_DISP_1_N:
              rowSet = pipeline.findRowSet( transformName, 0, nextTransforms[ i ].getName(), c );
              break;
            case Pipeline.TYPE_DISP_N_1:
              rowSet = pipeline.findRowSet( transformName, getCopy(), nextTransforms[ i ].getName(), 0 );
              break;
            case Pipeline.TYPE_DISP_N_N:
              rowSet = pipeline.findRowSet( transformName, getCopy(), nextTransforms[ i ].getName(), getCopy() );
              break;
            case Pipeline.TYPE_DISP_N_M:
              rowSet = pipeline.findRowSet( transformName, getCopy(), nextTransforms[ i ].getName(), c );
              break;
            default:
              break;
          }
          if ( rowSet != null ) {
            outputRowSets.add( rowSet );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.FoundOutputRowset", rowSet.getName() ) );
            }
          } else {
            if ( !transformMeta.isMapping() && !nextTransforms[ i ].isMapping() ) {
              logError( BaseMessages.getString( PKG, "BaseTransform.Log.UnableToFindOutputRowset" ) );
              setErrors( 1 );
              stopAll();
              return;
            }
          }
        }
      }
    } finally {
      inputRowSetsLock.writeLock().unlock();
      outputRowSetsLock.writeLock().unlock();
    }

    if ( transformMeta.getTargetTransformPartitioningMeta() != null ) {
      nextTransformPartitioningMeta = transformMeta.getTargetTransformPartitioningMeta();
    }

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.FinishedDispatching" ) );
    }
  }

  /**
   * Checks if is basic.
   *
   * @return true, if is basic
   */
  public boolean isBasic() {
    return log.isBasic();
  }

  /**
   * Checks if is detailed.
   *
   * @return true, if is detailed
   */
  public boolean isDetailed() {
    return log.isDetailed();
  }

  /**
   * Checks if is debug.
   *
   * @return true, if is debug
   */
  public boolean isDebug() {
    return log.isDebug();
  }

  /**
   * Checks if is row level.
   *
   * @return true, if is row level
   */
  public boolean isRowLevel() {
    return log.isRowLevel();
  }

  /**
   * Log minimal.
   *
   * @param message the message
   */
  public void logMinimal( String message ) {
    log.logMinimal( message );
  }

  /**
   * Log minimal.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logMinimal( String message, Object... arguments ) {
    log.logMinimal( message, arguments );
  }

  /**
   * Log basic.
   *
   * @param message the message
   */
  public void logBasic( String message ) {
    log.logBasic( message );
  }

  /**
   * Log basic.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logBasic( String message, Object... arguments ) {
    log.logBasic( message, arguments );
  }

  /**
   * Log detailed.
   *
   * @param message the message
   */
  public void logDetailed( String message ) {
    log.logDetailed( message );
  }

  /**
   * Log detailed.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logDetailed( String message, Object... arguments ) {
    log.logDetailed( message, arguments );
  }

  /**
   * Log debug.
   *
   * @param message the message
   */
  public void logDebug( String message ) {
    log.logDebug( message );
  }

  /**
   * Log debug.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logDebug( String message, Object... arguments ) {
    log.logDebug( message, arguments );
  }

  /**
   * Log rowlevel.
   *
   * @param message the message
   */
  public void logRowlevel( String message ) {
    log.logRowlevel( message );
  }

  /**
   * Log rowlevel.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logRowlevel( String message, Object... arguments ) {
    log.logRowlevel( message, arguments );
  }

  /**
   * Log error.
   *
   * @param message the message
   */
  public void logError( String message ) {
    log.logError( message );
  }

  /**
   * Log error.
   *
   * @param message the message
   * @param e       the e
   */
  public void logError( String message, Throwable e ) {
    log.logError( message, e );
  }

  /**
   * Log error.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logError( String message, Object... arguments ) {
    log.logError( message, arguments );
  }

  /**
   * Output is done.
   *
   * @return true, if successful
   */
  public boolean outputIsDone() {
    int nrstopped = 0;

    outputRowSetsLock.readLock().lock();
    try {
      for ( IRowSet rs : outputRowSets ) {
        if ( rs.isDone() ) {
          nrstopped++;
        }
      }
      return nrstopped >= outputRowSets.size();
    } finally {
      outputRowSetsLock.readLock().unlock();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#stopAll()
   */
  @Override
  public void stopAll() {
    stopped.set( true );
    pipeline.stopAll();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#isStopped()
   */
  @Override
  public boolean isStopped() {
    return stopped.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#isRunning()
   */
  @Override
  public boolean isRunning() {
    return running.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#isPaused()
   */
  @Override
  public boolean isPaused() {
    return paused.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#setStopped(boolean)
   */
  @Override
  public void setStopped( boolean stopped ) {
    this.stopped.set( stopped );
  }

  @Override
  public void setSafeStopped( boolean stopped ) {
    this.safeStopped.set( stopped );
  }

  @Override
  public boolean isSafeStopped() {
    return safeStopped.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#setRunning(boolean)
   */
  @Override
  public void setRunning( boolean running ) {
    this.running.set( running );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#pauseRunning()
   */
  @Override
  public void pauseRunning() {
    setPaused( true );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#resumeRunning()
   */
  @Override
  public void resumeRunning() {
    setPaused( false );
  }

  /**
   * Sets the paused.
   *
   * @param paused the new paused
   */
  public void setPaused( boolean paused ) {
    this.paused.set( paused );
  }

  /**
   * Sets the paused.
   *
   * @param paused the new paused
   */
  public void setPaused( AtomicBoolean paused ) {
    this.paused = paused;
  }

  /**
   * Checks if is initialising.
   *
   * @return true, if is initialising
   */
  public boolean isInitialising() {
    return init;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#markStart()
   */
  @Override
  public void markStart() {
    Calendar cal = Calendar.getInstance();
    startTime = cal.getTime();

    setInternalVariables();
  }

  /**
   * Sets the internal variables.
   */
  public void setInternalVariables() {
    setVariable( Const.INTERNAL_VARIABLE_TRANSFORM_NAME, transformName );
    setVariable( Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, Integer.toString( getCopy() ) );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#markStop()
   */
  @Override
  public synchronized void markStop() {

    // Only mark a transform as stopped once
    //
    if ( !markStopped.get() ) {
      markStopped.set( true );

      Calendar cal = Calendar.getInstance();
      stopTime = cal.getTime();

      // Here we are completely done with the pipeline.
      // Call all the attached listeners and notify the outside world that the transform has finished.
      //
      fireTransformFinishedListeners();

      // We're finally completely done with this transform.
      //
      setRunning( false );
    }
  }

  private synchronized void fireTransformFinishedListeners() {
    synchronized ( transformFinishedListeners ) {
      for ( ITransformFinishedListener transformListener : transformFinishedListeners ) {
        transformListener.transformFinished( pipeline, transformMeta, this );
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getRuntime()
   */
  @Override
  public long getExecutionDuration() {
    long lapsed;
    if ( startTime != null && stopTime == null ) {
      Calendar cal = Calendar.getInstance();
      long now = cal.getTimeInMillis();
      long st = startTime.getTime();
      lapsed = now - st;
    } else if ( startTime != null && stopTime != null ) {
      lapsed = stopTime.getTime() - startTime.getTime();
    } else {
      lapsed = 0;
    }

    return lapsed;
  }

  @Override public long getInputBufferSize() {
    long total = 0L;
    for ( IRowSet inputRowSet : getInputRowSets() ) {
      return inputRowSet.size();
    }
    return total;
  }

  @Override public long getOutputBufferSize() {
    long total = 0L;
    for ( IRowSet outputRowSet : getOutputRowSets() ) {
      return outputRowSet.size();
    }
    return total;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder string = new StringBuilder( 50 );

    if ( !Utils.isEmpty( partitionId ) ) {
      string.append( transformName ).append( '.' ).append( partitionId );
    } else {
      string.append( transformName ).append( '.' ).append( getCopy() );
    }

    return string.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#rowsetOutputSize()
   */
  @Override
  public int rowsetOutputSize() {
    int size = 0;

    outputRowSetsLock.readLock().lock();
    try {
      for ( IRowSet outputRowSet : outputRowSets ) {
        size += outputRowSet.size();
      }
    } finally {
      outputRowSetsLock.readLock().unlock();
    }

    return size;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#rowsetInputSize()
   */
  @Override
  public int rowsetInputSize() {
    int size = 0;

    inputRowSetsLock.readLock().lock();
    try {
      for ( IRowSet inputRowSet : inputRowSets ) {
        size += inputRowSet.size();
      }
    } finally {
      inputRowSetsLock.readLock().unlock();
    }

    return size;
  }

  /**
   * Perform actions to stop a running transform. This can be stopping running SQL queries (cancel), etc. Default it doesn't
   * do anything.
   *
   * @throws HopException in case something goes wrong
   */
  @Override public void stopRunning() throws HopException {
    // Nothing by default
  }

  /**
   * Log summary.
   */
  public void logSummary() {
    synchronized ( statusCountersLock ) {
      long li = getLinesInput();
      long lo = getLinesOutput();
      long lr = getLinesRead();
      long lw = getLinesWritten();
      long lu = getLinesUpdated();
      long lj = getLinesRejected();
      if ( li > 0 || lo > 0 || lr > 0 || lw > 0 || lu > 0 || lj > 0 || errors > 0 ) {
        logBasic( BaseMessages.getString( PKG, "BaseTransform.Log.SummaryInfo", String.valueOf( li ), String
          .valueOf( lo ), String.valueOf( lr ), String.valueOf( lw ), String.valueOf( lw ), String
          .valueOf( errors + lj ) ) );
      } else {
        logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.SummaryInfo", String.valueOf( li ), String
          .valueOf( lo ), String.valueOf( lr ), String.valueOf( lw ), String.valueOf( lw ), String
          .valueOf( errors + lj ) ) );
      }
    }
  }


  @Override
  public String getTransformPluginId() {
    if ( transformMeta != null ) {
      return transformMeta.getTransformPluginId();
    }
    return null;
  }

  /**
   * @return Returns the inputRowSets.
   */
  @Override
  public List<IRowSet> getInputRowSets() {
    inputRowSetsLock.readLock().lock();
    try {
      return new ArrayList<>( inputRowSets );
    } finally {
      inputRowSetsLock.readLock().unlock();
    }
  }

  @Override
  public void addRowSetToInputRowSets( IRowSet rowSet ) {
    inputRowSetsLock.writeLock().lock();
    try {
      inputRowSets.add( rowSet );
    } finally {
      inputRowSetsLock.writeLock().unlock();
    }
  }

  protected IRowSet getFirstInputRowSet() {
    inputRowSetsLock.readLock().lock();
    try {
      return inputRowSets.get( 0 );
    } finally {
      inputRowSetsLock.readLock().unlock();
    }
  }

  protected void clearInputRowSets() {
    inputRowSetsLock.writeLock().lock();
    try {
      inputRowSets.clear();
    } finally {
      inputRowSetsLock.writeLock().unlock();
    }
  }

  protected void swapFirstInputRowSetIfExists( String transformName ) {
    inputRowSetsLock.writeLock().lock();
    try {
      for ( int i = 0; i < inputRowSets.size(); i++ ) {
        BlockingRowSet rs = (BlockingRowSet) inputRowSets.get( i );
        if ( rs.getOriginTransformName().equalsIgnoreCase( transformName ) ) {
          // swap this one and position 0...that means, the main stream is always stream 0 --> easy!
          //
          BlockingRowSet zero = (BlockingRowSet) inputRowSets.get( 0 );
          inputRowSets.set( 0, rs );
          inputRowSets.set( i, zero );
        }
      }
    } finally {
      inputRowSetsLock.writeLock().unlock();
    }
  }

  /**
   * @param inputRowSets The inputRowSets to set.
   */
  public void setInputRowSets( List<IRowSet> inputRowSets ) {
    inputRowSetsLock.writeLock().lock();
    try {
      this.inputRowSets = inputRowSets;
    } finally {
      inputRowSetsLock.writeLock().unlock();
    }
  }

  /**
   * @return Returns the outputRowSets.
   */
  @Override
  public List<IRowSet> getOutputRowSets() {
    outputRowSetsLock.readLock().lock();
    try {
      return new ArrayList<>( outputRowSets );
    } finally {
      outputRowSetsLock.readLock().unlock();
    }
  }

  @Override
  public void addRowSetToOutputRowSets( IRowSet rowSet ) {
    outputRowSetsLock.writeLock().lock();
    try {
      outputRowSets.add( rowSet );
    } finally {
      outputRowSetsLock.writeLock().unlock();
    }
  }

  protected void clearOutputRowSets() {
    outputRowSetsLock.writeLock().lock();
    try {
      outputRowSets.clear();
    } finally {
      outputRowSetsLock.writeLock().unlock();
    }
  }

  /**
   * @param outputRowSets The outputRowSets to set.
   */
  public void setOutputRowSets( List<IRowSet> outputRowSets ) {
    outputRowSetsLock.writeLock().lock();
    try {
      this.outputRowSets = outputRowSets;
    } finally {
      outputRowSetsLock.writeLock().unlock();
    }
  }

  /**
   * @return Returns the distributed.
   */
  public boolean isDistributed() {
    return distributed;
  }

  /**
   * @param distributed The distributed to set.
   */
  public void setDistributed( boolean distributed ) {
    this.distributed = distributed;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#addRowListener(org.apache.hop.pipeline.transform.IRowListener)
   */
  @Override
  public void addRowListener( IRowListener rowListener ) {
    rowListeners.add( rowListener );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#removeRowListener(org.apache.hop.pipeline.transform.IRowListener)
   */
  @Override
  public void removeRowListener( IRowListener rowListener ) {
    rowListeners.remove( rowListener );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getRowListeners()
   */
  @Override
  public List<IRowListener> getRowListeners() {
    return Collections.unmodifiableList( rowListeners );
  }

  /**
   * Adds the result file.
   *
   * @param resultFile the result file
   */
  public void addResultFile( ResultFile resultFile ) {
    ReentrantReadWriteLock.WriteLock lock = resultFilesLock.writeLock();
    lock.lock();
    try {
      resultFiles.put( resultFile.getFile().toString(), resultFile );
    } finally {
      lock.unlock();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getResultFiles()
   */
  @Override
  public Map<String, ResultFile> getResultFiles() {
    ReentrantReadWriteLock.ReadLock lock = resultFilesLock.readLock();
    lock.lock();
    try {
      return new HashMap<>( this.resultFiles );
    } finally {
      lock.unlock();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getStatus()
   */
  @Override
  public ComponentExecutionStatus getStatus() {
    // Is this thread alive or not?
    //
    if ( isRunning() ) {
      if ( isStopped() ) {
        return ComponentExecutionStatus.STATUS_HALTING;
      } else {
        if ( isPaused() ) {
          return ComponentExecutionStatus.STATUS_PAUSED;
        } else {
          return ComponentExecutionStatus.STATUS_RUNNING;
        }
      }
    } else {
      // Transform is not running... What are we doing?
      //
      // An init thread is running...
      //
      if ( pipeline.isPreparing() ) {
        if ( isInitialising() ) {
          return ComponentExecutionStatus.STATUS_INIT;
        } else {
          // Done initializing, but other threads are still busy.
          // So this transform is idle
          //
          return ComponentExecutionStatus.STATUS_IDLE;
        }
      } else {
        // It's not running, it's not initializing, so what is it doing?
        //
        if ( isStopped() ) {
          return ComponentExecutionStatus.STATUS_STOPPED;
        } else {
          // To be sure (race conditions and all), get the rest in ITransformData object:
          //
          if ( data.getStatus() == ComponentExecutionStatus.STATUS_DISPOSED ) {
            return ComponentExecutionStatus.STATUS_FINISHED;
          } else {
            return data.getStatus();
          }
        }
      }
    }
  }

  /**
   * @return the partitionId
   */
  @Override
  public String getPartitionId() {
    return partitionId;
  }

  /**
   * @param partitionId the partitionId to set
   */
  @Override
  public void setPartitionId( String partitionId ) {
    this.partitionId = partitionId;
  }

  /**
   * @return the partitionTargets
   */
  public Map<String, BlockingRowSet> getPartitionTargets() {
    return partitionTargets;
  }

  /**
   * @param partitionTargets the partitionTargets to set
   */
  public void setPartitionTargets( Map<String, BlockingRowSet> partitionTargets ) {
    this.partitionTargets = partitionTargets;
  }

  /**
   * @return the repartitioning type
   */
  public int getRepartitioning() {
    return repartitioning;
  }

  /**
   * @param repartitioning the repartitioning type to set
   */
  @Override
  public void setRepartitioning( int repartitioning ) {
    this.repartitioning = repartitioning;
  }

  /**
   * @return the partitioned
   */
  @Override
  public boolean isPartitioned() {
    return partitioned;
  }

  /**
   * @param partitioned the partitioned to set
   */
  @Override
  public void setPartitioned( boolean partitioned ) {
    this.partitioned = partitioned;
  }

  /**
   * Check feedback.
   *
   * @param lines the lines
   * @return true, if successful
   */
  protected boolean checkFeedback( long lines ) {
    return getPipeline().isFeedbackShown()
      && ( lines > 0 ) && ( getPipeline().getFeedbackSize() > 0 )
      && ( lines % getPipeline().getFeedbackSize() ) == 0;
  }

  /**
   * @return the rowMeta
   */
  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }

  /**
   * @param rowMeta the rowMeta to set
   */
  public void setInputRowMeta( IRowMeta rowMeta ) {
    this.inputRowMeta = rowMeta;
  }

  /**
   * @return the errorRowMeta
   */
  public IRowMeta getErrorRowMeta() {
    return errorRowMeta;
  }

  /**
   * @param errorRowMeta the errorRowMeta to set
   */
  public void setErrorRowMeta( IRowMeta errorRowMeta ) {
    this.errorRowMeta = errorRowMeta;
  }

  /**
   * @return the previewRowMeta
   */
  public IRowMeta getPreviewRowMeta() {
    return previewRowMeta;
  }

  /**
   * @param previewRowMeta the previewRowMeta to set
   */
  public void setPreviewRowMeta( IRowMeta previewRowMeta ) {
    this.previewRowMeta = previewRowMeta;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#copyVariablesFrom(org.apache.hop.core.variables.IVariables)
   */
  @Override
  public void copyFrom( IVariables variables ) {
    this.variables.copyFrom( variables );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#environmentSubstitute(java.lang.String)
   */
  @Override
  public String resolve( String aString ) {
    return variables.resolve( aString );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#environmentSubstitute(java.lang.String[])
   */
  @Override
  public String[] resolve( String[] aString ) {
    return variables.resolve( aString );
  }

  @Override
  public String resolve( String aString, IRowMeta rowMeta, Object[] rowData )
    throws HopValueException {
    return variables.resolve( aString, rowMeta, rowData );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getParentVariableSpace()
   */
  @Override
  public IVariables getParentVariables() {
    return variables.getParentVariables();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hop.core.variables.IVariables#setParentVariableSpace(org.apache.hop.core.variables.IVariables)
   */
  @Override
  public void setParentVariables( IVariables parent ) {
    variables.setParentVariables( parent );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getVariable(java.lang.String, java.lang.String)
   */
  @Override
  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getVariable(java.lang.String)
   */
  @Override
  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#getBooleanValueOfVariable(java.lang.String, boolean)
   */
  @Override
  public boolean getVariableBoolean( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = resolve( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaString.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hop.core.variables.IVariables#initializeFrom(org.apache.hop.core.variables.IVariables)
   */
  @Override
  public void initializeFrom( IVariables parent ) {
    variables.initializeFrom( parent );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#listVariables()
   */
  @Override
  public String[] getVariableNames() {
    return variables.getVariableNames();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#setVariable(java.lang.String, java.lang.String)
   */
  @Override
  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#shareVariablesWith(org.apache.hop.core.variables.IVariables)
   */
  @Override
  public void shareWith( IVariables variables ) {
    this.variables = variables;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.variables.IVariables#injectVariables(java.util.Map)
   */
  @Override
  public void setVariables( Map<String, String> map ) {
    variables.setVariables( map );
  }

  /**
   * This method is executed by Pipeline right before the threads start and right after initialization.
   * <p>
   * More to the point: here we open remote output transform sockets.
   *
   * @throws HopTransformException In case there is an error
   */
  @Override
  public void initBeforeStart() throws HopTransformException {
  }


  @Override public boolean processRow() throws HopException {
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#canProcessOneRow()
   */
  @Override
  public boolean canProcessOneRow() {
    inputRowSetsLock.readLock().lock();
    try {
      switch ( inputRowSets.size() ) {
        case 0:
          return false;
        case 1:
          IRowSet set = inputRowSets.get( 0 );
          if ( set.isDone() ) {
            return false;
          }
          return set.size() > 0;
        default:
          boolean allDone = true;
          for ( IRowSet rowSet : inputRowSets ) {
            if ( !rowSet.isDone() ) {
              allDone = false;
            }
            if ( rowSet.size() > 0 ) {
              return true;
            }
          }
          return !allDone;
      }
    } finally {
      inputRowSetsLock.readLock().unlock();
    }
  }

  public void addTransformFinishedListener( ITransformFinishedListener transformFinishedListener ) {
    transformFinishedListeners.add( transformFinishedListener );
  }

  public void addTransformStartedListener( ITransformStartedListener transformStartedListener ) {
    transformStartedListeners.add( transformStartedListener );
  }

  /**
   * Gets transformFinishedListeners
   *
   * @return value of transformFinishedListeners
   */
  public List<ITransformFinishedListener> getTransformFinishedListeners() {
    return transformFinishedListeners;
  }

  /**
   * @param transformFinishedListeners The transformFinishedListeners to set
   */
  public void setTransformFinishedListeners( List<ITransformFinishedListener> transformFinishedListeners ) {
    this.transformFinishedListeners = transformFinishedListeners;
  }

  /**
   * Gets transformStartedListeners
   *
   * @return value of transformStartedListeners
   */
  public List<ITransformStartedListener> getTransformStartedListeners() {
    return transformStartedListeners;
  }

  /**
   * @param transformStartedListeners The transformStartedListeners to set
   */
  public void setTransformStartedListeners( List<ITransformStartedListener> transformStartedListeners ) {
    this.transformStartedListeners = transformStartedListeners;
  }

  @Override
  public boolean isMapping() {
    return transformMeta.isMapping();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectName()
   */
  @Override
  public String getObjectName() {
    return getTransformName();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#getLogChannel()
   */
  @Override
  public ILogChannel getLogChannel() {
    return log;
  }

  @Override public String getName() {
    return transformName;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  public int getCopyNr() {
    return copyNr;
  }

  /**
   * @param copyNr The copyNr to set
   */
  public void setCopyNr( int copyNr ) {
    this.copyNr = copyNr;
  }

  @Override public String getLogText() {
    StringBuffer buffer = HopLogStore.getAppender().getBuffer( log.getLogChannelId(), false );
    if ( buffer == null ) {
      return null;
    }
    return buffer.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getFilename()
   */
  @Override
  public String getFilename() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getLogChannelId()
   */
  @Override
  public String getLogChannelId() {
    return log.getLogChannelId();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectType()
   */
  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.TRANSFORM;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getParent()
   */
  @Override
  public ILoggingObject getParent() {
    return pipeline;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectCopy()
   */
  @Override
  public String getObjectCopy() {
    return Integer.toString( copyNr );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getLogLevel()
   */
  @Override
  public LogLevel getLogLevel() {
    return log != null ? log.getLogLevel() : null;
  }

  /**
   * Sets the log level.
   *
   * @param logLevel the new log level
   */
  public void setLogLevel( LogLevel logLevel ) {
    log.setLogLevel( logLevel );
  }

  /**
   * Close quietly.
   *
   * @param cl the object that can be closed.
   */
  public static void closeQuietly( Closeable cl ) {
    if ( cl != null ) {
      try {
        cl.close();
      } catch ( IOException ignored ) {
        // Ignore IOException on close
      }
    }
  }

  /**
   * Returns the container object ID.
   *
   * @return the containerObjectId
   */
  @Override
  public String getContainerId() {
    return containerObjectId;
  }

  /**
   * Sets the container object ID.
   *
   * @param containerObjectId the containerObjectId to set
   */
  public void setCarteObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransform#batchComplete()
   */
  @Override
  public void batchComplete() throws HopException {
  }

  /**
   * Returns the registration date
   *
   * @rerturn the registration date
   */
  @Override
  public Date getRegistrationDate() {
    return null;
  }

  @Override
  public boolean isGatheringMetrics() {
    return log != null && log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    if ( log != null ) {
      log.setGatheringMetrics( gatheringMetrics );
    }
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return log != null && log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    if ( log != null ) {
      log.setForcingSeparateLogging( forcingSeparateLogging );
    }
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  @Override
  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

  @Override
  public int getCurrentOutputRowSetNr() {
    return currentOutputRowSetNr;
  }

  @Override
  public void setCurrentOutputRowSetNr( int index ) {
    currentOutputRowSetNr = index;
  }

  @Override
  public int getCurrentInputRowSetNr() {
    return currentInputRowSetNr;
  }

  @Override
  public void setCurrentInputRowSetNr( int index ) {
    currentInputRowSetNr = index;
  }

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  private class DefaultRowHandler implements IRowHandler {
    @Override public Object[] getRow() throws HopException {
      return handleGetRow();
    }

    @Override public void putRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      handlePutRow( rowMeta, row );
    }

    @Override public void putError( IRowMeta rowMeta, Object[] row, long nrErrors, String errorDescriptions, String fieldNames, String errorCodes ) throws HopTransformException {
      handlePutError( variables, rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes );
    }

    @Override public Object[] getRowFrom( IRowSet rowSet ) throws HopTransformException {
      return handleGetRowFrom( rowSet );
    }

    @Override public void putRowTo( IRowMeta rowMeta, Object[] row, IRowSet rowSet ) throws HopTransformException {
      handlePutRowTo( rowMeta, row, rowSet );
    }

  }

  /**
   * Gets initStartDate
   *
   * @return value of initStartDate
   */
  public Date getInitStartDate() {
    return initStartDate;
  }

  /**
   * @param initStartDate The initStartDate to set
   */
  public void setInitStartDate( Date initStartDate ) {
    this.initStartDate = initStartDate;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * @param executionStartDate The executionStartDate to set
   */
  public void setExecutionStartDate( Date executionStartDate ) {
    this.executionStartDate = executionStartDate;
  }

  /**
   * Gets firstRowReadDate
   *
   * @return value of firstRowReadDate
   */
  public Date getFirstRowReadDate() {
    return firstRowReadDate;
  }

  /**
   * @param firstRowReadDate The firstRowReadDate to set
   */
  public void setFirstRowReadDate( Date firstRowReadDate ) {
    this.firstRowReadDate = firstRowReadDate;
  }

  /**
   * Gets lastRowWrittenDate
   *
   * @return value of lastRowWrittenDate
   */
  public Date getLastRowWrittenDate() {
    return lastRowWrittenDate;
  }

  /**
   * @param lastRowWrittenDate The lastRowWrittenDate to set
   */
  public void setLastRowWrittenDate( Date lastRowWrittenDate ) {
    this.lastRowWrittenDate = lastRowWrittenDate;
  }

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  public void setExecutionEndDate( Date executionEndDate ) {
    this.executionEndDate = executionEndDate;
  }
}
