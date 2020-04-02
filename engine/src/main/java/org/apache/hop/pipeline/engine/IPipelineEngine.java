package org.apache.hop.pipeline.engine;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;

import java.util.List;

/**
 * Describes the capabilities of an execution engine for a certain type of object called the subject
 *
 * @param <T> The subject class to execute
 */
public interface IPipelineEngine<T> {

  T getSubject();

  void setSubject(T t);

  String getPluginId();
  void setPluginId(String pluginId);

  /**
   * Ask the engine to generate a default pipeline engine configuration for this engine
   * @return a new pipeline engine run configuration
   */
  IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration();


  void setPipelineEngineRunConfiguration( IPipelineEngineRunConfiguration pipelineRunConfiguration);

  IPipelineEngineRunConfiguration getPipelineEngineRunConfiguration();

  /**
   * Executes the object/subject: calls prepareExecution and startThreads in sequence.
   *
   * @throws HopException if the object execution could not be prepared (initialized)
   */
  void execute() throws HopException;

  /**
   * Prepares for execution. This includes setting the parameters as well as preparing
   * anything needed in terms of resources, threads,....
   * It runs initialization on all the work that is needed to be done up-front.
   *
   * @throws HopException In case of an initialization error
   */
  void prepareExecution() throws HopException;

  /**
   * @return true if the engine is preparing execution
   */
  boolean isPreparing();

  /**
   * Check to see if the pipeline engine is ready to start threads.
   * @return True if the pipeline engine was prepared and is ready to start.
   */
  boolean isReadyToStart();

  /**
   * Starts the engine itself after prepareExecution.
   *
   * @throws HopException If there is an engine error during execution.
   */
  void startThreads() throws HopException;

  /**
   * Collect metrics from the various components in the engine.
   *
   * @return The engine metrics
   */
  EngineMetrics getEngineMetrics();

  /**
   * Get the engine metrics for a specific component name and/or copy nr
   * @param componentName the name of the component or null for all components
   * @param copyNr The copy nr to select or a negative number for all components
   * @return The engine metrics for the given
   */
  EngineMetrics getEngineMetrics(String componentName, int copyNr);

  /**
   * This method performs any cleanup operations on the various sub-components of the engine after execution.
   */
  void cleanup();

  /**
   * Wait until the execution is done.
   */
  void waitUntilFinished();

  /**
   * Stops all parts of the execution from running and alerts any registered listeners.
   */
  void stopAll();

  /**
   * See if there are any halted components in the engine: actions, auditing, ...
   * @return True if there are halted components
   */
  public boolean hasHaltedComponents();

  /**
   * Indicates whether or not the engine is running
   * @return True is the engine is running
   */
  boolean isRunning();

  /**
   * Pauses the execution (all components).
   */
  void pauseRunning();

  /**
   * Resume the execution (all components).
   */
  void resumeRunning();

  /**
   * @return a number larger than 0 in case of errors
   */
  int getErrors();

  /**
   * See if the engine has finished executing.
   *
   * @return True if the execution has finished
   */
  boolean isFinished();

  /**
   * Call the given listener lambda when this pipeline engine has completed execution.
   *
   * @param listener
   * @throws HopException
   */
  void addFinishedListener( IFinishedListener listener ) throws HopException;

  /**
   * Close unique database connections. If there are errors in the Result, perform a rollback
   *
   * @param result the result of the execution
   */
  void closeUniqueDatabaseConnections( Result result );

  /**
   * Retrieve the logging text of a particular component in the engine
   *
   * @param componentName The name of the component (transform)
   * @param copyNr        The copy number if multiple components are running in parallel (0 based).
   * @return The logging text
   */
  String getComponentLogText( String componentName, int copyNr );

  /**
   * Get a list of components
   *
   * @return The list of components in the engine
   */
  List<IEngineComponent> getComponents();

  /**
   * Find all component copies by name
   *
   * @param name   The name of the component to look for (transform)
   * @return The list of components found
   */
  List<IEngineComponent> getComponentCopies( String name );

  /**
   * Find a component by name and copy nr
   *
   * @param name   The name of the component to look for (transform)
   * @param copyNr The copy number to match
   * @return The component or null if nothing was found
   */
  IEngineComponent findComponent( String name, int copyNr );

  /**
   * Gets the log channel interface for the pipeline.
   *
   * @return the log channel
   */
  LogChannelInterface getLogChannel();

  /**
   * The log channel ID if there is any
   * @return
   */
  String getLogChannelId();

  /**
   * Sets the parent logging object.
   *
   * @param parent the new parent
   */
  void setParent( LoggingObjectInterface parent );

  /**
   * Get the result of the execution after it's done, a resume
   *
   * @return The Result of the execution of the object
   */
  Result getResult();


  void setMetaStore( IMetaStore metaStore );

  IMetaStore getMetaStore();

  void setLogLevel( LogLevel logLevel );

  LogLevel getLogLevel();


  /**
   * Temporary until we find a better way to preview/debug
   * @param preview
   */
  @Deprecated
  void setPreview( boolean preview );

  /**
   * Temporary until we find a better way to preview/debug
   * @return
   */
  @Deprecated
  boolean isPreview();

  /**
   * Retrieve output rows from a component copy.  Pass the rows to the rows received lambda.
   *
   * @param componentName
   * @param copyNr
   * @param nrRows
   * @param rowsReceived
   * @throws HopException
   */
  void retrieveComponentOutput( String componentName, int copyNr, int nrRows, IPipelineComponentRowsReceived rowsReceived ) throws HopException;

}
