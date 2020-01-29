package org.apache.hop.trans.engine;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LoggingObjectInterface;

import java.util.List;

/**
 * Describes the capabilities of an execution engine for a certain type of object called the subject
 * @param <T> The subject class to execute
 */
public interface IEngine<T> {

  T getSubject();

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
   * Starts the engine itself after prepareExecution.
   *
   * @throws HopException If there is an engine error during execution.
   */
  void startThreads() throws HopException;

  /**
   * Collect metrics from the various components in the engine.
   * @return The engine metrics
   */
  public EngineMetrics getEngineMetrics();

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
   * @return True if the execution has finished
   */
  boolean isFinished();

  /**
   * Close unique database connections. If there are errors in the Result, perform a rollback
   *
   * @param result the result of the execution
   */
  void closeUniqueDatabaseConnections( Result result );

  /**
   * Retrieve the logging text of a particular component in the engine
   * @param componentName The name of the component (step)
   * @param copyNr The copy number if multiple components are running in parallel (0 based).
   * @return The logging text
   */
  String getComponentLogText(String componentName, int copyNr);

  /**
   * Get a list of components
   * @return The list of components in the engine
   */
  List<IEngineComponent> getComponents();

  /**
   * Find a component by name and copy nr
   * @param name The name of the component to look for (step)
   * @param copyNr The copy number to match
   * @return The component or null if nothing was found
   */
  IEngineComponent findComponent(String name, int copyNr);

  /**
   * Gets the log channel interface for the transformation.
   *
   * @return the log channel
   * @see org.apache.hop.core.logging.HasLogChannelInterface#getLogChannel()
   */
  LogChannelInterface getLogChannel();

  /**
   * Sets the parent logging object.
   *
   * @param parent the new parent
   */
  void setParent( LoggingObjectInterface parent );

  /**
   * Get the result of the execution after it's done, a resume
   * @return The Result of the execution of the object
   */
  Result getResult();


}
