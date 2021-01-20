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

package org.apache.hop.workflow.actions.repeat;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Action(
    id = "Repeat",
    name = "Repeat",
    description = "Repeat execution of a workflow or a transformation",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    image = "repeat.svg",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/repeat.html")
public class Repeat extends ActionBase implements IAction, Cloneable {

  public static final String REPEAT_END_LOOP = "_REPEAT_END_LOOP_";

  public static final String FILENAME = "filename";
  public static final String RUN_CONFIGURATION = "run_configuration";
  public static final String VARIABLE_NAME = "variable_name";
  public static final String VARIABLE_VALUE = "variable_value";
  public static final String DELAY = "delay";
  public static final String KEEP_VALUES = "keep_values";

  public static final String LOGFILE_ENABLED = "logfile_enabled";
  public static final String LOGFILE_APPENDED = "logfile_appended";
  public static final String LOGFILE_BASE = "logfile_base";
  public static final String LOGFILE_EXTENSION = "logfile_extension";
  public static final String LOGFILE_ADD_DATE = "logfile_add_date";
  public static final String LOGFILE_ADD_TIME = "logfile_add_time";
  public static final String LOGFILE_ADD_REPETITION = "logfile_add_repetition";
  public static final String LOGFILE_UPDATE_INTERVAL = "logfile_update_interval";

  public static final String PARAMETERS = "parameters";
  public static final String PARAMETER = "parameter";

  private String filename;
  private List<ParameterDetails> parameters;
  private String variableName;
  private String variableValue;
  private String delay;
  private boolean keepingValues;

  private String runConfigurationName;

  // Here is a list of options to log to a file
  //
  private boolean logFileEnabled;
  private String logFileBase;
  private String logFileExtension = "log";
  private boolean logFileAppended = true;
  private boolean logFileDateAdded = true;
  private boolean logFileTimeAdded = false;
  private boolean logFileRepetitionAdded = false;
  private String logFileUpdateInterval = "5000";

  private class ExecutionResult {
    public Result result;
    public IVariables variables;
    public boolean flagSet;

    public ExecutionResult(Result result, IVariables variables, boolean flagSet) {
      this.result = result;
      this.variables = variables;
      this.flagSet = flagSet;
    }
  }

  public Repeat(String name, String description) {
    super(name, description);
    parameters = new ArrayList<>();
  }

  public Repeat() {
    this("", "");
  }

  @Override
  public Repeat clone() {
    return (Repeat) super.clone();
  }

  @Override
  public Result execute(Result prevResult, int nr) throws HopException {

    // So now we execute the transformation or workflow and continue until the variable has a
    // certain value.
    //
    String realFilename = resolve(filename);
    if (StringUtils.isEmpty(realFilename)) {
      throw new HopException("Please specify a transformation or workflow to repeat");
    }

    long delayInMs = -1;

    if (StringUtils.isNotEmpty(delay)) {
      String realDelay = resolve(delay);
      delayInMs = Const.toLong(realDelay, -1);
      if (delayInMs < 0) {
        throw new HopException("Unable to parse delay for string: " + realDelay);
      }
      delayInMs *= 1000; // convert to ms
    }

    // Clear flag
    //
    getExtensionDataMap().remove(REPEAT_END_LOOP);

    // If the variable is set at the beginning of the loop, don't execute at all!
    //
    if (isVariableValueSet(this)) {
      return prevResult;
    }

    ExecutionResult executionResult = null;

    boolean repeat = true;
    int repetitionNr = 0;
    while (repeat && !parentWorkflow.isStopped()) {
      repetitionNr++;
      executionResult = executeTransformationOrWorkflow(
        realFilename,
        nr,
        executionResult,
        repetitionNr
      );
      Result result = executionResult.result;
      if (!result.getResult() || result.getNrErrors() > 0 || result.isStopped()) {
        log.logError(
            "The repeating work encountered and error or was stopped. This ends the loop.");

        // On an false result, stop the loop
        //
        prevResult.setResult(false);

        repeat = false;
      } else {
        // End repeat if the End Repeat workflow action is executed
        //
        if (executionResult.flagSet) {
          repeat = false;
        } else {
          // Repeat as long as the variable is not set.
          //
          repeat = !isVariableValueSet(executionResult.variables);
        }
      }

      if (repeat && delayInMs > 0) {
        // See if we need to delay
        //
        long startTime = System.currentTimeMillis();
        while (!parentWorkflow.isStopped()
            && (System.currentTimeMillis() - startTime < delayInMs)) {
          try {
            Thread.sleep(100);
          } catch (Exception e) {
            // Ignore
          }
        }
      }
    }

    // Add last execution results
    //
    if (executionResult != null) {
      prevResult.add(executionResult.result);
    }

    return prevResult;
  }

  private ExecutionResult executeTransformationOrWorkflow(
      String realFilename, int nr, ExecutionResult previousResult, int repetitionNr)
      throws HopException {
    if (isPipeline(realFilename)) {
      return executePipeline(realFilename, nr, previousResult, repetitionNr);
    }
    if (isWorkflow(realFilename)) {
      return executeWorkflow(realFilename, nr, previousResult, repetitionNr);
    }
    throw new HopException("Sorry, I don't know if this is a pipeline or a workflow");
  }

  private ExecutionResult executePipeline(
      String realFilename, int nr, ExecutionResult previousResult, int repetitionNr)
      throws HopException {
    PipelineMeta pipelineMeta = loadPipeline(realFilename, getMetadataProvider(), this);
    IPipelineEngine<PipelineMeta> pipeline =
        PipelineEngineFactory.createPipelineEngine( this,
            runConfigurationName, getMetadataProvider(), pipelineMeta);
    pipeline.setParentWorkflow(getParentWorkflow());
    pipeline.setParent( this );
    if (keepingValues && previousResult != null) {
      pipeline.copyFrom(previousResult.variables);
    } else {
      pipeline.initializeFrom(getParentWorkflow());

      // Also copy the parameters over...
      //
      pipeline.copyParametersFromDefinitions( pipelineMeta);
    }
    pipeline.getPipelineMeta().setInternalHopVariables(pipeline);
    pipeline.setVariables(getVariablesMap(pipeline, previousResult));

    // TODO: check this!
    INamedParameters previousParams =
        previousResult == null ? null : (INamedParameters) previousResult.variables;
    IVariables previousVars = previousResult == null ? null : previousResult.variables;
    updateParameters(pipeline, previousVars, getParentWorkflow(), previousParams);

    pipeline.setLogLevel(getLogLevel());
    pipeline.setMetadataProvider(getMetadataProvider());

    // Inform the parent workflow we started something here...
    /*
    for ( IDelegationListener delegationListener : parentWorkflow.getDelegationListeners() ) {
      delegationListener.transformationDelegationStarted( pipeline, new TransExecutionConfiguration() );
    }
     */

    // Start logging before execution...
    //
    LogChannelFileWriter fileWriter = null;
    try {
      if (logFileEnabled) {
        fileWriter = logToFile(pipeline, repetitionNr);
      }

      // Run it!
      //
      pipeline.prepareExecution();
      pipeline.startThreads();
      pipeline.waitUntilFinished();

      boolean flagSet = pipeline.getExtensionDataMap().get(REPEAT_END_LOOP) != null;
      Result result = pipeline.getResult();
      return new ExecutionResult(result, pipeline, flagSet);
    } finally {
      if (logFileEnabled && fileWriter != null) {
        fileWriter.stopLogging();
      }
    }
  }

  private LogChannelFileWriter logToFile(ILoggingObject loggingObject, int repetitionNr)
      throws HopException {

    // Calculate the filename
    //
    Date currentDate = new Date();

    String filename = resolve(logFileBase);
    if (logFileDateAdded) {
      filename += "_" + new SimpleDateFormat("yyyyMMdd").format(currentDate);
    }
    if (logFileTimeAdded) {
      filename += "_" + new SimpleDateFormat("HHmmss").format(currentDate);
    }
    if (logFileRepetitionAdded) {
      filename += "_" + new DecimalFormat("0000").format(repetitionNr);
    }
    filename += "." + resolve(logFileExtension);

    String logChannelId = loggingObject.getLogChannelId();
    LogChannelFileWriter fileWriter =
        new LogChannelFileWriter(
            logChannelId,
            HopVfs.getFileObject(filename),
            logFileAppended,
            Const.toInt(logFileUpdateInterval, 5000));

    fileWriter.startLogging();

    return fileWriter;
  }

  private Map<String, String> getVariablesMap(
    INamedParameters namedParams, ExecutionResult previousResult) {
    String[] params = namedParams.listParameters();
    Map<String, String> variablesMap = new HashMap<>();

    if (keepingValues && previousResult != null) {
      for (String variableName : previousResult.variables.getVariableNames()) {
        variablesMap.put(variableName, previousResult.variables.getVariable(variableName));
      }
    } else {
      // Initialize the values of the defined parameters in the workflow action
      //
      for (ParameterDetails parameter : parameters) {
        String value = resolve(parameter.getField());
        variablesMap.put(parameter.getName(), value);
      }
    }
    return variablesMap;
  }

  private ExecutionResult executeWorkflow(
      String realFilename, int nr, ExecutionResult previousResult, int repetitionNr)
      throws HopException {

    WorkflowMeta workflowMeta = loadWorkflow(realFilename, getMetadataProvider(), this);
    IWorkflowEngine<WorkflowMeta> workflow = WorkflowEngineFactory.createWorkflowEngine(
      this,
      runConfigurationName,
      getMetadataProvider(),
      workflowMeta,
      this
    );
    workflow.setParentWorkflow(getParentWorkflow());
    workflow.setParentVariables(this);
    if (keepingValues && previousResult != null) {
      workflow.copyFrom(previousResult.variables);
    } else {
      workflow.initializeFrom(this);

      // Also copy the parameters over...
      //
      workflow.copyParametersFromDefinitions(workflowMeta);
    }

    workflow.getWorkflowMeta().setInternalHopVariables(workflow);
    workflow.setVariables(getVariablesMap(workflow, previousResult));

    // TODO: check this!
    INamedParameters previousParams = previousResult == null ? null : (INamedParameters) previousResult.variables;
    IVariables previousVars = previousResult == null ? null : (IVariables) previousResult.variables;
    updateParameters(workflow, previousVars, getParentWorkflow(), previousParams);

    workflow.setLogLevel(getLogLevel());

    if (parentWorkflow.isInteractive()) {
      workflow.setInteractive(true);
      workflow.getActionListeners().addAll(parentWorkflow.getActionListeners());
    }

    // Link the workflow with the sub-workflow
    parentWorkflow.getWorkflowTracker().addWorkflowTracker(workflow.getWorkflowTracker());
    // Link both ways!
    workflow.getWorkflowTracker().setParentWorkflowTracker(parentWorkflow.getWorkflowTracker());

    // Inform the parent workflow we started something here...
    /*
    for ( DelegationListener delegationListener : parentWorkflow.getDelegationListeners() ) {
      delegationListener.jobDelegationStarted( workflow, new WorkflowExecutionConfiguration() );
    }
     */

    // Start logging before execution...
    //
    LogChannelFileWriter fileWriter = null;
    try {
      if (logFileEnabled) {
        fileWriter = logToFile(workflow, repetitionNr);
      }

      Result result = workflow.startExecution();

      boolean flagSet = workflow.getExtensionDataMap().get(REPEAT_END_LOOP) != null;
      if (flagSet) {
        log.logBasic("End loop flag found, stopping loop.");
      }

      return new ExecutionResult(result, workflow, flagSet);
    } finally {
      if (logFileEnabled && fileWriter != null) {
        fileWriter.stopLogging();
      }
    }
  }

  private void updateParameters(
    INamedParameters subParams, IVariables subVars, INamedParameters... params) {
    // Inherit
    for ( INamedParameters param : params) {
      if (param != null) {
        // TODO : Merge
        // subParams.mergeParametersWith(param, true);
      }
    }

    // Any parameters to initialize from the workflow action?
    //
    String[] parameterNames = subParams.listParameters();
    for (ParameterDetails parameter : parameters) {
      if (Const.indexOfString(parameter.getName(), parameterNames) >= 0) {
        // Set this parameter
        //
        String value = resolve(parameter.getField());
        try {
          subParams.setParameterValue(parameter.getName(), value);
        } catch (UnknownParamException e) {
          // Ignore
        }
      }
    }

    // Changed values?
    //
    if (keepingValues && subVars != null) {
      for (String parameterName : subParams.listParameters()) {
        try {
          String value = subVars.getVariable(parameterName);
          subParams.setParameterValue(parameterName, value);
        } catch (UnknownParamException e) {
          // Ignore
        }
      }
    }

    // subParams.activateParameters(); TODO
  }

  private boolean isVariableValueSet(IVariables variables) {

    // See if there's a flag set.
    //
    if (StringUtils.isNotEmpty(variableName)) {
      String realVariable = variables.resolve(variableName);
      String value = variables.getVariable(realVariable);
      if (StringUtil.isEmpty(value)) {
        return false;
      }
      String realValue = resolve(variableValue);

      // If we didn't specify any specific value, the variable is simply set.
      //
      if (StringUtils.isEmpty(realValue)) {
        return true;
      }

      // The value in the variables and the expected value need to match
      //
      return realValue.equalsIgnoreCase(value);
    }
    return false;
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append(super.getXml());

    xml.append(XmlHandler.addTagValue(FILENAME, filename));
    xml.append(XmlHandler.addTagValue(RUN_CONFIGURATION, runConfigurationName));
    xml.append(XmlHandler.addTagValue(VARIABLE_NAME, variableName));
    xml.append(XmlHandler.addTagValue(VARIABLE_VALUE, variableValue));
    xml.append(XmlHandler.addTagValue(DELAY, delay));
    xml.append(XmlHandler.addTagValue(KEEP_VALUES, keepingValues));

    xml.append(XmlHandler.addTagValue(LOGFILE_ENABLED, logFileEnabled));
    xml.append(XmlHandler.addTagValue(LOGFILE_APPENDED, logFileAppended));
    xml.append(XmlHandler.addTagValue(LOGFILE_BASE, logFileBase));
    xml.append(XmlHandler.addTagValue(LOGFILE_EXTENSION, logFileExtension));
    xml.append(XmlHandler.addTagValue(LOGFILE_ADD_DATE, logFileDateAdded));
    xml.append(XmlHandler.addTagValue(LOGFILE_ADD_TIME, logFileTimeAdded));
    xml.append(XmlHandler.addTagValue(LOGFILE_ADD_REPETITION, logFileRepetitionAdded));
    xml.append(XmlHandler.addTagValue(LOGFILE_UPDATE_INTERVAL, logFileUpdateInterval));

    xml.append(XmlHandler.openTag(PARAMETERS));
    for (ParameterDetails parameter : parameters) {
      xml.append(XmlHandler.openTag(PARAMETER));
      xml.append(XmlHandler.addTagValue("name", parameter.getName()));
      xml.append(XmlHandler.addTagValue("value", parameter.getField()));
      xml.append(XmlHandler.closeTag(PARAMETER));
    }
    xml.append(XmlHandler.closeTag(PARAMETERS));

    return xml.toString();
  }

  @Override
  public void loadXml( Node actionNode, IHopMetadataProvider metadataProvider, IVariables variables )
      throws HopXmlException {
    super.loadXml(actionNode);

    filename = XmlHandler.getTagValue(actionNode, FILENAME);
    runConfigurationName = XmlHandler.getTagValue(actionNode, RUN_CONFIGURATION);
    variableName = XmlHandler.getTagValue(actionNode, VARIABLE_NAME);
    variableValue = XmlHandler.getTagValue(actionNode, VARIABLE_VALUE);
    delay = XmlHandler.getTagValue(actionNode, DELAY);
    keepingValues = "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, KEEP_VALUES));

    logFileEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, LOGFILE_ENABLED));
    logFileAppended = "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, LOGFILE_APPENDED));
    logFileDateAdded = "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, LOGFILE_ADD_DATE));
    logFileTimeAdded = "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, LOGFILE_ADD_TIME));
    logFileRepetitionAdded =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, LOGFILE_ADD_REPETITION));
    logFileBase = XmlHandler.getTagValue(actionNode, LOGFILE_BASE);
    logFileExtension = XmlHandler.getTagValue(actionNode, LOGFILE_EXTENSION);
    logFileUpdateInterval = XmlHandler.getTagValue(actionNode, LOGFILE_UPDATE_INTERVAL);

    Node parametersNode = XmlHandler.getSubNode(actionNode, PARAMETERS);
    List<Node> parameterNodes = XmlHandler.getNodes(parametersNode, PARAMETER);
    parameters = new ArrayList<>();
    for (Node parameterNode : parameterNodes) {
      String name = XmlHandler.getTagValue(parameterNode, "name");
      String field = XmlHandler.getTagValue(parameterNode, "value");
      parameters.add(new ParameterDetails(name, field));
    }
  }

  @Override
  public String[] getReferencedObjectDescriptions() {
    String referenceDescription;
    if (StringUtils.isEmpty(filename)) {
      referenceDescription="";
    } else if (filename.toLowerCase().endsWith(".hpl")) {
      referenceDescription = "The repeating pipeline";
    } else if (filename.toLowerCase().endsWith(".hwf")) {
      referenceDescription = "The repeating workflow";
    } else {
      referenceDescription = "The repeating workflow or pipeline";
    }

    return new String[] {referenceDescription};
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {StringUtils.isNotEmpty(filename)};
  }

  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    String realFilename = variables.resolve(filename);
    if (isPipeline(realFilename)) {
      return loadPipeline(realFilename, metadataProvider, variables);
    } else if (isWorkflow(realFilename)) {
      return loadWorkflow(realFilename, metadataProvider, variables);
    } else {
      // TODO: open the file, see what's in there.
      //
      throw new HopException(
          "Can't tell if this workflow action is referencing a transformation or a workflow");
    }
  }

  public boolean isPipeline(String realFilename) throws HopException {
    if (realFilename.toLowerCase().endsWith(".hpl")) {
      return true;
    }
    // See in the file
    try {
      Document document = XmlHandler.loadXmlFile(realFilename);
      return XmlHandler.getSubNode(document, PipelineMeta.XML_TAG) != null;
    } catch (Exception e) {
      // Not a valid file or Xml document
    }
    return false;
  }

  public boolean isWorkflow(String realFilename) {
    if (realFilename.toLowerCase().endsWith(".hwf")) {
      return true;
    }

    // See in the file
    try {
      Document document = XmlHandler.loadXmlFile(realFilename);
      return XmlHandler.getSubNode(document, WorkflowMeta.XML_TAG) != null;
    } catch (Exception e) {
      // Not a valid file or Xml document
    }
    return false;
  }

  private PipelineMeta loadPipeline(
      String realFilename, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta(realFilename, metadataProvider, true, variables);
    return pipelineMeta;
  }

  private WorkflowMeta loadWorkflow(
      String realFilename, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    WorkflowMeta workflowMeta = new WorkflowMeta(variables, realFilename, metadataProvider);
    return workflowMeta;
  }


  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  @Override
  public String getFilename() {
    return filename;
  }

  /** @param filename The filename to set */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public List<ParameterDetails> getParameters() {
    return parameters;
  }

  /** @param parameters The parameters to set */
  public void setParameters(List<ParameterDetails> parameters) {
    this.parameters = parameters;
  }

  /**
   * Gets variableName
   *
   * @return value of variableName
   */
  public String getVariableName() {
    return variableName;
  }

  /** @param variableName The variableName to set */
  public void setVariableName(String variableName) {
    this.variableName = variableName;
  }

  /**
   * Gets variableValue
   *
   * @return value of variableValue
   */
  public String getVariableValue() {
    return variableValue;
  }

  /** @param variableValue The variableValue to set */
  public void setVariableValue(String variableValue) {
    this.variableValue = variableValue;
  }

  /**
   * Gets delay
   *
   * @return value of delay
   */
  public String getDelay() {
    return delay;
  }

  /** @param delay The delay to set */
  public void setDelay(String delay) {
    this.delay = delay;
  }

  /**
   * Gets keepingValues
   *
   * @return value of keepingValues
   */
  public boolean isKeepingValues() {
    return keepingValues;
  }

  /** @param keepingValues The keepingValues to set */
  public void setKeepingValues(boolean keepingValues) {
    this.keepingValues = keepingValues;
  }

  /**
   * Gets logFileEnabled
   *
   * @return value of logFileEnabled
   */
  public boolean isLogFileEnabled() {
    return logFileEnabled;
  }

  /** @param logFileEnabled The logFileEnabled to set */
  public void setLogFileEnabled(boolean logFileEnabled) {
    this.logFileEnabled = logFileEnabled;
  }

  /**
   * Gets logFileBase
   *
   * @return value of logFileBase
   */
  public String getLogFileBase() {
    return logFileBase;
  }

  /** @param logFileBase The logFileBase to set */
  public void setLogFileBase(String logFileBase) {
    this.logFileBase = logFileBase;
  }

  /**
   * Gets logFileExtension
   *
   * @return value of logFileExtension
   */
  public String getLogFileExtension() {
    return logFileExtension;
  }

  /** @param logFileExtension The logFileExtension to set */
  public void setLogFileExtension(String logFileExtension) {
    this.logFileExtension = logFileExtension;
  }

  /**
   * Gets logFileAppended
   *
   * @return value of logFileAppended
   */
  public boolean isLogFileAppended() {
    return logFileAppended;
  }

  /** @param logFileAppended The logFileAppended to set */
  public void setLogFileAppended(boolean logFileAppended) {
    this.logFileAppended = logFileAppended;
  }

  /**
   * Gets logFileDateAdded
   *
   * @return value of logFileDateAdded
   */
  public boolean isLogFileDateAdded() {
    return logFileDateAdded;
  }

  /** @param logFileDateAdded The logFileDateAdded to set */
  public void setLogFileDateAdded(boolean logFileDateAdded) {
    this.logFileDateAdded = logFileDateAdded;
  }

  /**
   * Gets logFileTimeAdded
   *
   * @return value of logFileTimeAdded
   */
  public boolean isLogFileTimeAdded() {
    return logFileTimeAdded;
  }

  /** @param logFileTimeAdded The logFileTimeAdded to set */
  public void setLogFileTimeAdded(boolean logFileTimeAdded) {
    this.logFileTimeAdded = logFileTimeAdded;
  }

  /**
   * Gets logFileRepetitionAdded
   *
   * @return value of logFileRepetitionAdded
   */
  public boolean isLogFileRepetitionAdded() {
    return logFileRepetitionAdded;
  }

  /** @param logFileRepetitionAdded The logFileRepetitionAdded to set */
  public void setLogFileRepetitionAdded(boolean logFileRepetitionAdded) {
    this.logFileRepetitionAdded = logFileRepetitionAdded;
  }

  /**
   * Gets logFileUpdateInterval
   *
   * @return value of logFileUpdateInterval
   */
  public String getLogFileUpdateInterval() {
    return logFileUpdateInterval;
  }

  /** @param logFileUpdateInterval The logFileUpdateInterval to set */
  public void setLogFileUpdateInterval(String logFileUpdateInterval) {
    this.logFileUpdateInterval = logFileUpdateInterval;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /** @param runConfigurationName The runConfigurationName to set */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }
}
