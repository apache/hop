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

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.resource.IResourceExport;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.w3c.dom.Document;

@Action(
    id = "Repeat",
    name = "i18n::Repeat.Name",
    description = "i18n::Repeat.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    keywords = "i18n::Repeat.keywords",
    image = "repeat.svg",
    documentationUrl = "/workflow/actions/repeat.html",
    actionTransformTypes = {
      ActionTransformType.HOP_FILE,
      ActionTransformType.HOP_PIPELINE,
      ActionTransformType.HOP_WORKFLOW
    })
@Getter
@Setter
public class Repeat extends ActionBase implements IAction, Cloneable {

  public static final String REPEAT_END_LOOP = "_REPEAT_END_LOOP_";

  @HopMetadataProperty(key = "filename", hopMetadataPropertyType = HopMetadataPropertyType.HOP_FILE)
  private String filename;

  @HopMetadataProperty(key = "parameter", groupKey = "parameters")
  private List<ParameterDetails> parameters;

  @HopMetadataProperty(key = "variable_name")
  private String variableName;

  @HopMetadataProperty(key = "variable_value")
  private String variableValue;

  @HopMetadataProperty(key = "delay")
  private String delay;

  @HopMetadataProperty(key = "keep_values")
  private boolean keepingValues;

  @HopMetadataProperty(key = "run_configuration")
  private String runConfigurationName;

  // Here is a list of options to log to a file
  //
  @HopMetadataProperty(key = "logfile_enabled")
  private boolean logFileEnabled;

  @HopMetadataProperty(key = "logfile_base")
  private String logFileBase;

  @HopMetadataProperty(key = "logfile_extension")
  private String logFileExtension = "log";

  @HopMetadataProperty(key = "logfile_appended")
  private boolean logFileAppended = true;

  @HopMetadataProperty(key = "logfile_add_date")
  private boolean logFileDateAdded = true;

  @HopMetadataProperty(key = "logfile_add_time")
  private boolean logFileTimeAdded = false;

  @HopMetadataProperty(key = "logfile_add_repetition")
  private boolean logFileRepetitionAdded = false;

  @HopMetadataProperty(key = "logfile_update_interval")
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

    // Also check the variable in the workflow

    ExecutionResult executionResult = null;

    boolean repeat = true;
    int repetitionNr = 0;
    while (repeat && !parentWorkflow.isStopped()) {
      repetitionNr++;
      executionResult = executePipelineOrWorkflow(realFilename, nr, executionResult, repetitionNr);
      Result result = executionResult.result;
      if (!result.getResult() || result.getNrErrors() > 0 || result.isStopped()) {
        logError("The repeating work encountered and error or was stopped. This ends the loop.");

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
          // Repeat as long as the variable is not set in the child workflow or pipeline
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

  private ExecutionResult executePipelineOrWorkflow(
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
        PipelineEngineFactory.createPipelineEngine(
            this, resolve(runConfigurationName), getMetadataProvider(), pipelineMeta);
    pipeline.setParentWorkflow(getParentWorkflow());
    pipeline.setParent(this);
    if (keepingValues && previousResult != null) {
      pipeline.copyFrom(previousResult.variables);
    } else {
      pipeline.initializeFrom(getParentWorkflow());

      // Also copy the parameters over...
      //
      pipeline.copyParametersFromDefinitions(pipelineMeta);
    }
    pipeline.getPipelineMeta().setInternalHopVariables(pipeline);

    // A pipeline can only change variables in the parent workflow
    // So let's take the workflow variables to evaluate.
    pipeline.setVariables(getVariablesMap(getParentWorkflow(), previousResult));

    // TODO: check this!
    INamedParameters previousParams =
        previousResult == null ? null : (INamedParameters) previousResult.variables;
    IVariables previousVars = previousResult == null ? null : previousResult.variables;
    updateParameters(pipeline, previousVars, getParentWorkflow(), previousParams);

    pipeline.setLogLevel(getLogLevel());
    pipeline.setMetadataProvider(getMetadataProvider());

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
    IWorkflowEngine<WorkflowMeta> workflow =
        WorkflowEngineFactory.createWorkflowEngine(
            this, resolve(runConfigurationName), getMetadataProvider(), workflowMeta, this);
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
    INamedParameters previousParams =
        previousResult == null ? null : (INamedParameters) previousResult.variables;
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
        logBasic("End loop flag found, stopping loop.");
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
    for (INamedParameters param : params) {
      if (param != null) {}
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
  public String[] getReferencedObjectDescriptions() {
    String referenceDescription;
    if (StringUtils.isEmpty(filename)) {
      referenceDescription = "";
    } else if (filename.toLowerCase().endsWith(".hpl")) {
      referenceDescription = "The repeating pipeline";
    } else if (filename.toLowerCase().endsWith(".hwf")) {
      referenceDescription = "The repeating workflow";
    } else {
      referenceDescription = "The repeating workflow or pipeline";
    }

    return new String[] {referenceDescription};
  }

  @Override
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

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {

    copyFrom(variables);
    String realFileName = resolve(filename);
    AbstractMeta pipelineOrWorkflow;

    if (isPipeline(realFileName)) {
      pipelineOrWorkflow = loadPipeline(realFileName, metadataProvider, this);
    } else if (isWorkflow(realFileName)) {
      pipelineOrWorkflow = loadWorkflow(realFileName, metadataProvider, this);
    } else {
      throw new HopException(
          "Can't tell if this workflow action is referencing a transformation or a workflow");
    }

    String proposedNewFilename =
        ((IResourceExport) pipelineOrWorkflow)
            .exportResources(variables, definitions, namingInterface, metadataProvider);
    String newFilename =
        "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + proposedNewFilename;
    pipelineOrWorkflow.setFilename(newFilename);
    filename = newFilename;
    return proposedNewFilename;
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
    return new PipelineMeta(realFilename, metadataProvider, variables);
  }

  private WorkflowMeta loadWorkflow(
      String realFilename, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    return new WorkflowMeta(variables, realFilename, metadataProvider);
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
}
