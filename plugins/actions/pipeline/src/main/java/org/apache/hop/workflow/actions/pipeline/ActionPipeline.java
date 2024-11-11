/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.pipeline;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.FileUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This is the action that defines a pipeline to be run. */
@Action(
    id = "PIPELINE",
    image = "ui/images/pipeline.svg",
    name = "i18n::ActionPipeline.Name",
    description = "i18n::ActionPipeline.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    keywords = "i18n::ActionPipeline.keyword",
    documentationUrl = "/workflow/actions/pipeline.html",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
public class ActionPipeline extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionPipeline.class;

  public static final class ParameterDefinition {
    @HopMetadataProperty(key = "pass_all_parameters")
    private boolean passingAllParameters = true;

    @HopMetadataProperty(key = "parameter")
    private List<Parameter> parameters;

    public ParameterDefinition() {
      this.parameters = new ArrayList<>();
    }

    public boolean isPassingAllParameters() {
      return passingAllParameters;
    }

    public void setPassingAllParameters(boolean passingAllParameters) {
      this.passingAllParameters = passingAllParameters;
    }

    public List<Parameter> getParameters() {
      return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
      this.parameters = parameters;
    }

    public String[] getNames() {
      List<String> list = new ArrayList<>();
      for (Parameter parameter : parameters) {
        list.add(parameter.getName());
      }
      return list.toArray(new String[0]);
    }

    public String[] getValues() {
      List<String> list = new ArrayList<>();
      for (Parameter parameter : parameters) {
        list.add(parameter.getValue());
      }
      return list.toArray(new String[0]);
    }
  }

  public static final class Parameter {
    @HopMetadataProperty public String name;
    @HopMetadataProperty public String value;

    @HopMetadataProperty(key = "stream_name")
    public String field;

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public String getField() {
      return field;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public void setField(String field) {
      this.field = field;
    }
  }

  @HopMetadataProperty(
      key = "filename",
      hopMetadataPropertyType = HopMetadataPropertyType.FILE_REFERENCE)
  private String filename;

  @HopMetadataProperty(key = "params_from_previous")
  private boolean paramsFromPrevious;

  @HopMetadataProperty(key = "exec_per_row")
  private boolean execPerRow;

  @HopMetadataProperty(key = "clear_rows")
  private boolean clearResultRows;

  @HopMetadataProperty(key = "clear_files")
  private boolean clearResultFiles;

  @HopMetadataProperty(key = "create_parent_folder")
  private boolean createParentFolder;

  @HopMetadataProperty(key = "set_logfile")
  private boolean setLogfile;

  @HopMetadataProperty(key = "set_append_logfile")
  private boolean setAppendLogfile;

  @HopMetadataProperty(key = "logfile")
  private String logfile;

  @HopMetadataProperty(key = "logext")
  private String logext;

  @HopMetadataProperty(key = "add_date")
  private boolean addDate;

  @HopMetadataProperty(key = "add_time")
  private boolean addTime;

  @HopMetadataProperty(key = "loglevel", storeWithCode = true)
  private LogLevel logFileLevel;

  @HopMetadataProperty(key = "wait_until_finished")
  private boolean waitingToFinish = true;

  @HopMetadataProperty(key = "parameters")
  private ParameterDefinition parameterDefinition;

  @HopMetadataProperty(key = "run_configuration")
  private String runConfiguration;

  private IPipelineEngine<PipelineMeta> pipeline;

  public ActionPipeline(String name) {
    super(name, "");
    this.parameterDefinition = new ParameterDefinition();
  }

  public ActionPipeline() {
    this("");
    clear();
  }

  public ParameterDefinition getParameterDefinition() {
    return parameterDefinition;
  }

  public void setParameterDefinition(ParameterDefinition parameterDefinition) {
    this.parameterDefinition = parameterDefinition;
  }

  public void setFileName(String n) {
    filename = n;
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public String getRealFilename() {
    return resolve(getFilename());
  }

  public String getLogFilename() {
    String retval = "";
    if (setLogfile) {
      retval += logfile == null ? "" : logfile;
      Calendar cal = Calendar.getInstance();
      if (addDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        retval += "_" + sdf.format(cal.getTime());
      }
      if (addTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");
        retval += "_" + sdf.format(cal.getTime());
      }
      if (logext != null && logext.length() > 0) {
        retval += "." + logext;
      }
    }
    return retval;
  }

  @Override
  public void clear() {
    super.clear();

    filename = null;
    execPerRow = false;
    addDate = false;
    addTime = false;
    logfile = null;
    logext = null;
    setLogfile = false;
    clearResultRows = false;
    clearResultFiles = false;
    setAppendLogfile = false;
    waitingToFinish = true;
    createParentFolder = false;
    logFileLevel = LogLevel.BASIC;
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param result The result of the previous execution
   * @param nr the action number
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setEntryNr(nr);

    LogChannelFileWriter logChannelFileWriter = null;

    LogLevel pipelineLogLevel = parentWorkflow.getLogLevel();

    String realLogFilename = "";
    if (setLogfile) {
      pipelineLogLevel = logFileLevel;

      realLogFilename = resolve(getLogFilename());

      // We need to check here the log filename
      // if we do not have one, we must fail
      if (Utils.isEmpty(realLogFilename)) {
        logError(BaseMessages.getString(PKG, "ActionPipeline.Exception.LogFilenameMissing"));
        result.setNrErrors(1);
        result.setResult(false);
        return result;
      }
      // create parent folder?
      if (!FileUtil.createParentFolder(
          PKG, realLogFilename, createParentFolder, this.getLogChannel())) {
        result.setNrErrors(1);
        result.setResult(false);
        return result;
      }
      try {
        logChannelFileWriter =
            new LogChannelFileWriter(
                this.getLogChannelId(), HopVfs.getFileObject(realLogFilename), setAppendLogfile);
        logChannelFileWriter.startLogging();
      } catch (HopException e) {
        logError(
            BaseMessages.getString(
                PKG, "ActionPipeline.Error.UnableOpenAppender", realLogFilename, e.toString()));

        logError(Const.getStackTracker(e));
        result.setNrErrors(1);
        result.setResult(false);
        return result;
      }
    }

    logDetailed(
        BaseMessages.getString(PKG, "ActionPipeline.Log.OpeningPipeline", resolve(getFilename())));

    // Load the pipeline only once for the complete loop!
    // Throws an exception if it was not possible to load the pipeline, for example if the XML file
    // doesn't exist.
    // Log the stack trace and return an error condition from this
    //
    PipelineMeta pipelineMeta = null;
    try {
      pipelineMeta = getPipelineMeta(getMetadataProvider(), this);
    } catch (HopException e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPipeline.Exception.UnableToRunWorkflow",
              parentWorkflowMeta.getName(),
              getName(),
              StringUtils.trim(e.getMessage())),
          e);
      result.setNrErrors(1);
      result.setResult(false);
      return result;
    }

    int iteration = 0;

    RowMetaAndData resultRow = null;
    boolean first = true;
    List<RowMetaAndData> rows = new ArrayList<>(result.getRows());

    while ((first && !execPerRow)
        || (execPerRow && rows != null && iteration < rows.size() && result.getNrErrors() == 0)
            && !parentWorkflow.isStopped()) {
      // Clear the result rows of the result
      // Otherwise we double the amount of rows every iteration in the simple cases.
      //
      if (execPerRow) {
        result.getRows().clear();
      }
      if (rows != null && execPerRow) {
        resultRow = rows.get(iteration);
      } else {
        resultRow = null;
      }

      INamedParameters namedParam = new NamedParameters();
      for (Parameter parameter : parameterDefinition.getParameters()) {
        if (!Utils.isEmpty(parameter.getName())) {
          // We have a parameter
          //
          namedParam.addParameterDefinition(parameter.getName(), "", "Action runtime");
          if (Utils.isEmpty(Const.trim(parameter.getField()))) {
            // There is no field name specified.
            //
            String value = Const.NVL(resolve(parameter.getValue()), "");
            namedParam.setParameterValue(parameter.getName(), value);
          } else {
            // something filled in, in the field column...
            //
            String value = "";
            if (resultRow != null) {
              value = resultRow.getString(parameter.getField(), "");
            }
            namedParam.setParameterValue(parameter.getName(), value);
          }
        }
      }

      first = false;

      Result previousResult = result;

      try {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionPipeline.StartingPipeline",
                  getFilename(),
                  getName(),
                  getDescription()));
        }

        if (clearResultRows) {
          previousResult.setRows(new ArrayList<>());
        }

        if (clearResultFiles) {
          previousResult.getResultFiles().clear();
        }

        /*
         * Set one or more "result" rows on the pipeline...
         */
        if (execPerRow) {
          // Execute for each input row

          // Just pass a single row
          List<RowMetaAndData> newList = new ArrayList<>();
          newList.add(resultRow);

          // This previous result rows list can be either empty or not.
          // Depending on the checkbox "clear result rows"
          // In this case, it would execute the pipeline with one extra row each time
          // Can't figure out a real use-case for it, but hey, who am I to decide that, right?
          // :-)
          //
          previousResult.getRows().addAll(newList);

          if (paramsFromPrevious) { // Copy the input the parameters

            for (Parameter parameter : parameterDefinition.getParameters()) {
              if (!Utils.isEmpty(parameter.getName())) {
                // We have a parameter
                if (Utils.isEmpty(Const.trim(parameter.getField()))) {
                  namedParam.setParameterValue(
                      parameter.getName(), Const.NVL(resolve(parameter.getValue()), ""));
                } else {
                  String fieldValue = "";

                  if (resultRow != null) {
                    fieldValue = resultRow.getString(parameter.getField(), "");
                  }
                  // Get the value from the input stream
                  namedParam.setParameterValue(parameter.getName(), Const.NVL(fieldValue, ""));
                }
              }
            }
          }
        } else {

          if (paramsFromPrevious) {
            // Copy the input the parameters
            for (Parameter parameter : parameterDefinition.getParameters()) {
              if (!Utils.isEmpty(parameter.getName())) {
                // We have a parameter
                if (Utils.isEmpty(Const.trim(parameter.getField()))) {
                  namedParam.setParameterValue(
                      parameter.getName(), Const.NVL(resolve(parameter.getValue()), ""));
                } else {
                  String fieldValue = "";

                  if (resultRow != null) {
                    fieldValue = resultRow.getString(parameter.getField(), "");
                  }
                  // Get the value from the input stream
                  namedParam.setParameterValue(parameter.getName(), Const.NVL(fieldValue, ""));
                }
              }
            }
          }
        }

        // Handle the parameters...
        //
        String[] parameterNames = pipelineMeta.listParameters();

        prepareFieldNamesParameters(parameterDefinition.getParameters(), namedParam, this);

        if (StringUtils.isEmpty(runConfiguration)) {
          throw new HopException(
              "This action needs a run configuration to use to execute the specified pipeline");
        }

        runConfiguration = resolve(runConfiguration);
        logBasic(BaseMessages.getString(PKG, "ActionPipeline.RunConfig.Message", runConfiguration));

        // Create the pipeline from meta-data
        //
        pipeline =
            PipelineEngineFactory.createPipelineEngine(
                this, runConfiguration, getMetadataProvider(), pipelineMeta);
        pipeline.setParent(this);

        // set the parent workflow on the pipeline, variables are taken from here...
        //
        pipeline.setParentWorkflow(parentWorkflow);
        pipeline.setParentVariables(parentWorkflow);
        pipeline.setLogLevel(pipelineLogLevel);
        pipeline.setPreviousResult(previousResult);

        // inject the metadataProvider
        pipeline.setMetadataProvider(getMetadataProvider());

        // Handle parameters...
        //
        pipeline.initializeFrom(null);
        pipeline.copyParametersFromDefinitions(pipelineMeta);

        // Pass the parameter values and activate...
        //
        TransformWithMappingMeta.activateParams(
            pipeline,
            pipeline,
            this,
            parameterNames,
            parameterDefinition.getNames(),
            parameterDefinition.getValues(),
            parameterDefinition.isPassingAllParameters());

        // First get the root workflow
        //
        IWorkflowEngine<WorkflowMeta> rootWorkflow = parentWorkflow;
        while (rootWorkflow.getParentWorkflow() != null) {
          rootWorkflow = rootWorkflow.getParentWorkflow();
        }

        try {
          // Start execution...
          //
          pipeline.execute();

          // Wait until we're done with this pipeline
          //
          if (isWaitingToFinish()) {
            pipeline.waitUntilFinished();

            if (parentWorkflow.isStopped() || pipeline.getErrors() != 0) {
              pipeline.stopAll();
              result.setNrErrors(1);
            }
            updateResult(result);
          }
          if (setLogfile) {
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_LOG,
                    HopVfs.getFileObject(realLogFilename),
                    parentWorkflow.getWorkflowName(),
                    toString());
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
          }
        } catch (HopException e) {

          logError(BaseMessages.getString(PKG, "ActionPipeline.Error.UnablePrepareExec"), e);
          result.setNrErrors(1);
        }

      } catch (Exception e) {

        logError(
            BaseMessages.getString(PKG, "ActionPipeline.ErrorUnableOpenPipeline", e.getMessage()));
        logError(Const.getStackTracker(e));
        result.setNrErrors(1);
      }
      iteration++;
    }

    if (setLogfile) {
      if (logChannelFileWriter != null) {
        logChannelFileWriter.stopLogging();

        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_LOG,
                logChannelFileWriter.getLogFile(),
                parentWorkflow.getWorkflowName(),
                getName());
        result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

        // See if anything went wrong during file writing...
        //
        if (logChannelFileWriter.getException() != null) {
          logError("Unable to open log file [" + getLogFilename() + "] : ");
          logError(Const.getStackTracker(logChannelFileWriter.getException()));
          result.setNrErrors(1);
          result.setResult(false);
          return result;
        }
      }
    }

    if (result.getNrErrors() == 0) {
      result.setResult(true);
    } else {
      result.setResult(false);
    }

    return result;
  }

  protected void updateResult(Result result) {
    Result newResult = pipeline.getResult();
    result.clear(); // clear only the numbers, NOT the files or rows.
    result.add(newResult);

    if (!Utils.isEmpty(newResult.getRows())) {
      result.setRows(newResult.getRows());
    }
  }

  public PipelineMeta getPipelineMeta(IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    try {
      PipelineMeta pipelineMeta = null;
      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      IVariables tmpSpace = r.resolveCurrentDirectory(variables, parentWorkflow, getFilename());

      String realFilename = tmpSpace.resolve(getFilename());

      pipelineMeta = new PipelineMeta(realFilename, metadataProvider, this);

      if (pipelineMeta != null) {
        // Pass the metadata references
        //
        pipelineMeta.setMetadataProvider(metadataProvider);
      }

      return pipelineMeta;
    } catch (final HopException ke) {
      // if we get a HopException, simply re-throw it
      throw ke;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionPipeline.Exception.MetaDataLoad"), e);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public List<SqlStatement> getSqlStatements(
      IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    this.copyFrom(variables);
    PipelineMeta pipelineMeta = getPipelineMeta(metadataProvider, this);

    return pipelineMeta.getSqlStatements(variables);
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (setLogfile) {
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "logfile",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    }
    if (!Utils.isEmpty(filename)) {
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "filename",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    } else {
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "pipeline-name",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "directory",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(filename)) {
      // During this phase, the variable variables hasn't been initialized yet - it seems
      // to happen during the execute. As such, we need to use the workflow meta's resolution
      // of the variables.
      String realFileName = variables.resolve(filename);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realFileName, ResourceType.ACTIONFILE));
      references.add(reference);
    }
    return references;
  }

  /**
   * We're going to load the pipeline meta data referenced here. Then we're going to give it a new
   * filename, modify that filename in this entries. The parent caller will have made a copy of it,
   * so it should be OK to do so.
   *
   * <p>Exports the object to a flat-file system, adding content with filename keys to a set of
   * definitions. The supplied resource naming interface allows the object to name appropriately
   * without worrying about those parts of the implementation specific details.
   *
   * @param variables The variable variables to resolve (environment) variables with.
   * @param definitions The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named
   *     appropriately
   * @param metadataProvider the metadataProvider to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    // Try to load the pipeline from a file.
    // Modify this recursively too...
    //
    // AGAIN: there is no need to clone this action because the caller is responsible for this.
    //
    // First load the pipeline metadata...
    //
    copyFrom(variables);
    PipelineMeta pipelineMeta = getPipelineMeta(metadataProvider, variables);

    // Also go down into the pipeline and export the files there. (mapping recursively down)
    //
    String proposedNewFilename =
        pipelineMeta.exportResources(variables, definitions, namingInterface, metadataProvider);

    // To get a relative path to it, we inject ${Internal.Entry.Current.Directory}
    //
    String newFilename =
        "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + proposedNewFilename;

    // Set the correct filename inside the XML.
    //
    pipelineMeta.setFilename(newFilename);

    // change it in the action
    //
    filename = newFilename;

    return proposedNewFilename;
  }

  public String getLogfile() {
    return logfile;
  }

  /**
   * @return the waitingToFinish
   */
  public boolean isWaitingToFinish() {
    return waitingToFinish;
  }

  /**
   * @param waitingToFinish the waitingToFinish to set
   */
  public void setWaitingToFinish(boolean waitingToFinish) {
    this.waitingToFinish = waitingToFinish;
  }

  public String getRunConfiguration() {
    return runConfiguration;
  }

  public void setRunConfiguration(String runConfiguration) {
    this.runConfiguration = runConfiguration;
  }

  public IPipelineEngine<PipelineMeta> getPipeline() {
    return pipeline;
  }

  /**
   * @return The objects referenced in the transform, like a a pipeline, a workflow, a mapper, a
   *     reducer, a combiner, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "ActionPipeline.ReferencedObject.Description"),
    };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty(filename);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isPipelineDefined(),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the referenced object index to load (in case there are multiple references)
   * @param metadataProvider metadataProvider
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return getPipelineMeta(metadataProvider, variables);
  }

  @Override
  public void setParentWorkflowMeta(WorkflowMeta parentWorkflowMeta) {
    super.setParentWorkflowMeta(parentWorkflowMeta);
  }

  public void prepareFieldNamesParameters(
      List<Parameter> parameters, INamedParameters namedParam, ActionPipeline actionPipeline)
      throws UnknownParamException {
    for (Parameter parameter : parameters) {
      // Grab the parameter value set in the Pipeline action
      // Set parameter.getField() only if exists and if it is not declared any static
      // parameter.getValue()
      //
      String thisValue = namedParam.getParameterValue(parameter.getName());
      // Set value only if is not empty at namedParam and exists in parameter.getField
      if (!Utils.isEmpty(Const.trim(parameter.getField()))) {
        // If is not empty then we have to ask if it exists too in parameter.getValue(), since
        // the values in parameter.getValue() prevail over parameterFieldNames
        // If is empty at parameter.getValue(), then we can finally add that variable with that
        // value
        if (Utils.isEmpty(Const.trim(parameter.getValue()))) {
          actionPipeline.setVariable(parameter.getName(), Const.NVL(thisValue, ""));
        }
      } else {
        // Or if not in parameter.getValue() then we can add that variable with that value too
        actionPipeline.setVariable(parameter.getName(), Const.NVL(thisValue, ""));
      }
    }
  }

  public boolean isExecPerRow() {
    return execPerRow;
  }

  public void setExecPerRow(boolean runEveryResultRow) {
    this.execPerRow = runEveryResultRow;
  }

  public boolean isAddDate() {
    return addDate;
  }

  public boolean isAddTime() {
    return addTime;
  }

  public void setAddDate(boolean addDate) {
    this.addDate = addDate;
  }

  public void setAddTime(boolean addTime) {
    this.addTime = addTime;
  }

  public String getLogext() {
    return logext;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public void setLogfile(String logfile) {
    this.logfile = logfile;
  }

  public void setLogext(String logext) {
    this.logext = logext;
  }

  public boolean isSetLogfile() {
    return setLogfile;
  }

  public LogLevel getLogFileLevel() {
    return logFileLevel;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setSetLogfile(boolean setLogfile) {
    this.setLogfile = setLogfile;
  }

  public void setLogFileLevel(LogLevel logFileLevel) {
    this.logFileLevel = logFileLevel;
  }

  public void setCreateParentFolder(boolean createParentFolder) {
    this.createParentFolder = createParentFolder;
  }

  public boolean isParamsFromPrevious() {
    return paramsFromPrevious;
  }

  public boolean isSetAppendLogfile() {
    return setAppendLogfile;
  }

  public void setParamsFromPrevious(boolean paramsFromPrevious) {
    this.paramsFromPrevious = paramsFromPrevious;
  }

  public void setSetAppendLogfile(boolean setAppendLogfile) {
    this.setAppendLogfile = setAppendLogfile;
  }

  public boolean isClearResultRows() {
    return clearResultRows;
  }

  public boolean isClearResultFiles() {
    return clearResultFiles;
  }

  public void setClearResultRows(boolean clearResultRows) {
    this.clearResultRows = clearResultRows;
  }

  public void setClearResultFiles(boolean clearResultFiles) {
    this.clearResultFiles = clearResultFiles;
  }
}
