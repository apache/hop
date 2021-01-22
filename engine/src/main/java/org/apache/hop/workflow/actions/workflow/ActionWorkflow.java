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

package org.apache.hop.workflow.actions.workflow;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.LogChannelFileWriter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
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
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Recursive definition of a Workflow. This transform means that an entire Workflow has to be
 * executed. It can be the same Workflow, but just make sure that you don't get an endless loop.
 * Provide an escape routine using Eval.
 *
 * @author Matt
 * @since 01-10-2003, Rewritten on 18-06-2004
 */
@Action(
    id = "WORKFLOW",
    image = "ui/images/workflow.svg",
    name = "i18n::ActionWorkflow.Name",
    description = "i18n::ActionWorkflow.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/workflow.html")
public class ActionWorkflow extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionWorkflow.class; // For Translator

  private String filename;

  public boolean paramsFromPrevious;
  public boolean execPerRow;

  public String[] parameters;
  public String[] parameterFieldNames;
  public String[] parameterValues;

  public boolean setLogfile;
  public String logfile, logext;
  public boolean addDate, addTime;
  public LogLevel logFileLevel;

  public boolean parallel;
  public boolean setAppendLogfile;
  public boolean createParentFolder;

  public boolean waitingToFinish = true;
  public boolean followingAbortRemotely;

  public boolean passingAllParameters = true;

  private boolean passingExport;

  private String runConfiguration;

  public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.NOTHING;

  private IWorkflowEngine<WorkflowMeta> workflow;

  public ActionWorkflow(String name) {
    super(name, "");
  }

  public ActionWorkflow() {
    this("");
    clear();
  }

  private void allocateArgs(int nrArgs) {}

  private void allocateParams(int nrParameters) {
    parameters = new String[nrParameters];
    parameterFieldNames = new String[nrParameters];
    parameterValues = new String[nrParameters];
  }

  @Override
  public Object clone() {
    ActionWorkflow je = (ActionWorkflow) super.clone();
    if (parameters != null) {
      int nrParameters = parameters.length;
      je.allocateParams(nrParameters);
      System.arraycopy(parameters, 0, je.parameters, 0, nrParameters);
      System.arraycopy(parameterFieldNames, 0, je.parameterFieldNames, 0, nrParameters);
      System.arraycopy(parameterValues, 0, je.parameterValues, 0, nrParameters);
    }
    return je;
  }

  public void setFileName(String n) {
    filename = n;
  }

  /**
   * @return the filename
   * @deprecated use getFilename() instead.
   */
  @Deprecated
  public String getFileName() {
    return filename;
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public String getRealFilename() {
    return resolve(getFilename());
  }

  public boolean isPassingExport() {
    return passingExport;
  }

  public void setPassingExport(boolean passingExport) {
    this.passingExport = passingExport;
  }

  public String getRunConfiguration() {
    return runConfiguration;
  }

  public void setRunConfiguration(String runConfiguration) {
    this.runConfiguration = runConfiguration;
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
  public String getXml() {
    StringBuilder retval = new StringBuilder(400);

    retval.append(super.getXml());

    retval.append("      ").append(XmlHandler.addTagValue("run_configuration", runConfiguration));

    retval.append("      ").append(XmlHandler.addTagValue("filename", filename));

    retval
        .append("      ")
        .append(XmlHandler.addTagValue("params_from_previous", paramsFromPrevious));
    retval.append("      ").append(XmlHandler.addTagValue("exec_per_row", execPerRow));
    retval.append("      ").append(XmlHandler.addTagValue("set_logfile", setLogfile));
    retval.append("      ").append(XmlHandler.addTagValue("logfile", logfile));
    retval.append("      ").append(XmlHandler.addTagValue("logext", logext));
    retval.append("      ").append(XmlHandler.addTagValue("add_date", addDate));
    retval.append("      ").append(XmlHandler.addTagValue("add_time", addTime));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "loglevel",
                logFileLevel != null ? logFileLevel.getCode() : DEFAULT_LOG_LEVEL.getCode()));
    retval.append("      ").append(XmlHandler.addTagValue("wait_until_finished", waitingToFinish));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("follow_abort_remote", followingAbortRemotely));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("create_parent_folder", createParentFolder));
    retval.append("      ").append(XmlHandler.addTagValue("pass_export", passingExport));
    retval.append("      ").append(XmlHandler.addTagValue("run_configuration", runConfiguration));

    if (parameters != null) {
      retval.append("      ").append(XmlHandler.openTag("parameters"));

      retval
          .append("        ")
          .append(XmlHandler.addTagValue("pass_all_parameters", passingAllParameters));

      for (int i = 0; i < parameters.length; i++) {
        // This is a better way of making the XML file than the arguments.
        retval.append("            ").append(XmlHandler.openTag("parameter"));

        retval.append("            ").append(XmlHandler.addTagValue("name", parameters[i]));
        retval
            .append("            ")
            .append(XmlHandler.addTagValue("stream_name", parameterFieldNames[i]));
        retval.append("            ").append(XmlHandler.addTagValue("value", parameterValues[i]));

        retval.append("            ").append(XmlHandler.closeTag("parameter"));
      }
      retval.append("      ").append(XmlHandler.closeTag("parameters"));
    }
    retval.append("      ").append(XmlHandler.addTagValue("set_append_logfile", setAppendLogfile));

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);

      runConfiguration = XmlHandler.getTagValue(entrynode, "run_configuration");
      filename = XmlHandler.getTagValue(entrynode, "filename");

      paramsFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "params_from_previous"));
      execPerRow = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "exec_per_row"));
      setLogfile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "set_logfile"));
      addDate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_date"));
      addTime = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_time"));
      logfile = XmlHandler.getTagValue(entrynode, "logfile");
      logext = XmlHandler.getTagValue(entrynode, "logext");
      logFileLevel = LogLevel.getLogLevelForCode(XmlHandler.getTagValue(entrynode, "loglevel"));
      setAppendLogfile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "set_append_logfile"));
      passingExport = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "pass_export"));
      createParentFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "create_parent_folder"));
      runConfiguration = XmlHandler.getTagValue(entrynode, "run_configuration");

      String wait = XmlHandler.getTagValue(entrynode, "wait_until_finished");
      if (Utils.isEmpty(wait)) {
        waitingToFinish = true;
      } else {
        waitingToFinish = "Y".equalsIgnoreCase(wait);
      }

      followingAbortRemotely =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "follow_abort_remote"));

      // How many arguments?
      int argnr = 0;
      while (XmlHandler.getTagValue(entrynode, "argument" + argnr) != null) {
        argnr++;
      }
      allocateArgs(argnr);

      Node parametersNode = XmlHandler.getSubNode(entrynode, "parameters");

      String passAll = XmlHandler.getTagValue(parametersNode, "pass_all_parameters");
      passingAllParameters = Utils.isEmpty(passAll) || "Y".equalsIgnoreCase(passAll);

      int nrParameters = XmlHandler.countNodes(parametersNode, "parameter");
      allocateParams(nrParameters);

      for (int i = 0; i < nrParameters; i++) {
        Node knode = XmlHandler.getSubNodeByNr(parametersNode, "parameter", i);

        parameters[i] = XmlHandler.getTagValue(knode, "name");
        parameterFieldNames[i] = XmlHandler.getTagValue(knode, "stream_name");
        parameterValues[i] = XmlHandler.getTagValue(knode, "value");
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load 'workflow' action from XML node", xe);
    }
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setEntryNr(nr);

    LogChannelFileWriter logChannelFileWriter = null;
    LogLevel jobLogLevel = parentWorkflow.getLogLevel();

    if (setLogfile) {
      String realLogFilename = resolve(getLogFilename());
      // We need to check here the log filename
      // if we do not have one, we must fail
      if (Utils.isEmpty(realLogFilename)) {
        logError(BaseMessages.getString(PKG, "ActionWorkflow.Exception.LogFilenameMissing"));
        result.setNrErrors(1);
        result.setResult(false);
        return result;
      }

      // create parent folder?
      if (!createParentFolder(realLogFilename)) {
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
            "Unable to open file appender for file [" + getLogFilename() + "] : " + e.toString());
        logError(Const.getStackTracker(e));
        result.setNrErrors(1);
        result.setResult(false);
        return result;
      }
      jobLogLevel = logFileLevel;
    }

    try {
      // First load the workflow, outside of the loop...
      if (parentWorkflow.getWorkflowMeta() != null) {
        // reset the internal variables again.
        // Maybe we should split up the variables even more like in UNIX shells.
        // The internal variables need to be reset to be able use them properly
        // in 2 sequential sub workflows.
        parentWorkflow.getWorkflowMeta().setInternalHopVariables(this);
      }

      // Explain what we are loading...
      //
      logDetailed("Loading workflow from XML file : [" + resolve(filename) + "]");

      WorkflowMeta workflowMeta = getWorkflowMeta(getMetadataProvider(), this);

      // Verify that we loaded something, complain if we did not...
      //
      if (workflowMeta == null) {
        throw new HopException("Unable to load the workflow: please specify a filename");
      }

      verifyRecursiveExecution(parentWorkflow, workflowMeta);

      int iteration = 0;

      copyFrom(parentWorkflow);
      setParentVariables(parentWorkflow);

      RowMetaAndData resultRow = null;
      boolean first = true;
      List<RowMetaAndData> rows = new ArrayList<>(result.getRows());

      while ((first && !execPerRow)
          || (execPerRow && rows != null && iteration < rows.size() && result.getNrErrors() == 0)) {
        first = false;

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

        // First (optionally) copy all the parameter values from the parent workflow
        //
        if (paramsFromPrevious) {
          String[] parentParameters = parentWorkflow.listParameters();
          for (int idx = 0; idx < parentParameters.length; idx++) {
            String par = parentParameters[idx];
            String def = parentWorkflow.getParameterDefault(par);
            String val = parentWorkflow.getParameterValue(par);
            String des = parentWorkflow.getParameterDescription(par);

            namedParam.addParameterDefinition(par, def, des);
            namedParam.setParameterValue(par, val);
          }
        }

        // Now add those parameter values specified by the user in the action
        //
        if (parameters != null) {
          for (int idx = 0; idx < parameters.length; idx++) {
            if (!Utils.isEmpty(parameters[idx])) {

              // If it's not yet present in the parent workflow, add it...
              //
              if (Const.indexOfString(parameters[idx], namedParam.listParameters()) < 0) {
                // We have a parameter
                try {
                  namedParam.addParameterDefinition(parameters[idx], "", "Action runtime");
                } catch (DuplicateParamException e) {
                  // Should never happen
                  //
                  logError("Duplicate parameter definition for " + parameters[idx]);
                }
              }

              if (Utils.isEmpty(Const.trim(parameterFieldNames[idx]))) {
                namedParam.setParameterValue(
                    parameters[idx], Const.NVL(resolve(parameterValues[idx]), ""));
              } else {
                // something filled in, in the field column...
                //
                String value = "";
                if (resultRow != null) {
                  value = resultRow.getString(parameterFieldNames[idx], "");
                }
                namedParam.setParameterValue(parameters[idx], value);
              }
            }
          }
        }

        Result oneResult = new Result();

        List<RowMetaAndData> sourceRows = null;

        if (execPerRow) {
          // Execute for each input row
          // Just pass a single row
          //
          List<RowMetaAndData> newList = new ArrayList<>();
          newList.add(resultRow);
          sourceRows = newList;

          if (paramsFromPrevious) { // Copy the input the parameters

            if (parameters != null) {
              for (int idx = 0; idx < parameters.length; idx++) {
                if (!Utils.isEmpty(parameters[idx])) {
                  // We have a parameter
                  if (Utils.isEmpty(Const.trim(parameterFieldNames[idx]))) {
                    namedParam.setParameterValue(
                        parameters[idx], Const.NVL(resolve(parameterValues[idx]), ""));
                  } else {
                    String fieldValue = "";

                    if (resultRow != null) {
                      fieldValue = resultRow.getString(parameterFieldNames[idx], "");
                    }
                    // Get the value from the input stream
                    namedParam.setParameterValue(parameters[idx], Const.NVL(fieldValue, ""));
                  }
                }
              }
            }
          }
        } else {

          // Keep it as it was...
          //
          sourceRows = result.getRows();

          if (paramsFromPrevious) { // Copy the input the parameters

            if (parameters != null) {
              for (int idx = 0; idx < parameters.length; idx++) {
                if (!Utils.isEmpty(parameters[idx])) {
                  // We have a parameter
                  if (Utils.isEmpty(Const.trim(parameterFieldNames[idx]))) {
                    namedParam.setParameterValue(
                        parameters[idx], Const.NVL(resolve(parameterValues[idx]), ""));
                  } else {
                    String fieldValue = "";

                    if (resultRow != null) {
                      fieldValue = resultRow.getString(parameterFieldNames[idx], "");
                    }
                    // Get the value from the input stream
                    namedParam.setParameterValue(parameters[idx], Const.NVL(fieldValue, ""));
                  }
                }
              }
            }
          }
        }

        // Create a new workflow
        //
        workflow =
            WorkflowEngineFactory.createWorkflowEngine(
                this, resolve(runConfiguration), getMetadataProvider(), workflowMeta, this);
        workflow.setParentWorkflow(parentWorkflow);
        workflow.setLogLevel(jobLogLevel);
        workflow.shareWith(this);
        workflow.setInternalHopVariables();
        workflow.copyParametersFromDefinitions(workflowMeta);
        workflow.setInteractive(parentWorkflow.isInteractive());
        if (workflow.isInteractive()) {
          workflow.getActionListeners().addAll(parentWorkflow.getActionListeners());
        }

        // Set the parameters calculated above on this instance.
        //
        workflow.clearParameterValues();
        String[] parameterNames = workflow.listParameters();
        for (int idx = 0; idx < parameterNames.length; idx++) {
          // Grab the parameter value set in the action
          //
          String thisValue = namedParam.getParameterValue(parameterNames[idx]);
          if (!Utils.isEmpty(thisValue)) {
            // Set the value as specified by the user in the action
            //
            workflow.setParameterValue(parameterNames[idx], thisValue);
          } else {
            // See if the parameter had a value set in the parent workflow...
            // This value should pass down to the sub-workflow if that's what we
            // opted to do.
            //
            if (isPassingAllParameters()) {
              String parentValue = parentWorkflow.getParameterValue(parameterNames[idx]);
              if (!Utils.isEmpty(parentValue)) {
                workflow.setParameterValue(parameterNames[idx], parentValue);
              }
            }
          }
        }
        workflow.activateParameters(workflow);

        // Set the source rows we calculated above...
        //
        workflow.setSourceRows(sourceRows);

        // Link the workflow with the sub-workflow
        parentWorkflow.getWorkflowTracker().addWorkflowTracker(workflow.getWorkflowTracker());

        // Link both ways!
        workflow.getWorkflowTracker().setParentWorkflowTracker(parentWorkflow.getWorkflowTracker());

        ActionWorkflowRunner runner = new ActionWorkflowRunner(workflow, result, nr, log);
        Thread workflowRunnerThread = new Thread(runner);
        // PDI-6518
        // added UUID to thread name, otherwise threads do share names if workflows actions are
        // executed in parallel in a
        // parent workflow
        // if that happens, contained pipelines start closing each other's connections
        workflowRunnerThread.setName(
            Const.NVL(
                    workflow.getWorkflowMeta().getName(), workflow.getWorkflowMeta().getFilename())
                + " UUID: "
                + UUID.randomUUID().toString());
        workflowRunnerThread.start();

        // Keep running until we're done.
        //
        while (!runner.isFinished() && !parentWorkflow.isStopped()) {
          try {
            Thread.sleep(0, 1);
          } catch (InterruptedException e) {
            // Ignore
          }
        }

        // if the parent-workflow was stopped, stop the sub-workflow too...
        if (parentWorkflow.isStopped()) {
          workflow.stopExecution();
          runner.waitUntilFinished(); // Wait until finished!
        }

        oneResult = runner.getResult();

        result.clear(); // clear only the numbers, NOT the files or rows.
        result.add(oneResult);

        // Set the result rows too, if any ...
        if (!Utils.isEmpty(oneResult.getRows())) {
          result.setRows(new ArrayList<>(oneResult.getRows()));
        }

        // if one of them fails (in the loop), increase the number of errors
        //
        if (oneResult.getResult() == false) {
          result.setNrErrors(result.getNrErrors() + 1);
        }

        iteration++;
      }
    } catch (HopException ke) {
      logError("Error running action 'workflow' : ", ke);

      result.setResult(false);
      result.setNrErrors(1L);
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

    if (result.getNrErrors() > 0) {
      result.setResult(false);
    } else {
      result.setResult(true);
    }

    return result;
  }

  private boolean createParentFolder(String filename) {
    // Check for parent folder
    FileObject parentfolder = null;
    boolean resultat = true;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject(filename).getParent();
      if (!parentfolder.exists()) {
        if (createParentFolder) {
          if (log.isDebug()) {
            log.logDebug(
                BaseMessages.getString(
                    PKG,
                    "ActionWorkflow.Log.ParentLogFolderNotExist",
                    parentfolder.getName().toString()));
          }
          parentfolder.createFolder();
          if (log.isDebug()) {
            log.logDebug(
                BaseMessages.getString(
                    PKG,
                    "ActionWorkflow.Log.ParentLogFolderCreated",
                    parentfolder.getName().toString()));
          }
        } else {
          log.logError(
              BaseMessages.getString(
                  PKG,
                  "ActionWorkflow.Log.ParentLogFolderNotExist",
                  parentfolder.getName().toString()));
          resultat = false;
        }
      } else {
        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionWorkflow.Log.ParentLogFolderExists",
                  parentfolder.getName().toString()));
        }
      }
    } catch (Exception e) {
      resultat = false;
      log.logError(
          BaseMessages.getString(PKG, "ActionWorkflow.Error.ChekingParentLogFolderTitle"),
          BaseMessages.getString(
              PKG,
              "ActionWorkflow.Error.ChekingParentLogFolder",
              parentfolder.getName().toString()),
          e);
    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
          parentfolder = null;
        } catch (Exception ex) {
          // Ignore
        }
      }
    }

    return resultat;
  }

  /**
   * Make sure that we are not loading workflows recursively...
   *
   * @param parentWorkflow the parent workflow
   * @param workflowMeta the workflow metadata
   * @throws HopException in case both workflows are loaded from the same source
   */
  private void verifyRecursiveExecution(
      IWorkflowEngine<WorkflowMeta> parentWorkflow, WorkflowMeta workflowMeta) throws HopException {

    if (parentWorkflow == null) {
      return; // OK!
    }

    WorkflowMeta parentWorkflowMeta = parentWorkflow.getWorkflowMeta();

    if (parentWorkflowMeta.getName() == null && workflowMeta.getName() != null) {
      return; // OK
    }
    if (parentWorkflowMeta.getName() != null && workflowMeta.getName() == null) {
      return; // OK as well.
    }

    // Verify the filename for recursive execution
    //
    if (workflowMeta.getFilename() != null
        && workflowMeta.getFilename().equals(parentWorkflowMeta.getFilename())) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionWorkflowError.Recursive", workflowMeta.getFilename()));
    }

    // Also compare with the grand-parent (if there is any)
    verifyRecursiveExecution(parentWorkflow.getParentWorkflow(), workflowMeta);
  }

  @Override
  public void clear() {
    super.clear();

    filename = null;
    addDate = false;
    addTime = false;
    logfile = null;
    logext = null;
    setLogfile = false;
    setAppendLogfile = false;
    runConfiguration = null;
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
    WorkflowMeta workflowMeta = getWorkflowMeta(metadataProvider, variables);
    return workflowMeta.getSqlStatements(metadataProvider, null, variables);
  }

  public WorkflowMeta getWorkflowMeta(IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    WorkflowMeta workflowMeta = null;
    try {
      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      IVariables tmpSpace = r.resolveCurrentDirectory(variables, parentWorkflow, getFilename());

      String realFilename = tmpSpace.resolve(getFilename());
      workflowMeta = new WorkflowMeta(tmpSpace, realFilename, metadataProvider);
      if (workflowMeta != null) {
        workflowMeta.setMetadataProvider(metadataProvider);
      }
      return workflowMeta;
    } catch (Exception e) {
      throw new HopException("Unexpected error during workflow metadata load", e);
    }
  }

  /** @return Returns the runEveryResultRow. */
  public boolean isExecPerRow() {
    return execPerRow;
  }

  /** @param runEveryResultRow The runEveryResultRow to set. */
  public void setExecPerRow(boolean runEveryResultRow) {
    this.execPerRow = runEveryResultRow;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(filename)) {
      String realFileName = resolve(filename);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realFileName, ResourceType.ACTIONFILE));
      references.add(reference);
    }
    return references;
  }

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of
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
    // Try to load the pipeline from file.
    // Modify this recursively too...
    //
    // AGAIN: there is no need to clone this action because the caller is
    // responsible for this.
    //
    // First load the workflow meta data...
    //
    copyFrom(variables); // To make sure variables are available.
    WorkflowMeta workflowMeta = getWorkflowMeta(metadataProvider, variables);

    // Also go down into the workflow and export the files there. (going down
    // recursively)
    //
    String proposedNewFilename =
        workflowMeta.exportResources(this, definitions, namingInterface, metadataProvider);

    // To get a relative path to it, we inject
    // ${Internal.Entry.Current.Directory}
    //
    String newFilename =
        "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + proposedNewFilename;

    // Set the filename in the workflow
    //
    workflowMeta.setFilename(newFilename);

    // change it in the action
    //
    filename = newFilename;

    return proposedNewFilename;
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
  }

  protected String getLogfile() {
    return logfile;
  }

  /** @return the waitingToFinish */
  public boolean isWaitingToFinish() {
    return waitingToFinish;
  }

  /** @param waitingToFinish the waitingToFinish to set */
  public void setWaitingToFinish(boolean waitingToFinish) {
    this.waitingToFinish = waitingToFinish;
  }

  /** @return the followingAbortRemotely */
  public boolean isFollowingAbortRemotely() {
    return followingAbortRemotely;
  }

  /** @param followingAbortRemotely the followingAbortRemotely to set */
  public void setFollowingAbortRemotely(boolean followingAbortRemotely) {
    this.followingAbortRemotely = followingAbortRemotely;
  }

  public void setLoggingRemoteWork(boolean loggingRemoteWork) {
    // do nothing. for compatibility with IActionRunConfigurable
  }

  /** @return the passingAllParameters */
  public boolean isPassingAllParameters() {
    return passingAllParameters;
  }

  /** @param passingAllParameters the passingAllParameters to set */
  public void setPassingAllParameters(boolean passingAllParameters) {
    this.passingAllParameters = passingAllParameters;
  }

  public IWorkflowEngine<WorkflowMeta> getWorkflow() {
    return workflow;
  }

  private boolean isWorkflowDefined() {
    return !Utils.isEmpty(filename);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isWorkflowDefined(),
    };
  }

  /**
   * @return The objects referenced in the transform, like a a pipeline, a workflow, a mapper, a
   *     reducer, a combiner, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "ActionJob.ReferencedObject.Description"),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the referenced object index to load (in case there are multiple references)
   * @param metadataProvider the metadataProvider
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return getWorkflowMeta(metadataProvider, variables);
  }
}
