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

package org.apache.hop.workflow.actions.writetolog;

import java.util.Date;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** Action type to output message to the workflow log. */
@Action(
    id = "WRITE_TO_LOG",
    name = "i18n::ActionWriteToLog.Name",
    description = "i18n::ActionWriteToLog.Description",
    image = "WriteToLog.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionWriteToLog.keyword",
    documentationUrl = "/workflow/actions/writetolog.html")
public class ActionWriteToLog extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionWriteToLog.class;

  /** The log level with which the message should be logged. */
  @HopMetadataProperty(key = "loglevel", storeWithCode = true)
  private LogLevel actionLogLevel;

  @HopMetadataProperty(key = "logsubject")
  private String logSubject;

  @HopMetadataProperty(key = "logmessage")
  private String logMessage;

  public ActionWriteToLog(String n) {
    super(n, "");
    actionLogLevel = LogLevel.BASIC;
    logMessage = null;
    logSubject = null;
  }

  public ActionWriteToLog() {
    this("");
  }

  @Override
  public Object clone() {
    ActionWriteToLog action = (ActionWriteToLog) super.clone();
    return action;
  }

  private class LogWriterObject implements ILoggingObject {

    private ILogChannel log;
    private LogLevel logLevel;
    private ILoggingObject parent;
    private String subject;
    private String containerObjectId;

    public LogWriterObject(String subject, ILoggingObject parent, LogLevel logLevel) {
      this.subject = subject;
      this.parent = parent;
      this.logLevel = logLevel;
      this.log = new LogChannel(this, parent);
      this.containerObjectId = log.getContainerObjectId();
    }

    @Override
    public String getFilename() {
      return null;
    }

    @Override
    public String getLogChannelId() {
      return log.getLogChannelId();
    }

    @Override
    public String getObjectCopy() {
      return null;
    }

    @Override
    public String getObjectName() {
      return subject;
    }

    @Override
    public LoggingObjectType getObjectType() {
      return LoggingObjectType.ACTION;
    }

    @Override
    public ILoggingObject getParent() {
      return parent;
    }

    public ILogChannel getLogChannel() {
      return log;
    }

    @Override
    public LogLevel getLogLevel() {
      return logLevel;
    }

    /**
     * @return the execution container object id
     */
    @Override
    public String getContainerId() {
      return containerObjectId;
    }

    /** Stub */
    @Override
    public Date getRegistrationDate() {
      return null;
    }

    @Override
    public boolean isGatheringMetrics() {
      return parent.isGatheringMetrics();
    }

    @Override
    public void setGatheringMetrics(boolean gatheringMetrics) {
      parent.setGatheringMetrics(gatheringMetrics);
    }

    @Override
    public boolean isForcingSeparateLogging() {
      return parent.isForcingSeparateLogging();
    }

    @Override
    public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
      parent.setForcingSeparateLogging(forcingSeparateLogging);
    }
  }

  protected ILogChannel createLogChannel() {
    String subject = Const.nullToEmpty(resolve(getLogSubject()));
    LogWriterObject logWriterObject =
        new LogWriterObject(subject, this, parentWorkflow.getLogLevel());
    return logWriterObject.getLogChannel();
  }

  /** Output message to workflow log. */
  public boolean evaluate(Result result) {
    ILogChannel logChannel = createLogChannel();
    String message = Const.nullToEmpty(resolve(getLogMessage()));

    // Filter out empty messages and those that are not visible with the workflow's log level
    if (Utils.isEmpty(message) || !getActionLogLevel().isVisible(logChannel.getLogLevel())) {
      return true;
    }

    try {
      switch (getActionLogLevel()) {
        case ERROR -> logChannel.logError(message + Const.CR);
        case MINIMAL -> logChannel.logMinimal(message + Const.CR);
        case BASIC -> logChannel.logBasic(message + Const.CR);
        case DETAILED -> logChannel.logDetailed(message + Const.CR);
        case DEBUG -> logChannel.logDebug(message + Const.CR);
        case ROWLEVEL -> logChannel.logRowlevel(message + Const.CR);
        case NOTHING -> {}
      }

      return true;
    } catch (Exception e) {
      result.setNrErrors(1);
      logError(
          BaseMessages.getString(PKG, "WriteToLog.Error.Label"),
          BaseMessages.getString(PKG, "WriteToLog.Error.Description") + " : " + e.toString());
      return false;
    }
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param prevResult The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result prevResult, int nr) {
    prevResult.setResult(evaluate(prevResult));
    return prevResult;
  }

  @Override
  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  public String getLogMessage() {
    if (logMessage == null) {
      logMessage = "";
    }
    return logMessage;
  }

  public String getLogSubject() {
    if (logSubject == null) {
      logSubject = "";
    }
    return logSubject;
  }

  public void setLogMessage(String message) {
    this.logMessage = message;
  }

  public void setLogSubject(String subject) {
    this.logSubject = subject;
  }

  public LogLevel getActionLogLevel() {
    return actionLogLevel;
  }

  public void setActionLogLevel(LogLevel level) {
    this.actionLogLevel = level;
  }
}
