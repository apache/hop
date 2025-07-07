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

package org.apache.hop.workflow.actions.start;

import java.util.Calendar;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** The start action is starting point for workflow execution. */
@Action(
    id = ActionStart.ID,
    image = "ui/images/start.svg",
    name = "i18n::ActionStart.Name",
    description = "i18n::ActionStart.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    keywords = "i18n::ActionStart.keyword",
    documentationUrl = "/workflow/actions/start.html")
@Getter
@Setter
public class ActionStart extends ActionBase implements Cloneable, IAction {

  /** Action unique identifier" */
  public static final String ID = "SPECIAL";

  public static final int NOSCHEDULING = 0;
  public static final int INTERVAL = 1;
  public static final int DAILY = 2;
  public static final int WEEKLY = 3;
  public static final int MONTHLY = 4;
  public static final String STRING_START_REPEAT_LOOP = "START_REPEAT_LOOP";

  @HopMetadataProperty private boolean repeat = false;
  @HopMetadataProperty private int schedulerType = NOSCHEDULING;
  @HopMetadataProperty private String intervalSeconds = "0";
  @HopMetadataProperty private String intervalMinutes = "60";

  @HopMetadataProperty(key = "DayOfMonth")
  private String dayOfMonth = "1";

  @HopMetadataProperty private String weekDay = "1";
  @HopMetadataProperty private String minutes = "0";
  @HopMetadataProperty private String hour = "12";
  @HopMetadataProperty private boolean doNotWaitOnFirstExecution;

  public ActionStart() {
    this(null);
  }

  public ActionStart(String name) {
    super(name, "");
  }

  @Override
  public ActionStart clone() {
    return (ActionStart) super.clone();
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopWorkflowException {
    try {
      boolean firstExecution = isFirstRepeatExecution();
      long sleepTime = getNextExecutionTime(firstExecution);
      if (sleepTime > 0) {
        parentWorkflow
            .getLogChannel()
            .logBasic(
                parentWorkflow.getWorkflowName(),
                "Sleeping: " + (sleepTime / 1000 / 60) + " minutes (sleep time=" + sleepTime + ")");
        long totalSleep = 0L;
        while (totalSleep < sleepTime && !parentWorkflow.isStopped()) {
          Thread.sleep(1000L);
          totalSleep += 1000L;
        }
      }
    } catch (InterruptedException e) {
      throw new HopWorkflowException(e);
    }
    previousResult.setResult(true);
    return previousResult;
  }

  private boolean isFirstRepeatExecution() {
    return parentWorkflow.getExtensionDataMap().get(STRING_START_REPEAT_LOOP) == null;
  }

  private long getNextExecutionTime(boolean firstExecution) {
    // If Repeat is off, we don't wait at all of-course, no matter what the scheduling type says.
    //
    if (!isRepeat()) {
      return 0;
    }

    parentWorkflow.getExtensionDataMap().put(STRING_START_REPEAT_LOOP, "NOT_FIRST");

    // If the repeat option is disabled, we need to
    // In case we don't want to wait on the first execution, return 0:
    //
    if (isDoNotWaitOnFirstExecution() && firstExecution) {
      return 0;
    }
    switch (schedulerType) {
      case NOSCHEDULING:
        return 0;
      case INTERVAL:
        return getNextIntervalExecutionTime();
      case DAILY:
        return getNextDailyExecutionTime();
      case WEEKLY:
        return getNextWeeklyExecutionTime();
      case MONTHLY:
        return getNextMonthlyExecutionTime();
      default:
        break;
    }
    return 0;
  }

  private long getNextIntervalExecutionTime() {
    int interval = Const.toInt(resolve(intervalSeconds), 0) * 1000;
    interval += Const.toInt(resolve(intervalMinutes), -1) * 1000 * 60;
    return interval;
  }

  private long getNextMonthlyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = Const.toInt(resolve(hour), 0);
    if (amHour > 12) {
      amHour = amHour - 12;
      calendar.set(Calendar.AM_PM, Calendar.PM);
    } else {
      calendar.set(Calendar.AM_PM, Calendar.AM);
    }
    calendar.set(Calendar.HOUR, amHour);
    calendar.set(Calendar.MINUTE, Const.toInt(resolve(minutes), 0));
    calendar.set(Calendar.DAY_OF_MONTH, Const.toInt(resolve(dayOfMonth), 1));
    if (calendar.getTimeInMillis() <= nowMillis) {
      calendar.add(Calendar.MONTH, 1);
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  private long getNextWeeklyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = Const.toInt(resolve(hour), 0);
    if (amHour > 12) {
      amHour = amHour - 12;
      calendar.set(Calendar.AM_PM, Calendar.PM);
    } else {
      calendar.set(Calendar.AM_PM, Calendar.AM);
    }
    calendar.set(Calendar.HOUR, amHour);
    calendar.set(Calendar.MINUTE, Const.toInt(resolve(minutes), 0));
    calendar.set(Calendar.DAY_OF_WEEK, Const.toInt(resolve(weekDay), 0) + 1);
    if (calendar.getTimeInMillis() <= nowMillis) {
      calendar.add(Calendar.WEEK_OF_YEAR, 1);
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  private long getNextDailyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = Const.toInt(resolve(hour), 0);
    if (amHour > 12) {
      amHour = amHour - 12;
      calendar.set(Calendar.AM_PM, Calendar.PM);
    } else {
      calendar.set(Calendar.AM_PM, Calendar.AM);
    }
    calendar.set(Calendar.HOUR, amHour);
    calendar.set(Calendar.MINUTE, Const.toInt(resolve(minutes), 0));
    if (calendar.getTimeInMillis() <= nowMillis) {
      calendar.add(Calendar.DAY_OF_MONTH, 1);
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  public boolean evaluates() {
    return false;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public boolean isStart() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // Do nothing
  }
}
