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

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

import java.util.Calendar;
import java.util.List;

/** The start action is starting point for workflow execution. */
@Action(
    id = ActionStart.ID,
    image = "ui/images/start.svg",
    name = "i18n::ActionStart.Name",
    description = "i18n::ActionStart.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/start.html")
public class ActionStart extends ActionBase implements Cloneable, IAction {

  /** Action unique identifier" */
  public static final String ID = "SPECIAL";

  public static final int NOSCHEDULING = 0;
  public static final int INTERVAL = 1;
  public static final int DAILY = 2;
  public static final int WEEKLY = 3;
  public static final int MONTHLY = 4;

  private boolean repeat = false;
  private int schedulerType = NOSCHEDULING;
  private int intervalSeconds = 0;
  private int intervalMinutes = 60;
  private int dayOfMonth = 1;
  private int weekDay = 1;
  private int minutes = 0;
  private int hour = 12;

  public ActionStart() {
    this(null);
  }

  public ActionStart(String name) {
    super(name, "");
  }

  public Object clone() {
    ActionStart je = (ActionStart) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(200);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("repeat", repeat));
    retval.append("      ").append(XmlHandler.addTagValue("schedulerType", schedulerType));
    retval.append("      ").append(XmlHandler.addTagValue("intervalSeconds", intervalSeconds));
    retval.append("      ").append(XmlHandler.addTagValue("intervalMinutes", intervalMinutes));
    retval.append("      ").append(XmlHandler.addTagValue("hour", hour));
    retval.append("      ").append(XmlHandler.addTagValue("minutes", minutes));
    retval.append("      ").append(XmlHandler.addTagValue("weekDay", weekDay));
    retval.append("      ").append(XmlHandler.addTagValue("DayOfMonth", dayOfMonth));

    return retval.toString();
  }

  public void loadXml(Node actionNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(actionNode);
      repeat = "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, "repeat"));
      setSchedulerType(
          Const.toInt(XmlHandler.getTagValue(actionNode, "schedulerType"), NOSCHEDULING));
      setIntervalSeconds(Const.toInt(XmlHandler.getTagValue(actionNode, "intervalSeconds"), 0));
      setIntervalMinutes(Const.toInt(XmlHandler.getTagValue(actionNode, "intervalMinutes"), 0));
      setHour(Const.toInt(XmlHandler.getTagValue(actionNode, "hour"), 0));
      setMinutes(Const.toInt(XmlHandler.getTagValue(actionNode, "minutes"), 0));
      setWeekDay(Const.toInt(XmlHandler.getTagValue(actionNode, "weekDay"), 0));
      setDayOfMonth(Const.toInt(XmlHandler.getTagValue(actionNode, "dayOfMonth"), 0));
    } catch (HopException e) {
      throw new HopXmlException("Unable to load action of type 'special' from XML node", e);
    }
  }

  public Result execute(Result previousResult, int nr) throws HopWorkflowException {
    Result result = previousResult;

    try {
      long sleepTime = getNextExecutionTime();
      if (sleepTime > 0) {
        parentWorkflow
            .getLogChannel()
            .logBasic(
                parentWorkflow.getWorkflowName(),
                "Sleeping: "
                    + (sleepTime / 1000 / 60)
                    + " minutes (sleep time="
                    + sleepTime
                    + ")");
        long totalSleep = 0L;
        while (totalSleep < sleepTime && !parentWorkflow.isStopped()) {
          Thread.sleep(1000L);
          totalSleep += 1000L;
        }
      }
    } catch (InterruptedException e) {
      throw new HopWorkflowException(e);
    }
    result = previousResult;
    result.setResult(true);
    return result;
  }

  private long getNextExecutionTime() {
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
    return intervalSeconds * 1000 + intervalMinutes * 1000 * 60;
  }

  private long getNextMonthlyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = hour;
    if (amHour > 12) {
      amHour = amHour - 12;
      calendar.set(Calendar.AM_PM, Calendar.PM);
    } else {
      calendar.set(Calendar.AM_PM, Calendar.AM);
    }
    calendar.set(Calendar.HOUR, amHour);
    calendar.set(Calendar.MINUTE, minutes);
    calendar.set(Calendar.DAY_OF_MONTH, dayOfMonth);
    if (calendar.getTimeInMillis() <= nowMillis) {
      calendar.add(Calendar.MONTH, 1);
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  private long getNextWeeklyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = hour;
    if (amHour > 12) {
      amHour = amHour - 12;
      calendar.set(Calendar.AM_PM, Calendar.PM);
    } else {
      calendar.set(Calendar.AM_PM, Calendar.AM);
    }
    calendar.set(Calendar.HOUR, amHour);
    calendar.set(Calendar.MINUTE, minutes);
    calendar.set(Calendar.DAY_OF_WEEK, weekDay + 1);
    if (calendar.getTimeInMillis() <= nowMillis) {
      calendar.add(Calendar.WEEK_OF_YEAR, 1);
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  private long getNextDailyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = hour;
    if (amHour > 12) {
      amHour = amHour - 12;
      calendar.set(Calendar.AM_PM, Calendar.PM);
    } else {
      calendar.set(Calendar.AM_PM, Calendar.AM);
    }
    calendar.set(Calendar.HOUR, amHour);
    calendar.set(Calendar.MINUTE, minutes);
    if (calendar.getTimeInMillis() <= nowMillis) {
      calendar.add(Calendar.DAY_OF_MONTH, 1);
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  public boolean evaluates() {
    return false;
  }

  public boolean isUnconditional() {
    return true;
  }

  public int getSchedulerType() {
    return schedulerType;
  }

  public int getHour() {
    return hour;
  }

  public int getMinutes() {
    return minutes;
  }

  public int getWeekDay() {
    return weekDay;
  }

  public int getDayOfMonth() {
    return dayOfMonth;
  }

  public void setDayOfMonth(int dayOfMonth) {
    this.dayOfMonth = dayOfMonth;
  }

  public void setHour(int hour) {
    this.hour = hour;
  }

  public void setMinutes(int minutes) {
    this.minutes = minutes;
  }

  public void setWeekDay(int weekDay) {
    this.weekDay = weekDay;
  }

  public void setSchedulerType(int schedulerType) {
    this.schedulerType = schedulerType;
  }

  @Override
  public boolean isStart() {
    return true;
  }

  public boolean isRepeat() {
    return repeat;
  }

  public void setRepeat(boolean repeat) {
    this.repeat = repeat;
  }

  public int getIntervalSeconds() {
    return intervalSeconds;
  }

  public void setIntervalSeconds(int intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
  }

  public int getIntervalMinutes() {
    return intervalMinutes;
  }

  public void setIntervalMinutes(int intervalMinutes) {
    this.intervalMinutes = intervalMinutes;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {}
}
