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

package org.apache.hop.workflow.actions.special;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.w3c.dom.Node;

import java.util.Calendar;
import java.util.List;

/**
 * This class can contain a few special actions such as Start and Dummy.
 *
 * @author Matt
 * @since 05-11-2003
 */

public class ActionSpecial extends ActionBase implements Cloneable, IAction {
  public static final int NOSCHEDULING = 0;
  public static final int INTERVAL = 1;
  public static final int DAILY = 2;
  public static final int WEEKLY = 3;
  public static final int MONTHLY = 4;

  private boolean start;
  private boolean dummy;
  private boolean repeat = false;
  private int schedulerType = NOSCHEDULING;
  private int intervalSeconds = 0;
  private int intervalMinutes = 60;
  private int dayOfMonth = 1;
  private int weekDay = 1;
  private int minutes = 0;
  private int hour = 12;

  public ActionSpecial() {
    this( null, false, false );
  }

  public ActionSpecial( String name, boolean start, boolean dummy ) {
    super( name, "" );
    this.start = start;
    this.dummy = dummy;
  }

  public Object clone() {
    ActionSpecial je = (ActionSpecial) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "start", start ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "dummy", dummy ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "repeat", repeat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "schedulerType", schedulerType ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "intervalSeconds", intervalSeconds ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "intervalMinutes", intervalMinutes ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "hour", hour ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "minutes", minutes ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "weekDay", weekDay ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "DayOfMonth", dayOfMonth ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      start = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "start" ) );
      dummy = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "dummy" ) );
      repeat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "repeat" ) );
      setSchedulerType( Const.toInt( XmlHandler.getTagValue( entrynode, "schedulerType" ), NOSCHEDULING ) );
      setIntervalSeconds( Const.toInt( XmlHandler.getTagValue( entrynode, "intervalSeconds" ), 0 ) );
      setIntervalMinutes( Const.toInt( XmlHandler.getTagValue( entrynode, "intervalMinutes" ), 0 ) );
      setHour( Const.toInt( XmlHandler.getTagValue( entrynode, "hour" ), 0 ) );
      setMinutes( Const.toInt( XmlHandler.getTagValue( entrynode, "minutes" ), 0 ) );
      setWeekDay( Const.toInt( XmlHandler.getTagValue( entrynode, "weekDay" ), 0 ) );
      setDayOfMonth( Const.toInt( XmlHandler.getTagValue( entrynode, "dayOfMonth" ), 0 ) );
    } catch ( HopException e ) {
      throw new HopXmlException( "Unable to load action of type 'special' from XML node", e );
    }
  }

  public boolean isStart() {
    return start;
  }

  public boolean isDummy() {
    return dummy;
  }

  public Result execute( Result previousResult, int nr ) throws HopWorkflowException {
    Result result = previousResult;

    if ( isStart() ) {
      try {
        long sleepTime = getNextExecutionTime();
        if ( sleepTime > 0 ) {
          parentWorkflow.getLogChannel().logBasic(
            parentWorkflow.getWorkflowName(),
            "Sleeping: " + ( sleepTime / 1000 / 60 ) + " minutes (sleep time=" + sleepTime + ")" );
          long totalSleep = 0L;
          while ( totalSleep < sleepTime && !parentWorkflow.isStopped() ) {
            Thread.sleep( 1000L );
            totalSleep += 1000L;
          }
        }
      } catch ( InterruptedException e ) {
        throw new HopWorkflowException( e );
      }
      result = previousResult;
      result.setResult( true );
    } else if ( isDummy() ) {
      result = previousResult;
    }
    return result;
  }

  private long getNextExecutionTime() {
    switch ( schedulerType ) {
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
    if ( amHour > 12 ) {
      amHour = amHour - 12;
      calendar.set( Calendar.AM_PM, Calendar.PM );
    } else {
      calendar.set( Calendar.AM_PM, Calendar.AM );
    }
    calendar.set( Calendar.HOUR, amHour );
    calendar.set( Calendar.MINUTE, minutes );
    calendar.set( Calendar.DAY_OF_MONTH, dayOfMonth );
    if ( calendar.getTimeInMillis() <= nowMillis ) {
      calendar.add( Calendar.MONTH, 1 );
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  private long getNextWeeklyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = hour;
    if ( amHour > 12 ) {
      amHour = amHour - 12;
      calendar.set( Calendar.AM_PM, Calendar.PM );
    } else {
      calendar.set( Calendar.AM_PM, Calendar.AM );
    }
    calendar.set( Calendar.HOUR, amHour );
    calendar.set( Calendar.MINUTE, minutes );
    calendar.set( Calendar.DAY_OF_WEEK, weekDay + 1 );
    if ( calendar.getTimeInMillis() <= nowMillis ) {
      calendar.add( Calendar.WEEK_OF_YEAR, 1 );
    }
    return calendar.getTimeInMillis() - nowMillis;
  }

  private long getNextDailyExecutionTime() {
    Calendar calendar = Calendar.getInstance();

    long nowMillis = calendar.getTimeInMillis();
    int amHour = hour;
    if ( amHour > 12 ) {
      amHour = amHour - 12;
      calendar.set( Calendar.AM_PM, Calendar.PM );
    } else {
      calendar.set( Calendar.AM_PM, Calendar.AM );
    }
    calendar.set( Calendar.HOUR, amHour );
    calendar.set( Calendar.MINUTE, minutes );
    if ( calendar.getTimeInMillis() <= nowMillis ) {
      calendar.add( Calendar.DAY_OF_MONTH, 1 );
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

  public void setDayOfMonth( int dayOfMonth ) {
    this.dayOfMonth = dayOfMonth;
  }

  public void setHour( int hour ) {
    this.hour = hour;
  }

  public void setMinutes( int minutes ) {
    this.minutes = minutes;
  }

  public void setWeekDay( int weekDay ) {
    this.weekDay = weekDay;
  }

  public void setSchedulerType( int schedulerType ) {
    this.schedulerType = schedulerType;
  }

  public boolean isRepeat() {
    return repeat;
  }

  public void setRepeat( boolean repeat ) {
    this.repeat = repeat;
  }

  public int getIntervalSeconds() {
    return intervalSeconds;
  }

  public void setIntervalSeconds( int intervalSeconds ) {
    this.intervalSeconds = intervalSeconds;
  }

  public int getIntervalMinutes() {
    return intervalMinutes;
  }

  public void setIntervalMinutes( int intervalMinutes ) {
    this.intervalMinutes = intervalMinutes;
  }

  /**
   * @param dummy the dummy to set
   */
  public void setDummy( boolean dummy ) {
    this.dummy = dummy;
  }

  /**
   * @param start the start to set
   */
  public void setStart( boolean start ) {
    this.start = start;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {

  }

}
