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

package org.apache.hop.pipeline.transforms.systemdata;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * Get information from the System or the supervising pipeline.
 *
 * @author Matt
 * @since 4-aug-2003
 */
public class SystemData extends BaseTransform<SystemDataMeta, SystemDataData> implements ITransform<SystemDataMeta, SystemDataData> {

  public SystemData( TransformMeta transformMeta, SystemDataMeta meta, SystemDataData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private Object[] getSystemData( IRowMeta inputRowMeta, Object[] inputRowData ) throws HopException {
    Object[] row = new Object[ data.outputRowMeta.size() ];
    for ( int i = 0; i < inputRowMeta.size(); i++ ) {
      row[ i ] = inputRowData[ i ]; // no data is changed, clone is not needed here.
    }
    for ( int i = 0, index = inputRowMeta.size(); i < meta.getFieldName().length; i++, index++ ) {
      Calendar cal;

      int argnr = 0;

      switch ( meta.getFieldType()[ i ] ) {
        case TYPE_SYSTEM_INFO_SYSTEM_START:
          row[ index ] = getPipeline().getExecutionStartDate();
          break;
        case TYPE_SYSTEM_INFO_SYSTEM_DATE:
          row[ index ] = new Date();
          break;
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM:
          row[ index ] = getPipeline().getEngineMetrics().getStartDate();
          break;
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_TO:
          row[ index ] = getPipeline().getEngineMetrics().getEndDate();
          break;
        case TYPE_SYSTEM_INFO_JOB_DATE_FROM:
          row[ index ] = getPipeline().getParentWorkflow().getExecutionStartDate();
          break;
        case TYPE_SYSTEM_INFO_JOB_DATE_TO:
          row[ index ] = getPipeline().getParentWorkflow().getExecutionEndDate();
          break;
        case TYPE_SYSTEM_INFO_PREV_DAY_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.DAY_OF_MONTH, -1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_DAY_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.DAY_OF_MONTH, -1 );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_DAY_START:
          cal = Calendar.getInstance();
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_DAY_END:
          cal = Calendar.getInstance();
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_DAY_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_DAY_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_MONTH_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, -1 );
          cal.set( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_MONTH_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, -1 );
          cal.set( Calendar.DAY_OF_MONTH, cal.getActualMaximum( Calendar.DAY_OF_MONTH ) );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_MONTH_START:
          cal = Calendar.getInstance();
          cal.set( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_MONTH_END:
          cal = Calendar.getInstance();
          cal.set( Calendar.DAY_OF_MONTH, cal.getActualMaximum( Calendar.DAY_OF_MONTH ) );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_MONTH_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, 1 );
          cal.set( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_MONTH_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, 1 );
          cal.set( Calendar.DAY_OF_MONTH, cal.getActualMaximum( Calendar.DAY_OF_MONTH ) );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_COPYNR:
          row[ index ] = new Long( getCopy() );
          break;
        case TYPE_SYSTEM_INFO_PIPELINE_NAME:
          row[ index ] = getPipelineMeta().getName();
          break;
        case TYPE_SYSTEM_INFO_MODIFIED_USER:
          row[ index ] = getPipelineMeta().getModifiedUser();
          break;
        case TYPE_SYSTEM_INFO_MODIFIED_DATE:
          row[ index ] = getPipelineMeta().getModifiedDate();
          break;
        case TYPE_SYSTEM_INFO_HOSTNAME_REAL:
          row[ index ] = Const.getHostnameReal();
          break;
        case TYPE_SYSTEM_INFO_HOSTNAME:
          row[ index ] = Const.getHostname();
          break;
        case TYPE_SYSTEM_INFO_IP_ADDRESS:
          try {
            row[ index ] = Const.getIPAddress();
          } catch ( Exception e ) {
            throw new HopException( e );
          }
          break;
        case TYPE_SYSTEM_INFO_FILENAME:
          row[ index ] = getPipelineMeta().getFilename();
          break;
        case TYPE_SYSTEM_INFO_CURRENT_PID:
          row[ index ] = new Long( Management.getPID() );
          break;
        case TYPE_SYSTEM_INFO_JVM_TOTAL_MEMORY:
          row[ index ] = Runtime.getRuntime().totalMemory();
          break;
        case TYPE_SYSTEM_INFO_JVM_FREE_MEMORY:
          row[ index ] = Runtime.getRuntime().freeMemory();
          break;
        case TYPE_SYSTEM_INFO_JVM_MAX_MEMORY:
          row[ index ] = Runtime.getRuntime().maxMemory();
          break;
        case TYPE_SYSTEM_INFO_JVM_AVAILABLE_MEMORY:
          Runtime rt = Runtime.getRuntime();
          row[ index ] = rt.freeMemory() + ( rt.maxMemory() - rt.totalMemory() );
          break;
        case TYPE_SYSTEM_INFO_AVAILABLE_PROCESSORS:
          row[ index ] = (long) Runtime.getRuntime().availableProcessors();
          break;
        case TYPE_SYSTEM_INFO_JVM_CPU_TIME:
          row[ index ] = Management.getJVMCpuTime() / 1000000;
          break;
        case TYPE_SYSTEM_INFO_TOTAL_PHYSICAL_MEMORY_SIZE:
          row[ index ] = Management.getTotalPhysicalMemorySize();
          break;
        case TYPE_SYSTEM_INFO_TOTAL_SWAP_SPACE_SIZE:
          row[ index ] = Management.getTotalSwapSpaceSize();
          break;
        case TYPE_SYSTEM_INFO_COMMITTED_VIRTUAL_MEMORY_SIZE:
          row[ index ] = Management.getCommittedVirtualMemorySize();
          break;
        case TYPE_SYSTEM_INFO_FREE_PHYSICAL_MEMORY_SIZE:
          row[ index ] = Management.getFreePhysicalMemorySize();
          break;
        case TYPE_SYSTEM_INFO_FREE_SWAP_SPACE_SIZE:
          row[ index ] = Management.getFreeSwapSpaceSize();
          break;

        case TYPE_SYSTEM_INFO_PREV_WEEK_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.WEEK_OF_YEAR, -1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_WEEK_END:
          cal = Calendar.getInstance();
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_WEEK_OPEN_END:
          cal = Calendar.getInstance( Locale.ROOT );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          cal.add( Calendar.DAY_OF_WEEK, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_WEEK_START_US:
          cal = Calendar.getInstance( Locale.US );
          cal.add( Calendar.WEEK_OF_YEAR, -1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_WEEK_END_US:
          cal = Calendar.getInstance( Locale.US );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_WEEK_START:
          cal = Calendar.getInstance();
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_WEEK_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.WEEK_OF_YEAR, 1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_WEEK_OPEN_END:
          cal = Calendar.getInstance( Locale.ROOT );
          cal.add( Calendar.WEEK_OF_YEAR, 1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          cal.add( Calendar.DAY_OF_WEEK, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_WEEK_START_US:
          cal = Calendar.getInstance( Locale.US );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_WEEK_END_US:
          cal = Calendar.getInstance( Locale.US );
          cal.add( Calendar.WEEK_OF_YEAR, 1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_WEEK_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.WEEK_OF_YEAR, 1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_WEEK_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.WEEK_OF_YEAR, 2 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_WEEK_OPEN_END:
          cal = Calendar.getInstance( Locale.ROOT );
          cal.add( Calendar.WEEK_OF_YEAR, 2 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          cal.add( Calendar.DAY_OF_WEEK, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_WEEK_START_US:
          cal = Calendar.getInstance( Locale.US );
          cal.add( Calendar.WEEK_OF_YEAR, 1 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_WEEK_END_US:
          cal = Calendar.getInstance( Locale.US );
          cal.add( Calendar.WEEK_OF_YEAR, 2 );
          cal.set( Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, -1 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_QUARTER_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, -3 - ( cal.get( Calendar.MONTH ) % 3 ) );
          cal.set( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_QUARTER_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, -1 - ( cal.get( Calendar.MONTH ) % 3 ) );
          cal.set( Calendar.DAY_OF_MONTH, cal.getActualMaximum( Calendar.DATE ) );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_QUARTER_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, 0 - ( cal.get( Calendar.MONTH ) % 3 ) );
          cal.set( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_QUARTER_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, 2 - ( cal.get( Calendar.MONTH ) % 3 ) );
          cal.set( Calendar.DAY_OF_MONTH, cal.getActualMaximum( Calendar.DATE ) );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_QUARTER_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, 3 - ( cal.get( Calendar.MONTH ) % 3 ) );
          cal.set( Calendar.DAY_OF_MONTH, 1 );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_QUARTER_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.MONTH, 5 - ( cal.get( Calendar.MONTH ) % 3 ) );
          cal.set( Calendar.DAY_OF_MONTH, cal.getActualMaximum( Calendar.DATE ) );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_YEAR_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.YEAR, -1 );
          cal.set( Calendar.DAY_OF_YEAR, cal.getActualMinimum( Calendar.DATE ) );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREV_YEAR_END:
          cal = Calendar.getInstance();
          cal.set( Calendar.DAY_OF_YEAR, cal.getActualMinimum( Calendar.DATE ) );
          cal.add( Calendar.DAY_OF_YEAR, -1 );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_YEAR_START:
          cal = Calendar.getInstance();
          cal.set( Calendar.DAY_OF_YEAR, cal.getActualMinimum( Calendar.DATE ) );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_THIS_YEAR_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.YEAR, 1 );
          cal.set( Calendar.DAY_OF_YEAR, cal.getActualMinimum( Calendar.DATE ) );
          cal.add( Calendar.DAY_OF_YEAR, -1 );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_YEAR_START:
          cal = Calendar.getInstance();
          cal.add( Calendar.YEAR, 1 );
          cal.set( Calendar.DAY_OF_YEAR, cal.getActualMinimum( Calendar.DATE ) );
          cal.set( Calendar.HOUR_OF_DAY, 0 );
          cal.set( Calendar.MINUTE, 0 );
          cal.set( Calendar.SECOND, 0 );
          cal.set( Calendar.MILLISECOND, 0 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_NEXT_YEAR_END:
          cal = Calendar.getInstance();
          cal.add( Calendar.YEAR, 2 );
          cal.set( Calendar.DAY_OF_YEAR, cal.getActualMinimum( Calendar.DATE ) );
          cal.add( Calendar.DAY_OF_YEAR, -1 );
          cal.set( Calendar.HOUR_OF_DAY, 23 );
          cal.set( Calendar.MINUTE, 59 );
          cal.set( Calendar.SECOND, 59 );
          cal.set( Calendar.MILLISECOND, 999 );
          row[ index ] = cal.getTime();
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_RESULT:
          Result previousResult = getPipeline().getPreviousResult();
          boolean result = false;
          if ( previousResult != null ) {
            result = previousResult.getResult();
          }
          row[ index ] = result;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_EXIT_STATUS:
          previousResult = getPipeline().getPreviousResult();
          long value = 0;
          if ( previousResult != null ) {
            value = previousResult.getExitStatus();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_ENTRY_NR:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getEntryNr();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_FILES:
          previousResult = getPipeline().getPreviousResult();
          value = 0;

          if ( previousResult != null ) {
            value = previousResult.getResultFiles().size();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_FILES_RETRIEVED:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrFilesRetrieved();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_DELETED:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesDeleted();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_INPUT:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesInput();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_OUTPUT:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesOutput();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_READ:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesRead();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_REJECTED:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesRejected();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_UPDATED:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesUpdated();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_WRITTEN:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrLinesWritten();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_ROWS:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getRows().size();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_IS_STOPPED:
          previousResult = getPipeline().getPreviousResult();
          boolean stop = false;
          if ( previousResult != null ) {
            stop = previousResult.isStopped();
          }
          row[ index ] = stop;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_ERRORS:
          previousResult = getPipeline().getPreviousResult();
          value = 0;
          if ( previousResult != null ) {
            value = previousResult.getNrErrors();
          }
          row[ index ] = value;
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_LOG_TEXT:
          previousResult = getPipeline().getPreviousResult();
          String errorReason = null;
          if ( previousResult != null ) {
            errorReason = previousResult.getLogText();
          }
          row[ index ] = errorReason;
          break;

        default:
          break;
      }
    }

    return row;
  }

  public boolean processRow() throws HopException {
    Object[] row;
    if ( data.readsRows ) {
      row = getRow();
      if ( row == null ) {
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      }

    } else {
      row = new Object[] {}; // empty row
      incrementLinesRead();

      if ( first ) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      }
    }

    IRowMeta imeta = getInputRowMeta();
    if ( imeta == null ) {
      imeta = new RowMeta();
      this.setInputRowMeta( imeta );
    }

    row = getSystemData( imeta, row );

    if ( log.isRowLevel() ) {
      logRowlevel( "System info returned: " + data.outputRowMeta.getString( row ) );
    }

    putRow( data.outputRowMeta, row );

    if ( !data.readsRows ) {
      // Just one row and then stop!
      setOutputDone();
      return false;
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
//      data.readsRows = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
      if ( previous != null && previous.size() > 0 ) {
        data.readsRows = true;
      }

      return true;
    }
    return false;
  }

  public void dispose() {
    super.dispose();
  }

}
