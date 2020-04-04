/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core;

import org.apache.hop.core.logging.BaseLogTable;
import org.apache.hop.core.logging.ChannelLogTable;
import org.apache.hop.core.logging.JobEntryLogTable;
import org.apache.hop.core.logging.JobLogTable;
import org.apache.hop.core.logging.ILogTable;
import org.apache.hop.core.logging.MetricsLogTable;
import org.apache.hop.core.logging.PerformanceLogTable;
import org.apache.hop.core.logging.PipelineLogTable;
import org.apache.hop.core.logging.TransformLogTable;
import org.apache.hop.job.JobMeta;
import org.apache.hop.pipeline.PipelineMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class that filterers information, before exporting meta to xml.
 *
 * @author IvanNikolaychuk
 */
public class XmlExportHelper {

  /**
   * When exporting meta we should not export user global parameters.
   * Method makes clone for each table and deletes all global parameters.
   * We have to make clones of each table, because we don't want to change real tables content.
   *
   * @param pipelineMeta meta, that contains log tables to be refactored before export
   */
  public static void swapTables( PipelineMeta pipelineMeta ) {
    PipelineLogTable pipelineLogTable = pipelineMeta.getPipelineLogTable();
    if ( pipelineLogTable != null ) {
      PipelineLogTable clonePipelineLogTable = (PipelineLogTable) pipelineLogTable.clone();
      clonePipelineLogTable.setAllGlobalParametersToNull();
      pipelineMeta.setPipelineLogTable( clonePipelineLogTable );
    }

    TransformLogTable transformLogTable = pipelineMeta.getTransformLogTable();
    if ( transformLogTable != null ) {
      TransformLogTable cloneTransformLogTable = (TransformLogTable) transformLogTable.clone();
      cloneTransformLogTable.setAllGlobalParametersToNull();
      pipelineMeta.setTransformLogTable( cloneTransformLogTable );
    }

    PerformanceLogTable performanceLogTable = pipelineMeta.getPerformanceLogTable();
    if ( performanceLogTable != null ) {
      PerformanceLogTable clonePerformanceLogTable = (PerformanceLogTable) performanceLogTable.clone();
      clonePerformanceLogTable.setAllGlobalParametersToNull();
      pipelineMeta.setPerformanceLogTable( clonePerformanceLogTable );
    }

    ChannelLogTable channelLogTable = pipelineMeta.getChannelLogTable();
    if ( channelLogTable != null ) {
      ChannelLogTable cloneChannelLogTable = (ChannelLogTable) channelLogTable.clone();
      cloneChannelLogTable.setAllGlobalParametersToNull();
      pipelineMeta.setChannelLogTable( cloneChannelLogTable );
    }

    MetricsLogTable metricsLogTable = pipelineMeta.getMetricsLogTable();
    if ( metricsLogTable != null ) {
      MetricsLogTable cloneMetricsLogTable = (MetricsLogTable) metricsLogTable.clone();
      cloneMetricsLogTable.setAllGlobalParametersToNull();
      pipelineMeta.setMetricsLogTable( cloneMetricsLogTable );
    }
  }

  /**
   * @param jobMeta contains log tables to be refactored before export
   */
  public static void swapTables( JobMeta jobMeta ) {
    JobLogTable jobLogTable = jobMeta.getJobLogTable();
    if ( jobLogTable != null ) {
      JobLogTable cloneJobLogTable = (JobLogTable) jobLogTable.clone();
      cloneJobLogTable.setAllGlobalParametersToNull();
      jobMeta.setJobLogTable( cloneJobLogTable );
    }

    JobEntryLogTable jobEntryLogTable = jobMeta.getJobEntryLogTable();
    if ( jobEntryLogTable != null ) {
      JobEntryLogTable cloneEntryLogTable = (JobEntryLogTable) jobEntryLogTable.clone();
      cloneEntryLogTable.setAllGlobalParametersToNull();
      jobMeta.setJobEntryLogTable( cloneEntryLogTable );
    }

    ChannelLogTable channelLogTable = jobMeta.getChannelLogTable();
    if ( channelLogTable != null ) {
      ChannelLogTable cloneChannelLogTable = (ChannelLogTable) channelLogTable.clone();
      cloneChannelLogTable.setAllGlobalParametersToNull();
      jobMeta.setChannelLogTable( cloneChannelLogTable );
    }

    List<ILogTable> extraLogTables = jobMeta.getExtraLogTables();
    if ( extraLogTables != null ) {
      List<ILogTable> cloneExtraLogTables = new ArrayList<>();
      for ( ILogTable logTable : extraLogTables ) {
        if ( logTable instanceof BaseLogTable ) {
          if ( logTable instanceof Cloneable ) {
            BaseLogTable cloneExtraLogTable = (BaseLogTable) logTable.clone();
            cloneExtraLogTable.setAllGlobalParametersToNull();
            cloneExtraLogTables.add( (ILogTable) cloneExtraLogTable );
          }
        }
      }
      jobMeta.setExtraLogTables( cloneExtraLogTables );
    }
  }
}
