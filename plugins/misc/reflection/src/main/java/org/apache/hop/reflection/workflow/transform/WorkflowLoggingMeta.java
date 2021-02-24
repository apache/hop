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
 *
 */

package org.apache.hop.reflection.workflow.transform;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
  id = "WorkflowLogging",
  name = "i18n::WorkflowLogging.Transform.Name",
  description = "i18n::WorkflowLogging.Transform.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
  image = "ui/images/show-log.svg",
  keywords = "audit,log,metrics")
public class WorkflowLoggingMeta extends BaseTransformMeta
    implements ITransformMeta<WorkflowLogging, WorkflowLoggingData> {

  private boolean loggingActionResults;

  public WorkflowLoggingMeta() {
    super();
  }

  @Override
  public void setDefault() {
    loggingActionResults = true;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    inputRowMeta.clear();

    // Logging date
    inputRowMeta.addValueMeta(new ValueMetaDate("loggingDate"));

    // Logging date
    inputRowMeta.addValueMeta(new ValueMetaString("loggingPhase"));

    // Name of the workflow
    inputRowMeta.addValueMeta(new ValueMetaString("workflowName", 255, -1));

    // Filename of the workflow
    inputRowMeta.addValueMeta(new ValueMetaString("workflowFilename", 255, -1));

    // Start date of the workflow
    inputRowMeta.addValueMeta(new ValueMetaDate("workflowStart"));

    // End date of the workflow
    inputRowMeta.addValueMeta(new ValueMetaDate("workflowEnd"));

    // Workflow log channel ID
    inputRowMeta.addValueMeta(new ValueMetaString("workflowLogChannelId", 32, -1));

    // Pipeline log channel ID
    inputRowMeta.addValueMeta(new ValueMetaString("workflowLogChannelId", 32, -1));

    // Logging text of the workflow
    inputRowMeta.addValueMeta(new ValueMetaString("workflowLogging", 1000000, -1));

    // Number of errors
    inputRowMeta.addValueMeta(new ValueMetaInteger("workflowErrorCount", 3, 0));

    // Workflow status description
    inputRowMeta.addValueMeta(new ValueMetaString("workflowStatusDescription", 32, -1));

    if ( loggingActionResults ) {
      // Name of the action
      inputRowMeta.addValueMeta(new ValueMetaString("actionName"));

      // Copy number of the action
      inputRowMeta.addValueMeta(new ValueMetaInteger("actionNr"));

      // Copy number of the action
      inputRowMeta.addValueMeta(new ValueMetaBoolean("actionResult"));

      // action log channel ID
      inputRowMeta.addValueMeta(new ValueMetaString("actionLogChannelId", 32, -1));

      // action logging text
      inputRowMeta.addValueMeta(new ValueMetaString("actionLoggingText", 1000000, -1));

      // Number of errors
      inputRowMeta.addValueMeta(new ValueMetaInteger("actionErrors", 3, 0));

      // Logging date
      inputRowMeta.addValueMeta(new ValueMetaDate("actionLogDate"));

      // Execution duration in ms
      inputRowMeta.addValueMeta(new ValueMetaInteger("actionDuration", 12, 0));

      // Exit status
      inputRowMeta.addValueMeta(new ValueMetaInteger("actionExitStatus", 3, 0));

      // Nr of files retrieved
      inputRowMeta.addValueMeta(new ValueMetaInteger("actionNrFilesRetrieved", 10, 0));

      // Action filename
      inputRowMeta.addValueMeta(new ValueMetaString("actionFilename", 255, -1));

      // Action comment
      inputRowMeta.addValueMeta(new ValueMetaString("actionComment", 255, -1));

      // Action reason
      inputRowMeta.addValueMeta(new ValueMetaString("actionReason", 255, -1));

    }
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( "log_transforms", loggingActionResults ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {

    loggingActionResults ="Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "log_transforms" ) );

  }

  @Override
  public WorkflowLogging createTransform(
      TransformMeta transformMeta,
      WorkflowLoggingData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new WorkflowLogging(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public WorkflowLoggingData getTransformData() {
    return new WorkflowLoggingData();
  }

  /**
   * Gets loggingTransforms
   *
   * @return value of loggingTransforms
   */
  public boolean isLoggingActionResults() {
    return loggingActionResults;
  }

  /** @param loggingActionResults The loggingTransforms to set */
  public void setLoggingActionResults( boolean loggingActionResults ) {
    this.loggingActionResults = loggingActionResults;
  }
}
