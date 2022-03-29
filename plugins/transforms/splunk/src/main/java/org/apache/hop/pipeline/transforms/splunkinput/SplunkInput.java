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

package org.apache.hop.pipeline.transforms.splunkinput;

import com.splunk.JobResultsArgs;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.splunk.SplunkConnection;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class SplunkInput extends BaseTransform<SplunkInputMeta, SplunkInputData> {

  public SplunkInput(
      TransformMeta stepMeta,
      SplunkInputMeta meta,
      SplunkInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(stepMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    // Is the transform getting input?
    //
    List<TransformMeta> transforms = getPipelineMeta().findPreviousTransforms(getTransformMeta());

    // Connect to Neo4j
    //
    if (StringUtils.isEmpty(meta.getConnectionName())) {
      log.logError("You need to specify a Splunk connection to use in this transform");
      return false;
    }

    String connectionName = resolve(meta.getConnectionName());

    try {
      // Load the metadata
      //
      IHopMetadataSerializer<SplunkConnection> serializer =
          metadataProvider.getSerializer(SplunkConnection.class);
      if (!serializer.exists(connectionName)) {
        throw new HopException(
            "The referenced Splunk connection with name '"
                + connectionName
                + "' does not exist in the metadata");
      }
      data.splunkConnection = serializer.load(connectionName);
    } catch (HopException e) {
      log.logError(
          "Could not load Splunk connection '" + meta.getConnectionName() + "' from the metastore",
          e);
      return false;
    }

    try {

      data.serviceArgs = data.splunkConnection.getServiceArgs(this);

      data.service = Service.connect(data.serviceArgs);

    } catch (Exception e) {
      log.logError(
          "Unable to get or create a connection to Splunk connection named '"
              + data.splunkConnection.getName()
              + "'",
          e);
      return false;
    }

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    if (first) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = new RowMeta();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);

      // Run a one shot search in blocking mode
      //
      JobResultsArgs args = new JobResultsArgs();
      args.setCount(0);
      args.setOutputMode(JobResultsArgs.OutputMode.XML);

      data.eventsStream = data.service.oneshotSearch(resolve(meta.getQuery()), args);
    }

    try {
      ResultsReaderXml resultsReader = new ResultsReaderXml(data.eventsStream);
      HashMap<String, String> event;
      while ((event = resultsReader.getNextEvent()) != null) {

        Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

        for (int i = 0; i < meta.getReturnValues().size(); i++) {
          ReturnValue returnValue = meta.getReturnValues().get(i);
          String value = event.get(returnValue.getSplunkName());
          outputRow[i] = value;
        }

        incrementLinesInput();
        putRow(data.outputRowMeta, outputRow);
      }

    } catch (Exception e) {
      throw new HopException("Error reading from Splunk events stream", e);
    } finally {
      try {
        data.eventsStream.close();
      } catch (IOException e) {
        throw new HopException("Unable to close events stream", e);
      }
    }

    // Nothing more
    //
    setOutputDone();
    return false;
  }
}
