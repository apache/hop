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

package org.apache.hop.beam.pipeline.handler;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/**
 * Sort rows on Beam: the rows are passed on as they are.
 *
 * <p>A Beam pipeline does not keep the order of its rows from one transform to the next, so a sort
 * would be lost right after it. The transforms that need sorted input on the Hop engine don't on
 * Beam: Merge join and Memory group by group their rows by key. A pipeline that sorts in front of
 * them for the Hop engine therefore runs unchanged on Beam, without the cost of a sort.
 *
 * <p>Only passing unique rows can't be done without the sort, so the converter refuses Sort rows
 * with that option, see {@link #isPassingUniqueRows(TransformMeta)}.
 */
public class BeamSortRowsTransformHandler extends BeamBaseTransformHandler
    implements IBeamPipelineTransformHandler {

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  /**
   * @return true when the Sort rows transform only passes unique rows, which needs the sort
   */
  public static boolean isPassingUniqueRows(TransformMeta transformMeta) throws HopException {
    Node transformNode =
        XmlHandler.getSubNode(
            XmlHandler.loadXmlString(transformMeta.getXml()), TransformMeta.XML_TAG);
    return "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "unique_rows"));
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    // The rows go on unchanged: the next transform reads them under this transform's name
    //
    transformCollectionMap.put(transformMeta.getName(), input);
    log.logBasic(
        "Handled Sort rows (TRANSFORM) : "
            + transformMeta.getName()
            + ", passes the rows on unsorted: Beam does not keep the order of rows between"
            + " transforms");
  }
}
