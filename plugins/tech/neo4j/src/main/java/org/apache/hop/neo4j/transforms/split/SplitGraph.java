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

package org.apache.hop.neo4j.transforms.split;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphNodeData;
import org.apache.hop.neo4j.core.data.GraphRelationshipData;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class SplitGraph extends BaseTransform<SplitGraphMeta, SplitGraphData> {

  public SplitGraph(
      TransformMeta transformMeta,
      SplitGraphMeta meta,
      SplitGraphData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.graphFieldIndex = getInputRowMeta().indexOfValue(meta.getGraphField());
      if (data.graphFieldIndex < 0) {
        throw new HopException(
            "Unable to find graph field " + meta.getGraphField() + "' in the transform input");
      }
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.graphFieldIndex);
      if (valueMeta.getType() != ValueMetaGraph.TYPE_GRAPH) {
        throw new HopException("Please specify a Graph field to split");
      }

      data.typeField = null;
      if (StringUtils.isNotEmpty(meta.getTypeField())) {
        data.typeField = resolve(meta.getTypeField());
      }
      data.idField = null;
      if (StringUtils.isNotEmpty(meta.getIdField())) {
        data.idField = resolve(meta.getIdField());
      }
      data.propertySetField = null;
      if (StringUtils.isNotEmpty(meta.getPropertySetField())) {
        data.propertySetField = resolve(meta.getPropertySetField());
      }
    }

    ValueMetaGraph valueMeta =
        (ValueMetaGraph) getInputRowMeta().getValueMeta(data.graphFieldIndex);
    Object valueData = row[data.graphFieldIndex];
    GraphData graphData = valueMeta.getGraphData(valueData);

    for (GraphNodeData nodeData : graphData.getNodes()) {
      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = getInputRowMeta().size();
      GraphData copy = graphData.createEmptyCopy();
      copy.getNodes().add(nodeData.clone());

      outputRowData[data.graphFieldIndex] = copy;
      if (data.typeField != null) {
        outputRowData[index++] = "Node";
      }
      if (data.idField != null) {
        outputRowData[index++] = nodeData.getId();
      }
      if (data.propertySetField != null) {
        outputRowData[index++] = nodeData.getPropertySetId();
      }
      putRow(data.outputRowMeta, outputRowData);
    }

    for (GraphRelationshipData relationshipData : graphData.getRelationships()) {
      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = getInputRowMeta().size();
      GraphData copy = graphData.createEmptyCopy();
      copy.getRelationships().add(relationshipData.clone());

      outputRowData[data.graphFieldIndex] = copy;
      if (data.typeField != null) {
        outputRowData[index++] = "Relationship";
      }
      if (data.idField != null) {
        outputRowData[index++] = relationshipData.getId();
      }
      if (data.propertySetField != null) {
        outputRowData[index++] = relationshipData.getPropertySetId();
      }
      putRow(data.outputRowMeta, outputRowData);
    }

    return true;
  }
}
