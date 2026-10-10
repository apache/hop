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

package org.apache.hop.ai.transforms.extractgraph;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import org.apache.hop.ai.engine.AiChatModelFactory;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Reads entities and relationships out of a text with a chat model, one output row each. */
public class ExtractGraph extends BaseTransform<ExtractGraphMeta, ExtractGraphData> {

  private static final Class<?> PKG = ExtractGraphMeta.class;

  public ExtractGraph(
      TransformMeta transformMeta,
      ExtractGraphMeta meta,
      ExtractGraphData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (Utils.isEmpty(meta.getAiProvider())) {
      logError(BaseMessages.getString(PKG, "ExtractGraph.Validation.ProviderRequired"));
      return false;
    }
    data.entityTypes = ExtractGraphMeta.splitTypes(resolve(meta.getEntityTypes()));
    data.relationshipTypes = ExtractGraphMeta.splitTypes(resolve(meta.getRelationshipTypes()));
    return super.init();
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
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, null, this, getMetadataProvider());
      resolveInputField();
      openModel();
    }

    String text = data.inputRowMeta.getString(row, data.inputFieldIndex);
    if (Utils.isEmpty(text)) {
      // No text, no graph and no call to the model. This is not an error.
      passThroughWithoutResults(row);
      return true;
    }
    try {
      GraphExtraction graph =
          GraphExtraction.parse(ask(text), data.entityTypes, data.relationshipTypes);
      for (String dropped : graph.dropped()) {
        logBasic(BaseMessages.getString(PKG, "ExtractGraph.Log.Dropped", dropped));
      }
      if (graph.entities().isEmpty() && graph.relationships().isEmpty()) {
        passThroughWithoutResults(row);
        return true;
      }
      for (GraphExtraction.Entity entity : graph.entities()) {
        putRow(
            data.outputRowMeta,
            output(
                row,
                ExtractGraphMeta.KIND_ENTITY,
                entity.name(),
                entity.type(),
                entity.description(),
                null,
                null));
      }
      for (GraphExtraction.Relationship relationship : graph.relationships()) {
        putRow(
            data.outputRowMeta,
            output(
                row,
                ExtractGraphMeta.KIND_RELATIONSHIP,
                null,
                relationship.type(),
                relationship.description(),
                relationship.source(),
                relationship.target()));
      }
    } catch (HopException e) {
      if (!getTransformMeta().isDoingErrorHandling()) {
        throw e;
      }
      putError(data.inputRowMeta, row, 1, e.getMessage(), meta.getInputField(), "EXTRACTGRAPH001");
    }
    return true;
  }

  /**
   * A row that yields no graph produces no output, unless the option says to keep it: then it is
   * written once with the graph fields left empty.
   */
  private void passThroughWithoutResults(Object[] row) throws HopException {
    if (meta.isPassRowsWithoutResults()) {
      putRow(data.outputRowMeta, RowDataUtil.createResizedCopy(row, data.outputRowMeta.size()));
    }
  }

  private Object[] output(Object[] row, Object... values) {
    Object[] output = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    int index = data.inputRowMeta.size();
    for (Object value : values) {
      output[index++] = value;
    }
    return output;
  }

  private String ask(String text) throws HopException {
    ChatRequest.Builder request =
        ChatRequest.builder()
            .messages(SystemMessage.from(data.systemPrompt), UserMessage.from(text));
    if (data.responseFormat != null) {
      request.responseFormat(data.responseFormat);
    }
    try {
      return data.model.chat(request.build()).aiMessage().text();
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "ExtractGraph.Error.Calling"), e);
    }
  }

  private void resolveInputField() throws HopException {
    data.inputFieldIndex = data.inputRowMeta.indexOfValue(meta.getInputField());
    if (data.inputFieldIndex < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "ExtractGraph.Validation.InputFieldNotFound",
              String.valueOf(meta.getInputField())));
    }
  }

  private void openModel() throws HopException {
    data.model =
        AiChatModelFactory.createChatModel(
            resolve(meta.getAiProvider()),
            resolve(meta.getModelName()),
            this,
            getMetadataProvider());
    data.systemPrompt =
        GraphExtraction.systemPrompt(
            data.entityTypes, data.relationshipTypes, resolve(meta.getInstructions()));
    JsonSchema schema =
        GraphExtraction.schema(data.entityTypes, data.relationshipTypes, getTransformName());
    // As in Structured extract: hold the model to the schema where it can be held to it, and
    // otherwise rely on the prompt and the checks on the way back.
    data.responseFormat = AiChatModelFactory.responseFormatFor(data.model, schema);
    if (data.responseFormat == null) {
      logBasic(
          BaseMessages.getString(
              PKG, "ExtractGraph.Log.NoSchemaSupport", String.valueOf(meta.getModelName())));
    }
  }

  @Override
  public void dispose() {
    data.model = null;
    super.dispose();
  }
}
