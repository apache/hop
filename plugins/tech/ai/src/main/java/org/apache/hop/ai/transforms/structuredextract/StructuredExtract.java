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
package org.apache.hop.ai.transforms.structuredextract;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ai.engine.AiChatModelFactory;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Pulls named, typed fields out of a text field using a language model. */
public class StructuredExtract extends BaseTransform<StructuredExtractMeta, StructuredExtractData> {

  private static final Class<?> PKG = StructuredExtractMeta.class;

  public StructuredExtract(
      TransformMeta transformMeta,
      StructuredExtractMeta meta,
      StructuredExtractData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (Utils.isEmpty(meta.getAiProvider())) {
      logError(BaseMessages.getString(PKG, "StructuredExtract.Validation.ProviderRequired"));
      return false;
    }
    data.fields = usableFields();
    if (data.fields.isEmpty()) {
      logError(BaseMessages.getString(PKG, "StructuredExtract.Validation.NoFields"));
      return false;
    }
    return super.init();
  }

  private List<StructuredExtractField> usableFields() {
    List<StructuredExtractField> usable = new ArrayList<>();
    for (StructuredExtractField field : meta.getFields()) {
      if (field != null && !field.trimmedName().isEmpty()) {
        usable.add(field);
      }
    }
    return usable;
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

    try {
      putRow(data.outputRowMeta, extract(row));
    } catch (HopException e) {
      if (!getTransformMeta().isDoingErrorHandling()) {
        throw e;
      }
      putError(
          data.inputRowMeta, row, 1, e.getMessage(), meta.getInputField(), "STRUCTUREDEXTRACT001");
    }
    return true;
  }

  private Object[] extract(Object[] row) throws HopException {
    String text = data.inputRowMeta.getString(row, data.inputFieldIndex);
    Object[] output = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    if (Utils.isEmpty(text)) {
      // Nothing to read fields from. The row keeps its place with the new fields left empty,
      // rather than being dropped or sent to the error hop: an empty input is not a failure.
      return output;
    }

    String answer = ask(text);
    Object[] values = ExtractionParser.parse(answer, data.fields);
    int index = data.inputRowMeta.size();
    for (Object value : values) {
      output[index++] = value;
    }
    return output;
  }

  private String ask(String text) throws HopException {
    ChatRequest request = buildRequest(data.systemPrompt, text, data.responseFormat);
    try {
      return data.model.chat(request).aiMessage().text();
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "StructuredExtract.Error.Calling"), e);
    }
  }

  static ChatRequest buildRequest(String systemPrompt, String text, ResponseFormat responseFormat) {
    ChatRequest.Builder request =
        ChatRequest.builder().messages(SystemMessage.from(systemPrompt), UserMessage.from(text));
    if (responseFormat != null) {
      request.responseFormat(responseFormat);
    }
    return request.build();
  }

  /**
   * The instruction sent with every row.
   *
   * <p>The schema is described here even when the model is constrained by a real one: the field
   * descriptions are the user's own words about what each field means, and a model that can see
   * them extracts noticeably better than one working from field names alone.
   */
  static String buildSystemPrompt(String schemaDescription, String instructions) {
    StringBuilder prompt = new StringBuilder();
    prompt
        .append("Read the fields below out of the text the user sends. ")
        .append("Answer with a single JSON object and nothing else. ")
        .append("Use null for anything the text does not say. Do not invent values.\n\n")
        .append(schemaDescription);
    if (!Utils.isEmpty(instructions)) {
      prompt.append("\n\n").append(instructions);
    }
    return prompt.toString();
  }

  private void resolveInputField() throws HopException {
    data.inputFieldIndex = data.inputRowMeta.indexOfValue(meta.getInputField());
    if (data.inputFieldIndex < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "StructuredExtract.Validation.InputFieldNotFound",
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

    JsonSchema schema = ExtractionSchema.build(data.fields, getTransformName());
    data.schemaDescription = SchemaPrompt.describe(data.fields);
    data.systemPrompt = buildSystemPrompt(data.schemaDescription, resolve(meta.getInstructions()));

    // Constrain the model where it can be constrained. Where it cannot, the schema still goes in
    // the prompt and the answer is checked on the way back, which is the best available.
    data.responseFormat = responseFormatFor(data.model, schema);
    if (data.responseFormat == null) {
      logBasic(
          BaseMessages.getString(
              PKG, "StructuredExtract.Log.NoSchemaSupport", String.valueOf(meta.getModelName())));
    }
  }

  /** The schema as a response format, or null when the model does not accept one. */
  static ResponseFormat responseFormatFor(ChatModel model, JsonSchema schema) {
    return model.supportedCapabilities().contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA)
        ? ResponseFormat.builder().type(ResponseFormatType.JSON).jsonSchema(schema).build()
        : null;
  }

  @Override
  public void dispose() {
    data.model = null;
    super.dispose();
  }
}
