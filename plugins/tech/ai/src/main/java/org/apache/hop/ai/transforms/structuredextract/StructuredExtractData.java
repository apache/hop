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

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ResponseFormat;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class StructuredExtractData extends BaseTransformData implements ITransformData {

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  public ChatModel model;

  /**
   * The schema the model is constrained by, when it supports being constrained. Null when the model
   * does not advertise RESPONSE_FORMAT_JSON_SCHEMA, in which case the schema is described in the
   * prompt instead and the answer is checked on the way back.
   */
  public ResponseFormat responseFormat;

  /** The schema rendered as text, for the prompt. Used whether or not the model is constrained. */
  public String schemaDescription;

  /**
   * The instruction sent with every row, built once. It is the same for the whole run, and the
   * extra instructions are variable resolved here rather than per row, matching how the other
   * options are resolved.
   */
  public String systemPrompt;

  /** Fields with a usable name, in output order. The grid can hold blank rows; these cannot. */
  public List<StructuredExtractField> fields;

  public int inputFieldIndex = -1;

  public StructuredExtractData() {
    super();
  }
}
