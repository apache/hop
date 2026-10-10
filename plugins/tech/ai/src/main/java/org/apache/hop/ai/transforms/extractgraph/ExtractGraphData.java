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

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ResponseFormat;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class ExtractGraphData extends BaseTransformData implements ITransformData {
  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  public int inputFieldIndex = -1;

  public ChatModel model;
  public String systemPrompt;

  /** Null when the model can't be held to a JSON schema: the prompt then carries it alone. */
  public ResponseFormat responseFormat;

  public List<String> entityTypes;
  public List<String> relationshipTypes;
}
