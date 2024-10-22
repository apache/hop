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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class LanguageModelChatData extends BaseTransformData implements ITransformData {
  public int indexOfInputField;
  public int indexOfIdentifier;
  public int indexOfOutputFieldNamePrefix;
  public int indexOfModelType;
  public int indexOfModelName;
  public int indexOfFinishReason;
  public int indexOfInputTokenCount;
  public int indexOfOutputTokenCount;
  public int indexOfTotalTokenCount;
  public int indexOfInferenceTime;
  public int indexOfOutput;

  public IRowMeta previousRowMeta;
  public IRowMeta outputRowMeta;
  public int nrPrevFields;

  public LanguageModelChatData() {
    super();
    indexOfInputField = -1;
    indexOfIdentifier = -1;
    indexOfOutputFieldNamePrefix = -1;
    indexOfModelType = -1;
    indexOfModelName = -1;
    indexOfFinishReason = -1;
    indexOfInputTokenCount = -1;
    indexOfOutputTokenCount = -1;
    indexOfTotalTokenCount = -1;
    indexOfInferenceTime = -1;
    indexOfOutput = -1;
  }
}
