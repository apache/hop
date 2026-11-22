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

package org.apache.hop.pipeline.transforms.recordsfromstream;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rowsfromresult.RowsFromResultMeta;

/**
 * @deprecated Use RowsFromResultMeta
 */
@Deprecated(since = "2.10")
@Transform(
    id = "RecordsFromStream",
    image = "recordsfromstream.svg",
    name = "i18n::RecordsFromStream.Name",
    description = "i18n::RecordsFromStream.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
    keywords = "i18n::RecordsFromStreamMeta.keyword",
    documentationUrl = "/pipeline/transforms/getrecordsfromstream.html")
public class RecordsFromStreamMeta extends RowsFromResultMeta {

  public RecordsFromStreamMeta() {
    super();
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ITransformData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new RecordsFromStream(
        transformMeta, this, (RecordsFromStreamData) data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public ITransformData createTransformData() {
    return new RecordsFromStreamData();
  }
}
