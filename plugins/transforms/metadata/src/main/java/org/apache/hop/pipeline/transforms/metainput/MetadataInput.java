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

package org.apache.hop.pipeline.transforms.metainput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class MetadataInput extends BaseTransform<MetadataInputMeta, MetadataInputData> {
  public MetadataInput(
      TransformMeta transformMeta,
      MetadataInputMeta meta,
      MetadataInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields(outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    // Get the list of type key filters...
    //
    List<String> typeKeyFilters = new ArrayList<>();
    for (String typeKeyFilter : meta.getTypeKeyFilters()) {
      typeKeyFilters.add(StringUtils.trim(resolve(typeKeyFilter)));
    }

    // Loop over the metadata classes (types)
    List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
    for (Class<IHopMetadata> metadataClass : metadataClasses) {
      IHopMetadataSerializer<IHopMetadata> serializer =
          metadataProvider.getSerializer(metadataClass);
      HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);

      // See if we need to include this class in the output...
      //
      boolean include = typeKeyFilters.isEmpty();
      for (String typeKeyFilter : typeKeyFilters) {
        if (typeKeyFilter.equals(annotation.key())) {
          include = true;
        }
      }
      if (!include) {
        // Skip this one
        continue;
      }

      JsonMetadataParser<IHopMetadata> parser =
          new JsonMetadataParser<>(metadataClass, metadataProvider);

      // Loop over the objects of the specified class
      //
      for (String name : serializer.listObjectNames()) {
        IHopMetadata metadata = serializer.load(name);

        // Output the metadata...
        //
        Object[] outputRow = RowDataUtil.allocateRowData(outputRowMeta.size());
        int index = 0;

        // The metadata provider
        // Where is this metadata stored?
        //
        if (StringUtils.isNotEmpty(meta.getProviderFieldName())) {
          outputRow[index++] = metadata.getMetadataProviderName();
        }

        // Some information about the metadata class
        //
        if (StringUtils.isNotEmpty(meta.getTypeKeyFieldName())) {
          outputRow[index++] = annotation.key();
        }
        if (StringUtils.isNotEmpty(meta.getTypeDescriptionFieldName())) {
          outputRow[index++] = annotation.name();
        }
        if (StringUtils.isNotEmpty(meta.getTypeDescriptionFieldName())) {
          outputRow[index++] = annotation.description();
        }
        if (StringUtils.isNotEmpty(meta.getTypeClassFieldName())) {
          outputRow[index++] = metadataClass.getName();
        }
        // Some information about the metadata object...
        if (StringUtils.isNotEmpty(meta.getNameFieldName())) {
          outputRow[index++] = metadata.getName();
        }

        // A JSON representation of this metadata...
        //
        if (StringUtils.isNotEmpty(meta.getJsonFieldName())) {
          JSONObject jsonObject = parser.getJsonObject(metadata);
          outputRow[index] = jsonObject.toJSONString();
        }

        putRow(outputRowMeta, outputRow);
      }
    }

    setOutputDone();
    return false;
  }
}
