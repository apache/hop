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

package org.apache.hop.pipeline.transforms.mongodbinput;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;

@Transform(
    id = "MongoDbInput",
    image = "mongodb-input.svg",
    name = "i18n::MongoDbInput.Name",
    description = "i18n::MongoDbInput.Description",
    documentationUrl = "/pipeline/transforms/mongodbinput.html",
    keywords = "i18n::MongoDbInputMeta.keyword",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input")
@Getter
@Setter
public class MongoDbInputMeta extends MongoDbMeta<MongoDbInput, MongoDbInputData> {
  protected static final Class<?> PKG = MongoDbInputMeta.class;

  @HopMetadataProperty(key = "json_field_name", injectionKey = "JSON_OUTPUT_FIELD")
  private String jsonFieldName;

  @HopMetadataProperty(key = "fields_name", injectionKey = "JSON_FIELD")
  private String jsonField;

  @HopMetadataProperty(key = "json_query", injectionKey = "JSON_QUERY")
  private String jsonQuery;

  @HopMetadataProperty(key = "query_is_pipeline", injectionKey = "AGG_PIPELINE")
  private boolean aggPipeline = false;

  @HopMetadataProperty(key = "output_json", injectionKey = "OUTPUT_JSON")
  private boolean outputJson = true;

  @HopMetadataProperty(
      groupKey = "mongo_fields",
      key = "mongo_field",
      injectionKey = "MONGODB_FIELDS",
      injectionGroupKey = "MONGODB_FIELDS")
  private List<MongoField> fields;

  @HopMetadataProperty(key = "execute_for_each_row", injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executeForEachIncomingRow = false;

  @Override
  public Object clone() {
    MongoDbInputMeta meta = (MongoDbInputMeta) super.clone();
    return meta;
  }

  @Override
  public void setDefault() {
    jsonFieldName = "json";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    try {
      if (outputJson || Utils.isEmpty(fields)) {
        IValueMeta jsonValueMeta =
            ValueMetaFactory.createValueMeta(jsonFieldName, IValueMeta.TYPE_STRING);
        jsonValueMeta.setOrigin(origin);
        rowMeta.addValueMeta(jsonValueMeta);
      } else {
        for (MongoField f : fields) {
          int type = ValueMetaFactory.getIdForValueMeta(f.hopType);
          IValueMeta vm = ValueMetaFactory.createValueMeta(f.fieldName, type);
          vm.setOrigin(origin);
          if (f.indexedValues != null) {
            vm.setIndex(f.indexedValues.toArray()); // indexed values
          }
          rowMeta.addValueMeta(vm);
        }
      }
    } catch (Exception e) {
      throw new HopTransformException("Error processing output fields of transform", e);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    super.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);
  }
}
