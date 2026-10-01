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

package org.apache.hop.redis.transforms.redisinput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;

@Getter
@Setter
@Transform(
    id = "RedisInput",
    image = "redis-input.svg",
    name = "i18n::RedisInput.Name",
    description = "i18n::RedisInput.Description",
    documentationUrl = "/pipeline/transforms/redis-input.html",
    keywords = "i18n::RedisInputMeta.keyword",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input")
public class RedisInputMeta extends BaseTransformMeta<RedisInput, RedisInputData> {

  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.REDIS_CONNECTION)
  private String connectionName;

  /** Per-row Redis read mappings. */
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<RedisInputField> fields = new ArrayList<>();

  @Override
  public void setDefault() {
    connectionName = null;
    fields = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (fields == null) {
      return;
    }
    for (RedisInputField field : fields) {
      if (field == null || StringUtils.isEmpty(field.getValueField())) {
        continue;
      }
      String fieldName = variables.resolve(field.getValueField());
      RedisDataStructure structure = field.resolveDataStructure();
      // SET/LIST are always a JSON array string; BYTE codec yields binary for STRING/HASH.
      if (structure != RedisDataStructure.SET
          && structure != RedisDataStructure.LIST
          && field.getValueCodec() == RedisCodecType.BYTE) {
        ValueMetaBinary meta = new ValueMetaBinary(fieldName);
        meta.setOrigin(name);
        inputRowMeta.addValueMeta(meta);
      } else {
        ValueMetaString meta = new ValueMetaString(fieldName);
        meta.setOrigin(name);
        inputRowMeta.addValueMeta(meta);
      }
    }
  }
}
