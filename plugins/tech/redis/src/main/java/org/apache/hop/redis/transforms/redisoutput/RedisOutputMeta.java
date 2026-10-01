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

package org.apache.hop.redis.transforms.redisoutput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.RedisListPushDirection;

@Getter
@Setter
@Transform(
    id = "RedisOutput",
    image = "redis-output.svg",
    name = "i18n::RedisOutput.Name",
    description = "i18n::RedisOutput.Description",
    documentationUrl = "/pipeline/transforms/redis-output.html",
    keywords = "i18n::RedisOutputMeta.keyword",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output")
public class RedisOutputMeta extends BaseTransformMeta<RedisOutput, RedisOutputData> {

  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.REDIS_CONNECTION)
  private String connectionName;

  @HopMetadataProperty(key = "write_mode")
  private RedisOutputWriteMode writeMode = RedisOutputWriteMode.KEY_VALUE;

  @HopMetadataProperty(key = "data_structure")
  private RedisDataStructure dataStructure = RedisDataStructure.STRING;

  @HopMetadataProperty(key = "key_codec")
  private RedisCodecType keyCodec = RedisCodecType.STRING;

  @HopMetadataProperty(key = "value_codec")
  private RedisCodecType valueCodec = RedisCodecType.STRING;

  @HopMetadataProperty(key = "hash_key_codec")
  private RedisCodecType hashKeyCodec = RedisCodecType.STRING;

  @HopMetadataProperty(key = "hash_value_codec")
  private RedisCodecType hashValueCodec = RedisCodecType.STRING;

  @HopMetadataProperty(key = "key_field")
  private String keyField;

  @HopMetadataProperty(key = "value_field")
  private String valueField;

  @HopMetadataProperty(key = "hash_key_field")
  private String hashKeyField;

  @HopMetadataProperty(key = "hash_value_field")
  private String hashValueField;

  /** Optional TTL in seconds (static or variable). Empty means no expire. */
  @HopMetadataProperty(key = "ttl_seconds")
  private String ttlSeconds;

  @HopMetadataProperty(key = "list_push_direction")
  private RedisListPushDirection listPushDirection = RedisListPushDirection.RPUSH;

  /** Fields used when {@link #writeMode} is {@link RedisOutputWriteMode#STREAM_FIELDS}. */
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<RedisOutputField> fields = new ArrayList<>();

  @Override
  public void setDefault() {
    connectionName = null;
    writeMode = RedisOutputWriteMode.KEY_VALUE;
    dataStructure = RedisDataStructure.STRING;
    keyCodec = RedisCodecType.STRING;
    valueCodec = RedisCodecType.STRING;
    hashKeyCodec = RedisCodecType.STRING;
    hashValueCodec = RedisCodecType.STRING;
    keyField = "";
    valueField = "";
    hashKeyField = "";
    hashValueField = "";
    ttlSeconds = "";
    listPushDirection = RedisListPushDirection.RPUSH;
    fields = new ArrayList<>();
  }

  /** Null-safe setter kept explicitly so empty metadata never leaves {@code fields} null. */
  public void setFields(List<RedisOutputField> fields) {
    this.fields = fields == null ? new ArrayList<>() : fields;
  }
}
