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

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;

/**
 * One mapping row for Redis Input.
 *
 * <p>Supports STRING ({@code GET}), HASH ({@code HGET} only), SET ({@code SMEMBERS} as JSON array),
 * and LIST ({@code LRANGE} as JSON array). HGETALL and SISMEMBER are not supported.
 */
@Getter
@Setter
public class RedisInputField implements Cloneable {

  /** Redis key: stream field name or literal / variable text. */
  @HopMetadataProperty(key = "redis_key")
  private String redisKey;

  @HopMetadataProperty(key = "redis_key_codec")
  private RedisCodecType redisKeyCodec = RedisCodecType.STRING;

  @HopMetadataProperty(key = "data_structure")
  private RedisDataStructure dataStructure = RedisDataStructure.STRING;

  /** Hash field: stream field name or literal. Required for HASH. */
  @HopMetadataProperty(key = "hash_field")
  private String hashField;

  @HopMetadataProperty(key = "hash_field_codec")
  private RedisCodecType hashFieldCodec;

  /** Output field name for the decoded value (or JSON array for SET/LIST). */
  @HopMetadataProperty(key = "value_field")
  private String valueField;

  @HopMetadataProperty(key = "value_codec")
  private RedisCodecType valueCodec = RedisCodecType.STRING;

  @HopMetadataProperty(key = "list_start")
  private String listStart = "0";

  @HopMetadataProperty(key = "list_stop")
  private String listStop = "-1";

  public RedisInputField() {}

  public RedisInputField(RedisInputField other) {
    this.redisKey = other.redisKey;
    this.redisKeyCodec = other.redisKeyCodec;
    this.dataStructure = other.dataStructure;
    this.hashField = other.hashField;
    this.hashFieldCodec = other.hashFieldCodec;
    this.valueField = other.valueField;
    this.valueCodec = other.valueCodec;
    this.listStart = other.listStart;
    this.listStop = other.listStop;
  }

  @Override
  public RedisInputField clone() {
    return new RedisInputField(this);
  }

  public RedisDataStructure resolveDataStructure() {
    return dataStructure == null ? RedisDataStructure.STRING : dataStructure;
  }

  public String resolveListStart() {
    return StringUtils.isEmpty(listStart) ? "0" : listStart;
  }

  public String resolveListStop() {
    return StringUtils.isEmpty(listStop) ? "-1" : listStop;
  }
}
