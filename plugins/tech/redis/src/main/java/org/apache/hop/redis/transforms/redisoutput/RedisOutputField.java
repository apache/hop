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

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;

/**
 * One mapping row for {@link RedisOutputWriteMode#STREAM_FIELDS}.
 *
 * <p>Required: stream field, key, key codec, value codec (defaults STRING). Hash key / hash key
 * codec apply only when data structure is HASH.
 */
@Getter
@Setter
public class RedisOutputField implements Cloneable {

  @HopMetadataProperty(key = "stream_field")
  private String streamField;

  @HopMetadataProperty(key = "data_structure")
  private RedisDataStructure dataStructure = RedisDataStructure.STRING;

  /** Redis key: stream field name or literal / variable text. */
  @HopMetadataProperty(key = "key")
  private String key;

  @HopMetadataProperty(key = "key_codec")
  private RedisCodecType keyCodec = RedisCodecType.STRING;

  /** Hash field name: stream field name or literal. Ignored unless structure is HASH. */
  @HopMetadataProperty(key = "hash_key")
  private String hashKey;

  @HopMetadataProperty(key = "hash_key_codec")
  private RedisCodecType hashKeyCodec;

  @HopMetadataProperty(key = "value_codec")
  private RedisCodecType valueCodec = RedisCodecType.STRING;

  /**
   * Optional TTL in seconds (static or variable). Values greater than 0 set expiry; {@code 0} or
   * empty means no expire.
   */
  @HopMetadataProperty(key = "ttl_seconds")
  private String ttlSeconds = "0";

  /**
   * Legacy column from earlier STREAM_FIELDS design. Still read when {@link #key} is empty so old
   * pipelines keep working.
   */
  @HopMetadataProperty(key = "redis_name")
  private String redisName;

  public RedisOutputField() {}

  public RedisOutputField(RedisOutputField other) {
    this.streamField = other.streamField;
    this.dataStructure = other.dataStructure;
    this.key = other.key;
    this.keyCodec = other.keyCodec;
    this.hashKey = other.hashKey;
    this.hashKeyCodec = other.hashKeyCodec;
    this.valueCodec = other.valueCodec;
    this.ttlSeconds = other.ttlSeconds;
    this.redisName = other.redisName;
  }

  @Override
  public RedisOutputField clone() {
    return new RedisOutputField(this);
  }

  public RedisDataStructure resolveDataStructure() {
    return dataStructure == null ? RedisDataStructure.STRING : dataStructure;
  }

  /** Resolved Redis key expression (field name or literal). */
  public String resolveKey() {
    if (StringUtils.isNotEmpty(key)) {
      return key;
    }
    if (StringUtils.isNotEmpty(redisName)) {
      return redisName;
    }
    return streamField;
  }

  /** Resolved hash field name (literal / field name). Empty when not set. */
  public String resolveHashKey() {
    return hashKey;
  }
}
