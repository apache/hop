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

  public String getStreamField() {
    return streamField;
  }

  public void setStreamField(String streamField) {
    this.streamField = streamField;
  }

  public RedisDataStructure getDataStructure() {
    return dataStructure;
  }

  public void setDataStructure(RedisDataStructure dataStructure) {
    this.dataStructure = dataStructure;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public RedisCodecType getKeyCodec() {
    return keyCodec;
  }

  public void setKeyCodec(RedisCodecType keyCodec) {
    this.keyCodec = keyCodec;
  }

  public String getHashKey() {
    return hashKey;
  }

  public void setHashKey(String hashKey) {
    this.hashKey = hashKey;
  }

  public RedisCodecType getHashKeyCodec() {
    return hashKeyCodec;
  }

  public void setHashKeyCodec(RedisCodecType hashKeyCodec) {
    this.hashKeyCodec = hashKeyCodec;
  }

  public RedisCodecType getValueCodec() {
    return valueCodec;
  }

  public void setValueCodec(RedisCodecType valueCodec) {
    this.valueCodec = valueCodec;
  }

  public String getTtlSeconds() {
    return ttlSeconds;
  }

  public void setTtlSeconds(String ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  public String getRedisName() {
    return redisName;
  }

  public void setRedisName(String redisName) {
    this.redisName = redisName;
  }
}
