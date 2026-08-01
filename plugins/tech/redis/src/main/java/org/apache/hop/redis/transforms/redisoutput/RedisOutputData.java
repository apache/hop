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

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.redis.client.RedisClientSession;
import org.apache.hop.redis.codec.RedisCodecs;
import org.apache.hop.redis.codec.RedisValueCodec;
import org.apache.hop.redis.transforms.RedisDataStructure;

public class RedisOutputData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;
  public RedisClientSession session;
  public RedisCodecs codecs;
  public int keyFieldIndex = -1;
  public int valueFieldIndex = -1;
  public int hashKeyFieldIndex = -1;
  public int hashValueFieldIndex = -1;
  public Long ttlSeconds;

  /** Prepared STREAM_FIELDS mappings. */
  public StreamMapping[] streamMappings;

  public static final class StreamMapping {
    public RedisDataStructure structure;
    public int streamFieldIndex;

    /** Index of key stream field, or -1 when {@link #keyLiteral} is used. */
    public int keyFieldIndex = -1;

    public String keyLiteral;
    public RedisValueCodec keyCodec;

    /** Index of hash-key stream field, or -1 when literal / N/A. */
    public int hashKeyFieldIndex = -1;

    public String hashKeyLiteral;
    public RedisValueCodec hashKeyCodec;
    public RedisValueCodec valueCodec;

    /** Per-row TTL; null means no expire. */
    public Long ttlSeconds;
  }
}
