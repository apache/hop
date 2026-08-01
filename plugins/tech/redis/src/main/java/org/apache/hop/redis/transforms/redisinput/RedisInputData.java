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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.redis.client.RedisClientSession;
import org.apache.hop.redis.codec.RedisValueCodec;
import org.apache.hop.redis.transforms.RedisDataStructure;

@Getter
@Setter
public class RedisInputData extends BaseTransformData implements ITransformData {
  private IRowMeta outputRowMeta;
  private RedisClientSession session;
  private Mapping[] mappings;

  /** Index in the output row where the first mapping value is written. */
  private int firstValueIndex;

  @Getter
  @Setter
  public static final class Mapping {
    private RedisDataStructure structure;

    /** Index of Redis-key stream field, or -1 when {@link #keyLiteral} is used. */
    private int keyFieldIndex = -1;

    private String keyLiteral;
    private RedisValueCodec keyCodec;

    /** Index of hash-field stream field, or -1 when literal / N/A. */
    private int hashFieldIndex = -1;

    private String hashFieldLiteral;
    private RedisValueCodec hashFieldCodec;
    private RedisValueCodec valueCodec;
    private int valueOutputIndex;
    private long listStart;
    private long listStop;
  }
}
