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

package org.apache.hop.avro.transforms.avrodecode;

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class AvroDecodeData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;
  public int inputIndex;
  public String sourceFieldName;
  public ValueMetaAvroRecord avroValueMeta;

  /** True when the source field is Binary and a schema file is set. */
  public boolean decodingBinary;

  public IValueMeta sourceValueMeta;

  /**
   * Parsed once per transform copy. Also the writer schema: no separate reader schema is applied.
   */
  public Schema schema;

  /** Reused for every binary row of this copy. Not thread-safe, and each copy has its own. */
  public GenericDatumReader<GenericRecord> reader;

  /** Reused for every binary row of this copy via {@code DecoderFactory.binaryDecoder}. */
  public BinaryDecoder decoder;

  /** Reused as the target of {@code GenericDatumReader.read}. Cleared after a failed read. */
  public GenericRecord reusedRecord;

  /** Per target field, built on the first row: the resolved Avro field name. */
  public String[] avroFieldNames;

  /** Per target field, built on the first row: the value metadata to convert to. */
  public IValueMeta[] targetValueMetas;

  /** Per target field: the value metadata of the Hop type the value arrives as, by type. */
  public List<Map<Integer, IValueMeta>> standardValueMetas;

  public AvroDecodeData() {
    // Do nothing
  }
}
