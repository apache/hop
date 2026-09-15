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
package org.apache.hop.parquet.transforms.output;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * An incoming field to partition the output by. The field's value becomes a {@code name=value}
 * directory level in the Hive-style layout, and the field itself is not written into the Parquet
 * files.
 */
@Getter
@Setter
public class ParquetPartitionField {
  @HopMetadataProperty(key = "name")
  private String name;

  public ParquetPartitionField() {}

  public ParquetPartitionField(String name) {
    this.name = name;
  }

  public ParquetPartitionField(ParquetPartitionField f) {
    this(f.name);
  }
}
