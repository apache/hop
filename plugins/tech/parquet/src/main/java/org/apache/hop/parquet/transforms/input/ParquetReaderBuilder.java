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

package org.apache.hop.parquet.transforms.input;

import java.util.Objects;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

public class ParquetReaderBuilder<T> extends ParquetReader.Builder<T> {
  private final ReadSupport<T> readSupport;

  public ParquetReaderBuilder(ReadSupport<T> readSupport, InputFile inputFile) {
    super(inputFile);
    this.readSupport = Objects.requireNonNull(readSupport, "readSupport cannot be null");
  }

  /**
   * Gets readSupport
   *
   * @return value of readSupport
   */
  @Override
  public ReadSupport<T> getReadSupport() {
    return readSupport;
  }
}
