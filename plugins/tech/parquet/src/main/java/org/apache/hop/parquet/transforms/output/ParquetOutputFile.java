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

import java.io.OutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class ParquetOutputFile implements OutputFile {

  private final OutputStream outputStream;

  public ParquetOutputFile(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new ParquetOutputStream(outputStream);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return new ParquetOutputStream(outputStream);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }
}
