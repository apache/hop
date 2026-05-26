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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.apache.hop.parquet.transforms.input.ParquetInputMeta;
import org.apache.hop.parquet.transforms.input.ParquetReadSupport;
import org.apache.hop.parquet.transforms.input.ParquetReaderBuilder;
import org.apache.hop.parquet.transforms.input.ParquetStream;
import org.apache.parquet.hadoop.ParquetReader;

/** Test helper to read and validate Parquet files. */
final class ParquetTestUtil {

  private ParquetTestUtil() {}

  static String resourceFilePath(Class<?> clazz, String resourceName) throws URISyntaxException {
    return Path.of(
            Objects.requireNonNull(
                    clazz.getResource("/" + resourceName), "Missing test resource: " + resourceName)
                .toURI())
        .toAbsolutePath()
        .toString();
  }

  static List<ParquetField> fieldsFromRowMeta(IRowMeta rowMeta) {
    List<ParquetField> fields = new ArrayList<>();
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      fields.add(
          new ParquetField(
              valueMeta.getName(),
              valueMeta.getName(),
              valueMeta.getTypeDesc(),
              valueMeta.getFormatMask(),
              Integer.toString(valueMeta.getLength()),
              Integer.toString(valueMeta.getPrecision())));
    }
    return fields;
  }

  static IRowMeta readSchema(String filename) throws HopException {
    return readSchema(new Variables(), filename);
  }

  static IRowMeta readSchema(IVariables variables, String filename) throws HopException {
    return ParquetInputMeta.extractRowMeta(variables, filename);
  }

  static List<RowMetaAndData> readAllRows(String filename, List<ParquetField> fields)
      throws IOException {
    byte[] data = Files.readAllBytes(Path.of(filename));
    ParquetStream inputFile = new ParquetStream(data, filename);
    ParquetReadSupport readSupport = new ParquetReadSupport(fields);
    try (ParquetReader<RowMetaAndData> reader =
        new ParquetReaderBuilder<>(readSupport, inputFile).build()) {
      List<RowMetaAndData> rows = new ArrayList<>();
      RowMetaAndData row;
      while ((row = reader.read()) != null) {
        // ParquetReader reuses the same RowMetaAndData instance on every read().
        rows.add(row.clone());
      }
      return rows;
    }
  }
}
