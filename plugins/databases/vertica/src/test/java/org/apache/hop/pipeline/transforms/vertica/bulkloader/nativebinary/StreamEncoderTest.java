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

package org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.PipedInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.jupiter.api.Test;

/**
 * Checks the bytes the encoder puts on the wire against Vertica's native binary format, in
 * particular the column widths in the file header: a fixed width type has to declare its width, a
 * variable width type declares -1.
 */
class StreamEncoderTest {

  private static final byte[] SIGNATURE = {
    'N', 'A', 'T', 'I', 'V', 'E', 0x0A, (byte) 0xFF, 0x0D, 0x0A, 0x00
  };

  @Test
  void headerDeclaresTheWidthOfEveryColumn() throws IOException {
    byte[] header =
        encode(
            List.of(
                new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64),
                new ColumnSpec(ColumnSpec.ConstantWidthType.DATE),
                new ColumnSpec(ColumnSpec.UserDefinedWidthType.CHAR, 8),
                new ColumnSpec(ColumnSpec.UserDefinedWidthType.BINARY, 10),
                new ColumnSpec(ColumnSpec.VariableWidthType.VARCHAR, 255),
                new ColumnSpec(ColumnSpec.VariableWidthType.VARBINARY, 255),
                new ColumnSpec(ColumnSpec.PrecisionScaleWidthType.NUMERIC, 18, 4)));

    ByteBuffer buffer = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);

    byte[] signature = new byte[SIGNATURE.length];
    buffer.get(signature);
    assertArrayEquals(SIGNATURE, signature);

    assertEquals(5 + 4 * 7, buffer.getInt(), "header area length");
    assertEquals(1, buffer.getShort(), "format version");
    assertEquals(0, buffer.get(), "filler");
    assertEquals(7, buffer.getShort(), "number of columns");

    int[] widths = new int[7];
    for (int i = 0; i < widths.length; i++) {
      widths[i] = buffer.getInt();
    }
    // BINARY is fixed width and NUMERIC travels as a VARCHAR filler column, hence -1.
    assertArrayEquals(new int[] {8, 8, 8, 10, -1, -1, -1}, widths);
  }

  @Test
  void padsFixedWidthBinaryValuesUpToTheColumnWidth() throws Exception {
    ColumnSpec binary = new ColumnSpec(ColumnSpec.UserDefinedWidthType.BINARY, 4);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("PAYLOAD"));

    byte[] written = encodeRow(List.of(binary), rowMeta, new Object[] {new byte[] {1, 2}});

    ByteBuffer buffer = ByteBuffer.wrap(written).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(4, buffer.getInt(), "row data size");
    assertEquals(0, buffer.get(), "null bitmap");
    byte[] payload = new byte[4];
    buffer.get(payload);
    assertArrayEquals(new byte[] {1, 2, 0, 0}, payload);
  }

  @Test
  void marksMissingValuesInTheNullBitmap() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("A"));
    rowMeta.addValueMeta(new ValueMetaInteger("B"));

    byte[] written =
        encodeRow(
            List.of(
                new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64),
                new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64)),
            rowMeta,
            new Object[] {null, 7L});

    ByteBuffer buffer = ByteBuffer.wrap(written).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(8, buffer.getInt(), "only the second column carries data");
    // The first column (index 0) is the most significant bit.
    assertEquals((byte) 0b1000_0000, buffer.get());
    assertEquals(7L, buffer.getLong());
  }

  @Test
  void refusesARowThatIsNarrowerThanTheColumnSpec() throws IOException {
    PipedInputStream pipedInputStream = new PipedInputStream();
    StreamEncoder encoder =
        new StreamEncoder(
            List.of(
                new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64),
                new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64)),
            pipedInputStream);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("A"));

    assertThrows(
        IllegalArgumentException.class, () -> encoder.writeRow(rowMeta, new Object[] {1L}));
  }

  @Test
  void writesTheHeaderInUtf8() throws IOException {
    byte[] header = encode(List.of(new ColumnSpec(ColumnSpec.ConstantWidthType.BOOLEAN)));
    assertEquals("NATIVE", new String(header, 0, 6, StandardCharsets.UTF_8));
  }

  /** Writes the header for the given columns and returns the bytes handed to Vertica. */
  private static byte[] encode(List<ColumnSpec> columns) throws IOException {
    PipedInputStream pipedInputStream = new PipedInputStream();
    StreamEncoder encoder = new StreamEncoder(columns, pipedInputStream);
    encoder.writeHeader();
    encoder.close();
    return pipedInputStream.readAllBytes();
  }

  /** Writes a single row (without the header) and returns the bytes handed to Vertica. */
  private static byte[] encodeRow(List<ColumnSpec> columns, IRowMeta rowMeta, Object[] row)
      throws Exception {
    PipedInputStream pipedInputStream = new PipedInputStream();
    StreamEncoder encoder = new StreamEncoder(columns, pipedInputStream);
    encoder.writeRow(rowMeta, row);
    encoder.close();
    return pipedInputStream.readAllBytes();
  }
}
