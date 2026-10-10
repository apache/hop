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

package org.apache.hop.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.lakehouse.iceberg.io.HopVfsFileIO;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopVfsFileIOTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void writesReadsSeeksAndDeletes() throws Exception {
    HopVfsFileIO io = new HopVfsFileIO();
    String location = tempDir.resolve("nested/folder/file.bin").toUri().toString();
    byte[] content = "0123456789abcdefghij".getBytes(StandardCharsets.US_ASCII);

    OutputFile outputFile = io.newOutputFile(location);
    try (OutputStream out = outputFile.create()) {
      out.write(content);
    }
    assertThrows(AlreadyExistsException.class, outputFile::create);

    InputFile inputFile = io.newInputFile(location);
    assertTrue(inputFile.exists());
    assertEquals(content.length, inputFile.getLength());

    try (SeekableInputStream in = inputFile.newStream()) {
      in.seek(15);
      assertEquals('f', in.read());
      assertEquals(16, in.getPos());
      in.seek(2);
      byte[] buffer = new byte[3];
      assertEquals(3, in.read(buffer, 0, 3));
      assertArrayEquals("234".getBytes(StandardCharsets.US_ASCII), buffer);
      assertEquals(5, in.getPos());
    }
    try (InputStream in = inputFile.newStream()) {
      assertArrayEquals(content, in.readAllBytes());
    }

    io.deleteFile(location);
    assertFalse(io.newInputFile(location).exists());
  }
}
