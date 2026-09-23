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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link ParquetStream} */
class ParquetStreamTest {

  private static final byte[] CONTENT = "Apache Hop Parquet".getBytes(StandardCharsets.UTF_8);

  @BeforeAll
  static void setUpBeforeAll() throws Exception {
    HopClientEnvironment.init();
  }

  private static FileObject createFile(Path folder) throws Exception {
    FileObject fileObject = HopVfs.getFileObject(folder.resolve("test.parquet").toString());
    try (OutputStream outputStream = HopVfs.getOutputStream(fileObject, false)) {
      outputStream.write(CONTENT);
    }
    return fileObject;
  }

  /**
   * A local file is read through the native Parquet implementation. That one needs a real local
   * path: on Windows the drive letter lives in the root of the VFS file name, so using the VFS path
   * on its own would resolve against the current drive.
   */
  @Test
  void testLocalFileLength(@TempDir Path folder) throws Exception {
    FileObject fileObject = createFile(folder);

    try (ParquetStream stream = new ParquetStream(fileObject, fileObject.toString())) {
      assertEquals(CONTENT.length, stream.getLength());
    }
  }

  @Test
  void testLocalFileRead(@TempDir Path folder) throws Exception {
    FileObject fileObject = createFile(folder);

    try (ParquetStream stream = new ParquetStream(fileObject, fileObject.toString());
        SeekableInputStream inputStream = stream.newStream()) {
      byte[] buffer = new byte[CONTENT.length];
      inputStream.readFully(buffer);
      assertEquals(
          new String(CONTENT, StandardCharsets.UTF_8), new String(buffer, StandardCharsets.UTF_8));
    }
  }

  /** Parquet puts this in its error messages ("...in file %s") so it has to stay readable. */
  @Test
  void testToStringIsThePlainFilename(@TempDir Path folder) throws Exception {
    FileObject fileObject = createFile(folder);
    String filename = folder.resolve("test.parquet").toString();

    try (ParquetStream stream = new ParquetStream(fileObject, filename)) {
      assertEquals(filename, stream.toString());
    }
  }

  /**
   * A file which isn't local (S3, Azure, ... here the in-memory ram:// file system) is read through
   * VFS, seeking back and forth between the footer and the column chunks.
   */
  @Test
  void testRemoteParquetFileIsReadThroughVfs(@TempDir Path folder) throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message m { required int64 id; required binary name (STRING); }");
    Path local = folder.resolve("remote.parquet");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(local))
            .withType(schema)
            .withPageSize(1024)
            .build()) {
      for (long id = 0; id < 1000; id++) {
        writer.write(groups.newGroup().append("id", id).append("name", "row " + id));
      }
    }
    FileObject remote = HopVfs.getFileObject("ram:///parquet/remote.parquet");
    remote.getParent().createFolder();
    try (OutputStream outputStream = HopVfs.getOutputStream(remote, false)) {
      outputStream.write(Files.readAllBytes(local));
    }

    List<ParquetField> fields =
        List.of(
            new ParquetField("id", "id", "Integer", null, "-1", "-1"),
            new ParquetField("name", "name", "String", null, "-1", "-1"));
    long count = 0;
    try (ParquetStream stream = new ParquetStream(remote, remote.toString());
        ParquetReader<RowMetaAndData> reader =
            new ParquetReaderBuilder<>(new ParquetReadSupport(fields), stream).build()) {
      assertEquals(Files.size(local), stream.getLength());
      RowMetaAndData row;
      while ((row = reader.read()) != null) {
        assertEquals(count, row.getData()[0]);
        assertEquals("row " + count, row.getData()[1]);
        count++;
      }
    } finally {
      remote.delete();
    }
    assertEquals(1000, count);
  }

  /** Seeking a remote file reopens it and skips to the position asked for. */
  @Test
  void testRemoteStreamSeeksBothWays() throws Exception {
    FileObject remote = HopVfs.getFileObject("ram:///parquet/seek.bin");
    remote.getParent().createFolder();
    try (OutputStream outputStream = HopVfs.getOutputStream(remote, false)) {
      outputStream.write(CONTENT);
    }
    try (ParquetStream stream = new ParquetStream(remote, remote.toString());
        SeekableInputStream inputStream = stream.newStream()) {
      inputStream.seek(7);
      assertEquals(7, inputStream.getPos());
      assertEquals('H', inputStream.read());
      assertEquals(8, inputStream.getPos());

      inputStream.seek(0);
      byte[] buffer = new byte[6];
      inputStream.readFully(buffer);
      assertEquals("Apache", new String(buffer, StandardCharsets.UTF_8));
      assertEquals(6, inputStream.getPos());
    } finally {
      remote.delete();
    }
  }
}
