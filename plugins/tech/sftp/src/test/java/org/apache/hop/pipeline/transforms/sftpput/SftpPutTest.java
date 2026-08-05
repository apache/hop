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
package org.apache.hop.pipeline.transforms.sftpput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.sftpput.SftpPutMeta.AfterSftpPut;
import org.apache.hop.vfs.sftp.SftpConnectionFileProvider;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What the transform makes of its configuration before it touches the server: every one of these
 * ends the pipeline, so the message has to say what to fix.
 */
class SftpPutTest {

  private static final String CONNECTION_NAME = "unit-test-sftp";

  private TransformMockHelper<SftpPutMeta, ITransformData> helper;
  private MemoryMetadataProvider metadataProvider;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    helper = new TransformMockHelper<>("SFTP Put", SftpPutMeta.class, ITransformData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);

    SftpConnection connection = new SftpConnection();
    connection.setName(CONNECTION_NAME);
    connection.setServerName("sftp.example.com");
    connection.setUsername("hop");

    metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(SftpConnection.class).save(connection);

    // Registering a provider doesn't open anything, so the transform gets past its "is this
    // connection available as a VFS scheme" guard without a server in sight.
    HopVfs.getFileSystemManager()
        .addProvider(CONNECTION_NAME, new SftpConnectionFileProvider(new Variables(), connection));
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
    // The provider above is registered on the one and only file system manager.
    HopVfs.reset();
  }

  @Test
  void testConnectionIsMandatory() {
    SftpPutMeta meta = meta();
    meta.setConnection("");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("Please specify an SFTP connection"), e.getMessage());
  }

  @Test
  void testUnknownConnectionIsReported() {
    SftpPutMeta meta = meta();
    meta.setConnection("does-not-exist");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("does-not-exist"), e.getMessage());
  }

  /** A connection which isn't registered as a VFS scheme can't be uploaded through. */
  @Test
  void testConnectionWithoutAVfsProviderIsReported() throws Exception {
    SftpConnection other = new SftpConnection();
    other.setName("not-registered");
    other.setServerName("sftp.example.com");
    metadataProvider.getSerializer(SftpConnection.class).save(other);

    SftpPutMeta meta = meta();
    meta.setConnection("not-registered");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("not-registered"), e.getMessage());
  }

  @Test
  void testSourceFieldIsMandatory() {
    SftpPutMeta meta = meta();
    meta.setSourceFileFieldName("");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("source file name or content"), e.getMessage());
  }

  @Test
  void testSourceFieldHasToBeInTheStream() {
    SftpPutMeta meta = meta();
    meta.setSourceFileFieldName("absent");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("absent"), e.getMessage());
  }

  @Test
  void testRemoteFolderFieldIsMandatory() {
    SftpPutMeta meta = meta();
    meta.setRemoteDirectoryFieldName("");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("remote folder"), e.getMessage());
  }

  /** Uploading the content of a field leaves nothing to derive the remote file name from. */
  @Test
  void testStreamModeNeedsARemoteFilenameField() {
    SftpPutMeta meta = meta();
    meta.setInputIsStream(true);
    meta.setRemoteFilenameFieldName("");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("remote file name"), e.getMessage());
  }

  @Test
  void testMoveNeedsADestinationFolderField() {
    SftpPutMeta meta = meta();
    meta.setAfterSftpPut(AfterSftpPut.MOVE);
    meta.setDestinationFolderFieldName("");

    HopException e = assertThrows(HopException.class, () -> processOneRow(meta));
    assertTrue(e.getMessage().contains("move the source file to"), e.getMessage());
  }

  /**
   * A configuration which passes every check gets to the upload, and a row which fails there stops
   * the transform with an error rather than sailing on quietly. The source file of the row below
   * doesn't exist, so no server is needed to get there.
   */
  @Test
  void testAFailingRowStopsTheTransform() throws Exception {
    SftpPut transform = processOneRow(meta());

    assertEquals(1, transform.getErrors());
    assertFalse(transform.processRow(), "the transform should have stopped");
  }

  private SftpPutMeta meta() {
    SftpPutMeta meta = new SftpPutMeta();
    meta.setConnection(CONNECTION_NAME);
    meta.setSourceFileFieldName("sourcefile");
    meta.setRemoteDirectoryFieldName("remotefolder");
    meta.setRemoteFilenameFieldName("remotefile");
    return meta;
  }

  /**
   * Feed the transform a single row. Anything the first row runs into ends the pipeline, so it
   * comes back out of processRow().
   */
  private SftpPut processOneRow(SftpPutMeta meta) throws Exception {
    SftpPutData data = new SftpPutData();
    SftpPut transform =
        new SftpPut(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.setMetadataProvider(metadataProvider);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("sourcefile"));
    inputRowMeta.addValueMeta(new ValueMetaString("remotefolder"));
    inputRowMeta.addValueMeta(new ValueMetaString("remotefile"));
    transform.setInputRowMeta(inputRowMeta);

    SftpPut spied = spy(transform);
    doReturn(new Object[] {"/tmp/no-such-file-for-the-sftp-put-test.txt", "/upload", "target.txt"})
        .when(spied)
        .getRow();
    spied.processRow();
    return spied;
  }
}
