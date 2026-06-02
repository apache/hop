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

package org.apache.hop.pipeline.transforms.ssh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PublicKey;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.sshd.server.SshServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Unit test ssh transform */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class SshTransformTest extends SshTestSupport {
  @TempDir static java.nio.file.Path tempDir;
  private static SshServer server;
  private static int sshPort;

  private TransformMockHelper<SshMeta, SshData> mockHelper;

  @BeforeAll
  static void beforeAll() throws Exception {
    PublicKey authorizedPublicKey = null;
    if (isSshKeygenAvailable()) {
      String keyName = "ssh_form";
      Path privateKeyPath = generateRsaKeyPair(tempDir, keyName);
      authorizedPublicKey = loadPublicKeyFromPubFile(Path.of(privateKeyPath + ".pub"));
    }
    server = startPasswordSshServer(authorizedPublicKey);
    sshPort = server.getPort();
  }

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("SSH TEST", SshMeta.class, SshData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void initFailsWhenUserNameMissing() {
    SshMeta meta = passwordMeta(sshPort);
    meta.setUserName(null);
    Ssh ssh = newSsh(meta, new SshData());

    assertFalse(ssh.init());
  }

  @Test
  void initFailsWhenStdOutFieldMissing() {
    SshMeta meta = passwordMeta(sshPort);
    meta.setStdOutFieldName(null);
    Ssh ssh = newSsh(meta, new SshData());

    assertFalse(ssh.init());
  }

  @Test
  void initOpensSessionOnValidPasswordConfig() {
    SshMeta meta = passwordMeta(sshPort);
    SshData data = new SshData();
    Ssh ssh = newSsh(meta, data);

    assertTrue(ssh.init());
    assertNotNull(data.session);
    assertTrue(data.session.isConnected());
    ssh.dispose();
    assertNull(data.session);
  }

  @Test
  void staticCommandProcessesSingleRow() throws Exception {
    String token = "hop-static";
    SshMeta meta = passwordMeta(sshPort);
    meta.setCommand(echoCommand(token));
    SshData data = new SshData();
    Ssh ssh = newSsh(meta, data);
    QueueRowSet output = attachOutput(ssh);

    assertTrue(ssh.init());
    assertTrue(ssh.processRow());
    Object[] row = output.getRow();
    assertNotNull(row);
    assertTrue(row[0].toString().contains(token));

    assertFalse(ssh.processRow());
    ssh.dispose();
  }

  @Test
  void dynamicCommandReadsCommandFromInputField() throws Exception {
    String token = "hop-dynamic";
    SshMeta meta = passwordMeta(sshPort);
    meta.setDynamicCommandField(true);
    meta.setCommandFieldName("cmd");
    meta.setCommand(null);

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("cmd"));
    QueueRowSet input = new QueueRowSet();
    input.setRowMeta(inputMeta);
    input.putRow(inputMeta, new Object[] {echoCommand(token)});
    input.setDone();

    SshData data = new SshData();
    Ssh ssh = newSsh(meta, data);
    ssh.addRowSetToInputRowSets(input);
    QueueRowSet output = attachOutput(ssh);

    assertTrue(ssh.init());
    assertTrue(ssh.processRow());
    Object[] row = output.getRow();
    assertNotNull(row);
    assertEquals("cmd", inputMeta.getValueMeta(0).getName());
    assertTrue(row[1].toString().contains(token), () -> "row=" + java.util.Arrays.toString(row));

    assertFalse(ssh.processRow());
    ssh.dispose();
  }

  @Test
  void dynamicCommandEmptyCommandRoutesToErrorRowWhenErrorHandlingEnabled() throws Exception {
    SshMeta meta = passwordMeta(sshPort);
    meta.setDynamicCommandField(true);
    meta.setCommandFieldName("cmd");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("cmd"));
    QueueRowSet input = new QueueRowSet();
    input.setRowMeta(inputMeta);
    input.putRow(inputMeta, new Object[] {""});
    input.setDone();

    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    TransformErrorMeta errorMeta = mock(TransformErrorMeta.class);
    when(mockHelper.transformMeta.getTransformErrorMeta()).thenReturn(errorMeta);
    when(errorMeta.getErrorRowMeta(any())).thenReturn(inputMeta);

    SshData data = new SshData();
    Ssh ssh = newSsh(meta, data);
    ssh.addRowSetToInputRowSets(input);
    attachOutput(ssh);

    assertTrue(ssh.init());
    assertTrue(ssh.processRow());
    ssh.dispose();
  }

  @Test
  void outputIncludesStdErrFlagWhenConfigured() throws Exception {
    SshMeta meta = passwordMeta(sshPort);
    meta.setCommand(echoCommand("flag-test"));
    meta.setStdErrFieldName("hasErr");

    SshData data = new SshData();
    Ssh ssh = newSsh(meta, data);
    QueueRowSet output = attachOutput(ssh);

    assertTrue(ssh.init());
    assertTrue(ssh.processRow());
    Object[] row = output.getRow();
    assertNotNull(row);
    assertEquals(2, row.length);
    assertInstanceOf(Boolean.class, row[1]);

    ssh.dispose();
  }

  @Test
  void getFieldsForStaticCommandClearsInputAndAddsOutput() throws Exception {
    SshMeta meta = passwordMeta(sshPort);
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("keep"));

    meta.getFields(row, "ssh", null, null, localVariables(), null);

    assertEquals(2, row.size());
    assertEquals("stdOut", row.getValueMeta(0).getName());
    assertEquals(ValueMetaBoolean.TYPE_BOOLEAN, row.getValueMeta(1).getType());
  }

  @AfterAll
  static void stop() throws IOException {
    if (server != null) {
      stopSharedServer(server);
    }
  }

  private Ssh newSsh(SshMeta meta, SshData data) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    Ssh ssh =
        new Ssh(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    ssh.setMetadataProvider(mock(IHopMetadataProvider.class));
    return ssh;
  }

  private static QueueRowSet attachOutput(Ssh ssh) {
    QueueRowSet output = new QueueRowSet();
    ssh.addRowSetToOutputRowSets(output);
    return output;
  }
}
