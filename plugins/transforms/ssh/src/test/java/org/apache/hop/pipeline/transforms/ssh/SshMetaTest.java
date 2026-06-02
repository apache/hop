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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;

class SshMetaTest extends SshTestSupport {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testEncryptedPasswords() throws HopException {
    String plaintextPassword = "MyEncryptedPassword";
    String plaintextPassphrase = "MyEncryptedPassPhrase";
    String plaintextProxyPassword = "MyEncryptedProxyPassword";

    SshMeta sshMeta = new SshMeta();
    sshMeta.setPassword(plaintextPassword);
    sshMeta.setPassPhrase(plaintextPassphrase);
    sshMeta.setProxyPassword(plaintextProxyPassword);

    StringBuilder xmlString = new StringBuilder(50);
    xmlString.append(XmlHandler.getXmlHeader()).append(Const.CR);
    xmlString.append(XmlHandler.openTag("transform")).append(Const.CR);

    xmlString.append(sshMeta.getXml());
    xmlString.append(XmlHandler.closeTag("transform")).append(Const.CR);
    Node sshXmlNode = XmlHandler.loadXmlString(xmlString.toString(), "transform");

    assertEquals(
        Encr.encryptPasswordIfNotUsingVariables(plaintextPassword),
        XmlHandler.getTagValue(sshXmlNode, "password"));
    assertEquals(
        Encr.encryptPasswordIfNotUsingVariables(plaintextPassphrase),
        XmlHandler.getTagValue(sshXmlNode, "passPhrase"));
    assertEquals(
        Encr.encryptPasswordIfNotUsingVariables(plaintextProxyPassword),
        XmlHandler.getTagValue(sshXmlNode, "proxyPassword"));
  }

  @Test
  void testRoundTrips() throws HopException {
    List<String> commonFields =
        Arrays.asList(
            "dynamicCommandField",
            "command",
            "commandFieldName",
            "port",
            "serverName",
            "userName",
            "password",
            "usePrivateKey",
            "keyFileName",
            "passPhrase",
            "stdOutFieldName",
            "stdErrFieldName",
            "timeOut",
            "proxyHost",
            "proxyPort",
            "proxyUsername",
            "proxyPassword");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    LoadSaveTester tester = new LoadSaveTester(SshMeta.class, commonFields, getterMap, setterMap);

    tester.testSerialization();
  }

  @Test
  void defaultValues() {
    SshMeta meta = new SshMeta();
    assertEquals("22", meta.getPort());
    assertEquals("stdOut", meta.getStdOutFieldName());
    assertEquals("stdErr", meta.getStdErrFieldName());
    assertTrue(meta.isUsePrivateKey());
    assertFalse(meta.isDynamicCommandField());
  }

  @Test
  void supportsErrorHandling() {
    assertTrue(new SshMeta().supportsErrorHandling());
  }

  @Test
  void getFieldsStaticCommandReplacesInputRowMeta() throws HopException {
    SshMeta meta = new SshMeta();
    meta.setDynamicCommandField(false);
    meta.setStdOutFieldName("out");
    meta.setStdErrFieldName("");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("inputField"));

    meta.getFields(row, "ssh", null, null, new Variables(), null);

    assertEquals(1, row.size());
    assertEquals("out", row.getValueMeta(0).getName());
  }

  @Test
  void getFieldsDynamicCommandKeepsInputFields() throws HopException {
    SshMeta meta = new SshMeta();
    meta.setDynamicCommandField(true);
    meta.setStdOutFieldName("out");
    meta.setStdErrFieldName("errFlag");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("cmd"));

    meta.getFields(row, "ssh", null, null, new Variables(), null);

    assertEquals(3, row.size());
    assertEquals("cmd", row.getValueMeta(0).getName());
    assertEquals("out", row.getValueMeta(1).getName());
    assertEquals("errFlag", row.getValueMeta(2).getName());
  }

  @Test
  void checkReportsErrorWhenServerMissing(@TempDir java.nio.file.Path tempDir) {
    SshMeta meta = new SshMeta();
    meta.setUsePrivateKey(false);
    meta.setServerName(null);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setTransform(meta);
    PipelineMeta pipelineMeta = new PipelineMeta();

    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        new RowMeta(),
        new String[] {"input"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText()
                            .equals(
                                BaseMessages.getString(
                                    SshMeta.class, "SSHMeta.CheckResult.TargetHostMissing"))));
  }

  @Test
  void checkReportsErrorWhenPrivateKeyEnabledButPathMissing() {
    SshMeta meta = new SshMeta();
    meta.setServerName("localhost");
    meta.setUsePrivateKey(true);
    meta.setKeyFileName(null);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setTransform(meta);

    meta.check(
        remarks,
        new PipelineMeta(),
        transformMeta,
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText()
                            .equals(
                                BaseMessages.getString(
                                    SshMeta.class,
                                    "SSHMeta.CheckResult.PrivateKeyFileNameMissing"))));
  }

  @Test
  void checkReportsErrorWhenNoInputTransforms() {
    SshMeta meta = new SshMeta();
    meta.setServerName("localhost");
    meta.setUsePrivateKey(false);

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setTransform(meta);

    meta.check(
        remarks,
        new PipelineMeta(),
        transformMeta,
        new RowMeta(),
        new String[0],
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText()
                            .equals(
                                BaseMessages.getString(
                                    SshMeta.class, "SSHMeta.CheckResult.NoInpuReceived"))));
  }

  @Test
  void checkOkWhenServerAndInputPresent(@TempDir java.nio.file.Path keyDir) throws Exception {
    Assumptions.assumeTrue(isSshKeygenAvailable(), "ssh-keygen is required to generate test keys");
    String keyName = "ssh_meta";
    Path privateKey = generateRsaKeyPair(keyDir, keyName);
    SshMeta meta = new SshMeta();
    meta.setServerName("localhost");
    meta.setUsePrivateKey(true);
    meta.setKeyFileName(privateKey.toUri().toString());

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setTransform(meta);

    meta.check(
        remarks,
        new PipelineMeta(),
        transformMeta,
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_OK
                        && r.getText()
                            .equals(
                                BaseMessages.getString(
                                    SshMeta.class, "SSHMeta.CheckResult.TargetHostOK"))));
    assertFalse(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_OK
                        && r.getText()
                            .equals(
                                BaseMessages.getString(
                                    SshMeta.class,
                                    "SSHMeta.CheckResult.PrivateKeyFileExists",
                                    privateKey.toString()))));
  }
}
