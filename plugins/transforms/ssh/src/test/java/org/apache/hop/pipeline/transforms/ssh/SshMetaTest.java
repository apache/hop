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

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SshMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  public void testEncryptedPasswords() throws HopException {
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
  public void testRoundTrips() throws HopException {
    List<String> commonFields =
        Arrays.<String>asList(
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
}
