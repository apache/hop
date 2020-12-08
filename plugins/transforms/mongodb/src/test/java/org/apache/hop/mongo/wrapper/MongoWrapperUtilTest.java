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

package org.apache.hop.mongo.wrapper;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputMeta;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMeta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Created by bryan on 8/22/14. */
public class MongoWrapperUtilTest {
  private static final String SOCKET_TIMEOUT = "mongoDbSocketTimeout";
  private static final String CONNECTION_TIMEOUT = "mongoDbConnectionTimeout";
  private static final String PASSWORD = "password";

  private MongoWrapperClientFactory cachedFactory;
  private MongoWrapperClientFactory mockFactory;

  @Before
  public void setup() {
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    mockFactory = mock(MongoWrapperClientFactory.class);
    MongoWrapperUtil.setMongoWrapperClientFactory(mockFactory);
  }

  @After
  public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory(cachedFactory);
  }

  @Test
  public void testCreateCalledNoReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock(MongoDbMeta.class);
    IVariables variables = mock(IVariables.class);
    ILogChannel logChannelInterface = mock(ILogChannel.class);

    MongoClientWrapper wrapper = mock(MongoClientWrapper.class);
    when(mockFactory.createMongoClientWrapper(
            any(MongoProperties.class), any(HopMongoUtilLogger.class)))
        .thenReturn(wrapper);
    assertEquals(
        wrapper,
        MongoWrapperUtil.createMongoClientWrapper(mongoDbMeta, variables, logChannelInterface));
  }

  @Test
  public void testCreateCalledReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock(MongoDbMeta.class);
    IVariables variables = mock(IVariables.class);
    ILogChannel logChannelInterface = mock(ILogChannel.class);

    MongoClientWrapper wrapper = mock(MongoClientWrapper.class);
    when(mongoDbMeta.getReadPrefTagSets()).thenReturn(Arrays.asList("test", "test2"));
    when(mockFactory.createMongoClientWrapper(
            any(MongoProperties.class), any(HopMongoUtilLogger.class)))
        .thenReturn(wrapper);
    assertEquals(
        wrapper,
        MongoWrapperUtil.createMongoClientWrapper(mongoDbMeta, variables, logChannelInterface));
  }

  @Test
  public void testCreatePropertiesBuilder() {
    MongoDbMeta input = new MongoDbInputMeta();
    setSocetAndConnectionTimeouts(
        input, "${" + CONNECTION_TIMEOUT + "}", "${" + SOCKET_TIMEOUT + "}");

    MongoDbMeta output = new MongoDbOutputMeta();
    setSocetAndConnectionTimeouts(
        output, "${" + CONNECTION_TIMEOUT + "}", "${" + SOCKET_TIMEOUT + "}");

    IVariables vars = new Variables();
    vars.setVariable(CONNECTION_TIMEOUT, "200");
    vars.setVariable(SOCKET_TIMEOUT, "500");

    MongoProperties inProps = MongoWrapperUtil.createPropertiesBuilder(input, vars).build();
    MongoProperties outProps = MongoWrapperUtil.createPropertiesBuilder(output, vars).build();

    checkProps(inProps, "200", "500");
    checkProps(outProps, "200", "500");
  }

  @Test
  public void testPropertiesBuilderEncrPassword() throws HopException {
    final String pass = "pass";
    testPropertiesBuilderForPassword(true, pass);
    testPropertiesBuilderForPassword(false, pass);
  }

  private void testPropertiesBuilderForPassword(boolean isEncrypted, String password)
      throws HopException {
    MongoDbMeta input = new MongoDbInputMeta();
    setPassword(input, "${" + PASSWORD + "}");

    MongoDbMeta output = new MongoDbOutputMeta();
    setPassword(output, "${" + PASSWORD + "}");

    IVariables vars = new Variables();

    initEncryptor();

    String value;
    if (isEncrypted) {
      value = Encr.encryptPasswordIfNotUsingVariables(password);
    } else {
      value = password;
    }
    vars.setVariable(PASSWORD, value);

    MongoProperties inProps = MongoWrapperUtil.createPropertiesBuilder(input, vars).build();
    MongoProperties outProps = MongoWrapperUtil.createPropertiesBuilder(output, vars).build();

    checkPass(inProps, password);
    checkPass(outProps, password);
  }

  private void initEncryptor() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init(true);
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  private void setSocetAndConnectionTimeouts(MongoDbMeta meta, String connection, String session) {
    meta.setConnectTimeout(connection);
    meta.setSocketTimeout(session);
  }

  private void setPassword(MongoDbMeta meta, String password) {
    meta.setAuthenticationPassword(password);
  }

  private void checkProps(MongoProperties props, String cTimeout, String sTimeout) {
    assertEquals(cTimeout, props.get(MongoProp.connectTimeout));
    assertEquals(sTimeout, props.get(MongoProp.socketTimeout));
  }

  private void checkPass(MongoProperties props, String password) {
    assertEquals(password, props.get(MongoProp.PASSWORD));
  }
}
