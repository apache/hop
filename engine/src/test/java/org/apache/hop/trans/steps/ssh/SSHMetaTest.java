/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.trans.steps.ssh;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SSHMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testEncryptedPasswords() throws HopXMLException {
    String plaintextPassword = "MyEncryptedPassword";
    String plaintextPassphrase = "MyEncryptedPassPhrase";
    String plaintextProxyPassword = "MyEncryptedProxyPassword";

    SSHMeta sshMeta = new SSHMeta();
    sshMeta.setpassword( plaintextPassword );
    sshMeta.setPassphrase( plaintextPassphrase );
    sshMeta.setProxyPassword( plaintextProxyPassword );

    StringBuilder xmlString = new StringBuilder( 50 );
    xmlString.append( XMLHandler.getXMLHeader() ).append( Const.CR );
    xmlString.append( XMLHandler.openTag( "step" ) ).append( Const.CR );
    xmlString.append( sshMeta.getXML() );
    xmlString.append( XMLHandler.closeTag( "step" ) ).append( Const.CR );
    Node sshXMLNode = XMLHandler.loadXMLString( xmlString.toString(), "step" );

    assertEquals( Encr.encryptPasswordIfNotUsingVariables( plaintextPassword ),
      XMLHandler.getTagValue( sshXMLNode, "password" ) );
    assertEquals( Encr.encryptPasswordIfNotUsingVariables( plaintextPassphrase ),
      XMLHandler.getTagValue( sshXMLNode, "passPhrase" ) );
    assertEquals( Encr.encryptPasswordIfNotUsingVariables( plaintextProxyPassword ),
      XMLHandler.getTagValue( sshXMLNode, "proxyPassword" ) );
  }

  @Test
  public void testRoundTrips() throws HopException {
    List<String> commonFields = Arrays.<String>asList( "dynamicCommandField", "command", "commandfieldname", "port",
      "servername", "userName", "password", "usePrivateKey", "keyFileName", "passPhrase", "stdOutFieldName",
      "stdErrFieldName", "timeOut", "proxyHost", "proxyPort", "proxyUsername", "proxyPassword" );

    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "dynamicCommandField", "isDynamicCommand" );
    getterMap.put( "command", "getCommand" );
    getterMap.put( "commandfieldname", "getcommandfieldname" );
    getterMap.put( "port", "getPort" );
    getterMap.put( "servername", "getServerName" );
    getterMap.put( "userName", "getuserName" );
    getterMap.put( "password", "getpassword" );
    getterMap.put( "usePrivateKey", "isusePrivateKey" );
    getterMap.put( "keyFileName", "getKeyFileName" );
    getterMap.put( "passPhrase", "getPassphrase" );
    getterMap.put( "stdOutFieldName", "getStdOutFieldName" );
    getterMap.put( "stdErrFieldName", "getStdErrFieldName" );
    getterMap.put( "timeOut", "getTimeOut" );
    getterMap.put( "proxyHost", "getProxyHost" );
    getterMap.put( "proxyPort", "getProxyPort" );
    getterMap.put( "proxyUsername", "getProxyUsername" );
    getterMap.put( "proxyPassword", "getProxyPassword" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "dynamicCommandField", "setDynamicCommand" );
    setterMap.put( "command", "setCommand" );
    setterMap.put( "commandfieldname", "setcommandfieldname" );
    setterMap.put( "port", "setPort" );
    setterMap.put( "servername", "setServerName" );
    setterMap.put( "userName", "setuserName" );
    setterMap.put( "password", "setpassword" );
    setterMap.put( "usePrivateKey", "usePrivateKey" );
    setterMap.put( "keyFileName", "setKeyFileName" );
    setterMap.put( "passPhrase", "setPassphrase" );
    setterMap.put( "stdOutFieldName", "setstdOutFieldName" );
    setterMap.put( "stdErrFieldName", "setStdErrFieldName" );
    setterMap.put( "timeOut", "setTimeOut" );
    setterMap.put( "proxyHost", "setProxyHost" );
    setterMap.put( "proxyPort", "setProxyPort" );
    setterMap.put( "proxyUsername", "setProxyUsername" );
    setterMap.put( "proxyPassword", "setProxyPassword" );

    LoadSaveTester tester = new LoadSaveTester( SSHMeta.class, commonFields, getterMap, setterMap );

    tester.testSerialization();
  }

}
