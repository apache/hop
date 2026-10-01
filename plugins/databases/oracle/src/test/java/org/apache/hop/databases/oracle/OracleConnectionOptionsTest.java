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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * Covers the TLS / wallet / connection type options added to the Oracle connection, and above all
 * that a connection saved before they existed keeps producing exactly the URL it used to.
 */
class OracleConnectionOptionsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private OracleDatabaseMeta meta;
  private IVariables variables;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
    meta = new OracleDatabaseMeta();
    meta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    // The plugin registry sets this on a real connection; extra options are keyed by it.
    meta.setPluginId("ORACLE");
    meta.addDefaultOptions();
    variables = new Variables();
    variables.initializeFrom(null);
  }

  @Nested
  @DisplayName("URL generation without TLS")
  class PlainUrls {

    @Test
    @DisplayName("AUTOMATIC is the default and reproduces the historical URLs")
    void automaticIsTheDefault() throws Exception {
      assertEquals(OracleConnectionType.AUTOMATIC, meta.getConnectionType());

      // No prefix: Hop has always assumed a SID here.
      assertEquals("jdbc:oracle:thin:@FOO:1521:BAR", meta.getURL("FOO", "1521", "BAR"));
      // ':' marks a SID, '/' marks a service name.
      assertEquals("jdbc:oracle:thin:@FOO:1521:BAR", meta.getURL("FOO", "1521", ":BAR"));
      assertEquals("jdbc:oracle:thin:@FOO:1521/BAR", meta.getURL("FOO", "1521", "/BAR"));
      // No host and port: the database name is a descriptor of its own.
      assertEquals("jdbc:oracle:thin:@FOO", meta.getURL("", "", "FOO"));
      assertEquals("jdbc:oracle:thin:@FOO", meta.getURL(null, "-1", "FOO"));
      assertEquals("jdbc:oracle:thin:@FOO", meta.getURL(null, null, "FOO"));
      assertEquals("jdbc:oracle:thin:@", meta.getURL("", "", ""));
    }

    @Test
    @DisplayName("a connection saved before these options existed still works")
    void nullConnectionTypeBehavesAsAutomatic() throws Exception {
      // This is what deserializing XML without a <connectionType> element leaves behind.
      meta.setConnectionType(null);
      meta.setTlsCredentialType(null);

      assertEquals(OracleConnectionType.AUTOMATIC, meta.getConnectionType());
      assertEquals(OracleTlsCredentialType.NONE, meta.getTlsCredentialType());
      assertEquals("jdbc:oracle:thin:@FOO:1521:BAR", meta.getURL("FOO", "1521", "BAR"));
      assertTrue(meta.getConnectionProperties(variables).isEmpty());
    }

    @Test
    @DisplayName("an explicit SID does not need the ':' marker, and tolerates it")
    void explicitSid() throws Exception {
      meta.setConnectionType(OracleConnectionType.SID);

      assertEquals("jdbc:oracle:thin:@FOO:1521:BAR", meta.getURL("FOO", "1521", "BAR"));
      // The marker is redundant now: it must not end up in the URL twice.
      assertEquals("jdbc:oracle:thin:@FOO:1521:BAR", meta.getURL("FOO", "1521", ":BAR"));
    }

    @Test
    @DisplayName("an explicit service name uses the //host:port/service form")
    void explicitServiceName() throws Exception {
      meta.setConnectionType(OracleConnectionType.SERVICE_NAME);

      assertEquals("jdbc:oracle:thin:@//FOO:1521/BAR", meta.getURL("FOO", "1521", "BAR"));
      assertEquals("jdbc:oracle:thin:@//FOO:1521/BAR", meta.getURL("FOO", "1521", "/BAR"));
    }

    @Test
    @DisplayName("a TNS alias ignores host and port")
    void tnsAlias() throws Exception {
      meta.setConnectionType(OracleConnectionType.TNS_ALIAS);

      assertEquals("jdbc:oracle:thin:@mydb_high", meta.getURL("ignored", "1521", "mydb_high"));
    }

    @Test
    @DisplayName("a hand written descriptor is passed through untouched")
    void descriptor() throws Exception {
      meta.setConnectionType(OracleConnectionType.DESCRIPTOR);
      String descriptor =
          "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=node1)(PORT=1521))"
              + "(ADDRESS=(PROTOCOL=TCP)(HOST=node2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=svc)))";

      assertEquals("jdbc:oracle:thin:@" + descriptor, meta.getURL("ignored", "1521", descriptor));
    }

    @Test
    @DisplayName("the OCI branch is untouched")
    void ociUrl() throws Exception {
      // Anything other than TYPE_ACCESS_NATIVE takes the OCI path. Nothing in the UI can select it
      // -- getAccessTypeList() only offers NATIVE -- but the branch is still there, so pin it.
      meta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE + 1);

      assertEquals(
          "jdbc:oracle:oci:@(description=(address=(host=FOO)(protocol=tcp)(port=1521))"
              + "(connect_data=(sid=BAR)))",
          meta.getURL("FOO", "1521", "BAR"));
      assertEquals("jdbc:oracle:oci:@BAR", meta.getURL("", "", "BAR"));
    }
  }

  @Nested
  @DisplayName("URL generation with TLS")
  class TcpsUrls {

    @BeforeEach
    void enableTcps() {
      meta.setUseTcps(true);
    }

    @Test
    @DisplayName("the generated descriptor matches the shape Oracle documents for TCPS")
    void serviceNameDescriptor() throws Exception {
      meta.setConnectionType(OracleConnectionType.SERVICE_NAME);

      assertEquals(
          "jdbc:oracle:thin:@(description=(address=(protocol=tcps)(host=db.example.com)"
              + "(port=2484))(connect_data=(service_name=ORCLPDB1)))",
          meta.getURL("db.example.com", "2484", "ORCLPDB1"));
    }

    @Test
    @DisplayName("an explicit SID produces a sid= descriptor")
    void sidDescriptor() throws Exception {
      meta.setConnectionType(OracleConnectionType.SID);

      assertEquals(
          "jdbc:oracle:thin:@(description=(address=(protocol=tcps)(host=FOO)(port=2484))"
              + "(connect_data=(sid=BAR)))",
          meta.getURL("FOO", "2484", "BAR"));
    }

    @Test
    @DisplayName("AUTOMATIC reads the database name exactly as it does without TLS")
    void automaticKeepsItsReadingOfTheDatabaseName() throws Exception {
      // No prefix keeps the SID assumption, so that ticking the TLS box on an existing connection
      // doesn't quietly change what it connects to.
      assertTrue(meta.getURL("FOO", "2484", "BAR").contains("(connect_data=(sid=BAR))"));
      assertTrue(meta.getURL("FOO", "2484", ":BAR").contains("(connect_data=(sid=BAR))"));
      assertTrue(meta.getURL("FOO", "2484", "/BAR").contains("(connect_data=(service_name=BAR))"));
    }

    @Test
    @DisplayName("an empty or file-based port falls back to the conventional TCPS port")
    void defaultsToTheTcpsPort() throws Exception {
      meta.setConnectionType(OracleConnectionType.SERVICE_NAME);

      assertTrue(meta.getURL("FOO", "", "BAR").contains("(port=2484)"));
      assertTrue(meta.getURL("FOO", "-1", "BAR").contains("(port=2484)"));
      // An explicit port always wins.
      assertTrue(meta.getURL("FOO", "1522", "BAR").contains("(port=1522)"));
    }

    @Test
    @DisplayName("a TNS alias and a hand written descriptor stay out of the way")
    void aliasAndDescriptorAreNotWrapped() throws Exception {
      // The protocol comes from tnsnames.ora or from the descriptor itself, so there is nothing
      // for us to rewrite.
      meta.setConnectionType(OracleConnectionType.TNS_ALIAS);
      assertEquals("jdbc:oracle:thin:@mydb_high", meta.getURL("FOO", "2484", "mydb_high"));

      meta.setConnectionType(OracleConnectionType.DESCRIPTOR);
      assertEquals(
          "jdbc:oracle:thin:@(DESCRIPTION=x)", meta.getURL("FOO", "2484", "(DESCRIPTION=x)"));
    }

    @Test
    @DisplayName("AUTOMATIC without host and port keeps treating the name as a descriptor")
    void automaticWithoutHostAndPort() throws Exception {
      assertEquals("jdbc:oracle:thin:@(DESCRIPTION=x)", meta.getURL("", "", "(DESCRIPTION=x)"));
    }
  }

  @Nested
  @DisplayName("Connection properties")
  class ConnectionProperties {

    @Test
    @DisplayName("nothing configured contributes nothing")
    void emptyByDefault() {
      assertTrue(meta.getConnectionProperties(variables).isEmpty());
    }

    @Test
    @DisplayName("TNS_ADMIN is set with or without TLS, because plain tnsnames.ora needs it too")
    void tnsAdminIsIndependentOfTls() {
      meta.setTnsAdmin("/opt/wallet");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals("/opt/wallet", properties.get(OracleDatabaseMeta.PROPERTY_TNS_ADMIN));
      assertEquals(1, properties.size());

      meta.setUseTcps(true);
      assertEquals(
          "/opt/wallet",
          meta.getConnectionProperties(variables).get(OracleDatabaseMeta.PROPERTY_TNS_ADMIN));
    }

    @Test
    @DisplayName("no TCPS and no credentials means no TLS properties at all")
    void nothingConfiguredMeansNoTlsProperties() {
      meta.setUseTcps(false);
      meta.setTlsCredentialType(OracleTlsCredentialType.NONE);
      // Filled in but unreachable: the credential type is what decides.
      meta.setWalletDirectory("/opt/wallet");
      meta.setTrustStoreFile("/opt/truststore.jks");

      assertTrue(meta.getConnectionProperties(variables).isEmpty());
    }

    @Test
    @DisplayName("credentials still apply when the protocol comes from a TNS alias")
    void credentialsApplyWithoutTheTcpsCheckbox() {
      // A tnsnames.ora entry can say protocol=tcps by itself, in which case there is nothing for
      // the checkbox to switch on -- but the wallet still has to reach the driver.
      meta.setConnectionType(OracleConnectionType.TNS_ALIAS);
      meta.setUseTcps(false);
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      meta.setWalletDirectory("/opt/wallet");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals(
          "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/opt/wallet)))",
          properties.get(OracleDatabaseMeta.PROPERTY_WALLET_LOCATION));
      assertEquals("true", properties.get(OracleDatabaseMeta.PROPERTY_SSL_SERVER_DN_MATCH));
    }

    @Test
    @DisplayName("one way TLS only asserts that the certificate belongs to the server")
    void oneWayTls() {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.NONE);

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals("true", properties.get(OracleDatabaseMeta.PROPERTY_SSL_SERVER_DN_MATCH));
      assertEquals(1, properties.size());
    }

    @Test
    @DisplayName("a wallet is handed to the driver as oracle.net.wallet_location")
    void walletLocation() throws Exception {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      meta.setWalletDirectory("/opt/wallet");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals(
          "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/opt/wallet)))",
          properties.get(OracleDatabaseMeta.PROPERTY_WALLET_LOCATION));
      // Deliberately not in the URL: see OracleDatabaseMeta#getConnectionProperties.
      assertFalse(meta.getURL("FOO", "2484", "BAR").contains("my_wallet_directory"));
    }

    @Test
    @DisplayName("a PKCS12 wallet password is decrypted on its way to the driver")
    void walletPasswordIsDecrypted() {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      meta.setWalletDirectory("/opt/wallet");
      meta.setWalletPassword(Encr.encryptPasswordIfNotUsingVariables("s3cret"));

      assertEquals(
          "s3cret",
          meta.getConnectionProperties(variables).get(OracleDatabaseMeta.PROPERTY_WALLET_PASSWORD));
    }

    @Test
    @DisplayName("an auto-login wallet needs no password")
    void ssoWalletHasNoPassword() {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      meta.setWalletDirectory("/opt/wallet");

      assertFalse(
          meta.getConnectionProperties(variables)
              .containsKey(OracleDatabaseMeta.PROPERTY_WALLET_PASSWORD));
    }

    @Test
    @DisplayName("JKS sets the javax.net.ssl properties, passwords decrypted")
    void javaKeyStore() {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
      meta.setTrustStoreFile("/opt/truststore.jks");
      meta.setTrustStorePassword(Encr.encryptPasswordIfNotUsingVariables("trustpw"));
      meta.setTrustStoreType("JKS");
      meta.setKeyStoreFile("/opt/keystore.jks");
      meta.setKeyStorePassword(Encr.encryptPasswordIfNotUsingVariables("keypw"));
      meta.setKeyStoreType("PKCS12");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals("/opt/truststore.jks", properties.get(OracleDatabaseMeta.PROPERTY_TRUST_STORE));
      assertEquals("trustpw", properties.get(OracleDatabaseMeta.PROPERTY_TRUST_STORE_PASSWORD));
      assertEquals("JKS", properties.get(OracleDatabaseMeta.PROPERTY_TRUST_STORE_TYPE));
      assertEquals("/opt/keystore.jks", properties.get(OracleDatabaseMeta.PROPERTY_KEY_STORE));
      assertEquals("keypw", properties.get(OracleDatabaseMeta.PROPERTY_KEY_STORE_PASSWORD));
      assertEquals("PKCS12", properties.get(OracleDatabaseMeta.PROPERTY_KEY_STORE_TYPE));
    }

    @Test
    @DisplayName("a truststore on its own does not invent keystore properties")
    void trustStoreWithoutKeyStore() {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
      meta.setTrustStoreFile("/opt/truststore.jks");

      Properties properties = meta.getConnectionProperties(variables);
      assertTrue(properties.containsKey(OracleDatabaseMeta.PROPERTY_TRUST_STORE));
      assertFalse(properties.containsKey(OracleDatabaseMeta.PROPERTY_KEY_STORE));
      assertFalse(properties.containsKey(OracleDatabaseMeta.PROPERTY_KEY_STORE_PASSWORD));
    }

    @Test
    @DisplayName("wallet and JKS never reach the driver together")
    void walletAndJksAreExclusive() {
      meta.setUseTcps(true);
      meta.setWalletDirectory("/opt/wallet");
      meta.setTrustStoreFile("/opt/truststore.jks");
      meta.setKeyStoreFile("/opt/keystore.jks");

      // The driver gives the wallet precedence over the keystore properties, so sending both would
      // let half of what the user filled in be ignored without a word.
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      Properties wallet = meta.getConnectionProperties(variables);
      assertTrue(wallet.containsKey(OracleDatabaseMeta.PROPERTY_WALLET_LOCATION));
      assertFalse(wallet.containsKey(OracleDatabaseMeta.PROPERTY_TRUST_STORE));
      assertFalse(wallet.containsKey(OracleDatabaseMeta.PROPERTY_KEY_STORE));

      meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
      Properties jks = meta.getConnectionProperties(variables);
      assertFalse(jks.containsKey(OracleDatabaseMeta.PROPERTY_WALLET_LOCATION));
      assertTrue(jks.containsKey(OracleDatabaseMeta.PROPERTY_TRUST_STORE));
    }

    @Test
    @DisplayName("server DN verification can be turned off, and says so explicitly")
    void serverDnMatch() {
      meta.setUseTcps(true);
      meta.setSslServerDnMatch(false);

      assertEquals(
          "false",
          meta.getConnectionProperties(variables)
              .get(OracleDatabaseMeta.PROPERTY_SSL_SERVER_DN_MATCH));
    }

    @Test
    @DisplayName("an expected certificate DN is passed on when given")
    void serverCertDn() {
      meta.setUseTcps(true);
      meta.setSslServerCertDn("CN=db.example.com,O=Example");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals(
          "CN=db.example.com,O=Example",
          properties.get(OracleDatabaseMeta.PROPERTY_SSL_SERVER_CERT_DN));
    }

    @Test
    @DisplayName("every path is resolved through variables")
    void variablesAreResolved() {
      variables.setVariable("WALLET_HOME", "/secure/wallet");
      variables.setVariable("TNS_HOME", "/secure/tns");
      variables.setVariable("STORE_PW", "fromvariable");

      meta.setUseTcps(true);
      meta.setTnsAdmin("${TNS_HOME}");
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      meta.setWalletDirectory("${WALLET_HOME}");
      meta.setWalletPassword("${STORE_PW}");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals("/secure/tns", properties.get(OracleDatabaseMeta.PROPERTY_TNS_ADMIN));
      assertEquals(
          "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/secure/wallet)))",
          properties.get(OracleDatabaseMeta.PROPERTY_WALLET_LOCATION));
      assertEquals("fromvariable", properties.get(OracleDatabaseMeta.PROPERTY_WALLET_PASSWORD));
    }

    @Test
    @DisplayName("variables resolve in the JKS fields as well")
    void variablesAreResolvedForJks() {
      variables.setVariable("CERT_HOME", "/secure/certs");

      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
      meta.setTrustStoreFile("${CERT_HOME}/truststore.jks");
      meta.setKeyStoreFile("${CERT_HOME}/keystore.jks");

      Properties properties = meta.getConnectionProperties(variables);
      assertEquals(
          "/secure/certs/truststore.jks", properties.get(OracleDatabaseMeta.PROPERTY_TRUST_STORE));
      assertEquals(
          "/secure/certs/keystore.jks", properties.get(OracleDatabaseMeta.PROPERTY_KEY_STORE));
    }
  }

  @Nested
  @DisplayName("DatabaseMeta integration")
  class Integration {

    @Test
    @DisplayName("the plugin's properties reach DatabaseMeta")
    void pluginPropertiesAreMerged() {
      DatabaseMeta databaseMeta = new DatabaseMeta();
      databaseMeta.setIDatabase(meta);
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
      meta.setWalletDirectory("/opt/wallet");

      Properties properties = databaseMeta.getConnectionProperties(variables);
      assertEquals(
          "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/opt/wallet)))",
          properties.get(OracleDatabaseMeta.PROPERTY_WALLET_LOCATION));
    }

    @Test
    @DisplayName("options tab entries are merged in alongside them")
    void extraOptionsAreMerged() {
      DatabaseMeta databaseMeta = new DatabaseMeta();
      databaseMeta.setIDatabase(meta);
      meta.setUseTcps(true);
      meta.setTnsAdmin("/opt/tns");
      databaseMeta.addExtraOption("ORACLE", "oracle.jdbc.mapDateToTimestamp", "false");

      Properties properties = databaseMeta.getConnectionProperties(variables);
      assertEquals("/opt/tns", properties.get(OracleDatabaseMeta.PROPERTY_TNS_ADMIN));
      assertEquals("false", properties.get("oracle.jdbc.mapDateToTimestamp"));
    }

    @Test
    @DisplayName("an explicit option still overrides a computed one, as the escape hatch")
    void extraOptionsWin() {
      DatabaseMeta databaseMeta = new DatabaseMeta();
      databaseMeta.setIDatabase(meta);
      meta.setTnsAdmin("/opt/tns");
      databaseMeta.addExtraOption("ORACLE", OracleDatabaseMeta.PROPERTY_TNS_ADMIN, "/override/tns");

      assertEquals(
          "/override/tns",
          databaseMeta
              .getConnectionProperties(variables)
              .get(OracleDatabaseMeta.PROPERTY_TNS_ADMIN));
    }
  }

  @Nested
  @DisplayName("Serialization")
  class Serialization {

    @Test
    @DisplayName("every new option survives a round trip")
    void roundTrip() throws Exception {
      meta.setConnectionType(OracleConnectionType.SERVICE_NAME);
      meta.setTnsAdmin("/opt/tns");
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
      meta.setWalletDirectory("/opt/wallet");
      meta.setWalletPassword("walletpw");
      meta.setTrustStoreFile("/opt/truststore.jks");
      meta.setTrustStorePassword("trustpw");
      meta.setTrustStoreType("JKS");
      meta.setKeyStoreFile("/opt/keystore.jks");
      meta.setKeyStorePassword("keypw");
      meta.setKeyStoreType("PKCS12");
      meta.setSslServerDnMatch(false);
      meta.setSslServerCertDn("CN=db.example.com");

      OracleDatabaseMeta loaded = roundTripThroughXml(meta);

      assertEquals(OracleConnectionType.SERVICE_NAME, loaded.getConnectionType());
      assertEquals("/opt/tns", loaded.getTnsAdmin());
      assertTrue(loaded.isUseTcps());
      assertEquals(OracleTlsCredentialType.JKS, loaded.getTlsCredentialType());
      assertEquals("/opt/wallet", loaded.getWalletDirectory());
      assertEquals("/opt/truststore.jks", loaded.getTrustStoreFile());
      assertEquals("JKS", loaded.getTrustStoreType());
      assertEquals("/opt/keystore.jks", loaded.getKeyStoreFile());
      assertEquals("PKCS12", loaded.getKeyStoreType());
      assertFalse(loaded.isSslServerDnMatch());
      assertEquals("CN=db.example.com", loaded.getSslServerCertDn());
    }

    @Test
    @DisplayName("passwords are not stored in the clear")
    void passwordsAreEncrypted() throws Exception {
      meta.setUseTcps(true);
      meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
      meta.setTrustStorePassword(Encr.encryptPasswordIfNotUsingVariables("trustpw"));
      meta.setKeyStorePassword(Encr.encryptPasswordIfNotUsingVariables("keypw"));
      meta.setWalletPassword(Encr.encryptPasswordIfNotUsingVariables("walletpw"));

      String xml = XmlMetadataUtil.serializeObjectToXml(meta);

      assertFalse(xml.contains("trustpw"));
      assertFalse(xml.contains("keypw"));
      assertFalse(xml.contains("walletpw"));

      // ... and still arrive at the driver as the real thing.
      OracleDatabaseMeta loaded = roundTripThroughXml(meta);
      Properties properties = loaded.getConnectionProperties(variables);
      assertEquals("trustpw", properties.get(OracleDatabaseMeta.PROPERTY_TRUST_STORE_PASSWORD));
      assertEquals("keypw", properties.get(OracleDatabaseMeta.PROPERTY_KEY_STORE_PASSWORD));
    }

    @Test
    @DisplayName("XML written before these options existed loads with the old behaviour")
    void olderXmlWithoutTheNewElements() throws Exception {
      Node node = XmlHandler.loadXmlString("<oracle></oracle>", "oracle");
      OracleDatabaseMeta loaded =
          XmlMetadataUtil.deSerializeFromXml(
              node, OracleDatabaseMeta.class, new MemoryMetadataProvider());
      loaded.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);

      assertEquals(OracleConnectionType.AUTOMATIC, loaded.getConnectionType());
      assertEquals(OracleTlsCredentialType.NONE, loaded.getTlsCredentialType());
      assertFalse(loaded.isUseTcps());
      assertEquals("jdbc:oracle:thin:@FOO:1521:BAR", loaded.getURL("FOO", "1521", "BAR"));
      assertTrue(loaded.getConnectionProperties(variables).isEmpty());
    }

    private OracleDatabaseMeta roundTripThroughXml(OracleDatabaseMeta source) throws Exception {
      String xml = "<oracle>" + XmlMetadataUtil.serializeObjectToXml(source) + "</oracle>";
      Node node = XmlHandler.loadXmlString(xml, "oracle");
      OracleDatabaseMeta loaded =
          XmlMetadataUtil.deSerializeFromXml(
              node, OracleDatabaseMeta.class, new MemoryMetadataProvider());
      loaded.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
      return loaded;
    }
  }
}
