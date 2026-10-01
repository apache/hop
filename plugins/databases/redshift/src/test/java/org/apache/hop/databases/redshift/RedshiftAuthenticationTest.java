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
package org.apache.hop.databases.redshift;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The authentication modes and the serverless endpoint helper of the Redshift connection. */
class RedshiftAuthenticationTest {

  private static final String SERVERLESS_HOST =
      "workgroup.123456789012.eu-west-1.redshift-serverless.amazonaws.com";

  private RedshiftDatabaseMeta dbMeta;
  private IVariables variables;

  @BeforeAll
  static void initHop() throws Exception {
    // The secret access key goes through Encr, which needs the password encoder plugin.
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    dbMeta = new RedshiftDatabaseMeta();
    dbMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    variables = new Variables();
  }

  // ------------------------------------------------------------------ defaults

  /**
   * The whole point of the defaults: a connection saved before any of this existed has to build the
   * exact same URL and add no properties of its own.
   */
  @Test
  void aConnectionThatSaysNothingBehavesExactlyAsBefore() {
    assertEquals(RedshiftDeploymentType.PROVISIONED, dbMeta.getDeploymentType());
    assertEquals(RedshiftAuthenticationType.DATABASE, dbMeta.getAuthenticationType());

    assertEquals(
        "jdbc:redshift://my-cluster.abc123.eu-west-1.redshift.amazonaws.com:5439/dev",
        dbMeta.getURL("my-cluster.abc123.eu-west-1.redshift.amazonaws.com", "5439", "dev"));
    assertTrue(dbMeta.getConnectionProperties(variables).isEmpty());
  }

  // ------------------------------------------------------------------ the URL

  @Test
  void databaseAuthenticationKeepsThePlainScheme() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.DATABASE);

    assertTrue(dbMeta.getURL("host", "5439", "dev").startsWith("jdbc:redshift://"));
  }

  /** The driver only goes and fetches credentials when the URL says so. */
  @Test
  void everyIamModeSwitchesTheUrlToTheIamScheme() {
    for (RedshiftAuthenticationType type : RedshiftAuthenticationType.values()) {
      dbMeta.setAuthenticationType(type);
      String url = dbMeta.getURL("host", "5439", "dev");
      if (type == RedshiftAuthenticationType.DATABASE) {
        assertFalse(url.startsWith("jdbc:redshift:iam://"), type.name());
      } else {
        assertTrue(url.startsWith("jdbc:redshift:iam://"), type.name());
      }
    }
  }

  // ------------------------------------------------------------------ serverless endpoint

  @Test
  void buildsTheServerlessEndpointFromWorkgroupAccountAndRegion() {
    assertEquals(
        SERVERLESS_HOST,
        RedshiftDatabaseMeta.buildServerlessHostname("workgroup", "123456789012", "eu-west-1"));
  }

  /** With no host name entered, the endpoint is built from the workgroup. */
  @Test
  void aServerlessConnectionBuildsItsOwnHostnameWhenNoneWasEntered() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setWorkgroup("workgroup");
    dbMeta.setAccountId("123456789012");
    dbMeta.setAwsRegion("eu-west-1");

    assertEquals(
        "jdbc:redshift://" + SERVERLESS_HOST + ":5439/dev", dbMeta.getURL("", "5439", "dev"));
  }

  /**
   * getURL() has no variables to resolve with, but DatabaseMeta resolves what it returns, so the
   * pieces have to survive into the URL untouched.
   */
  @Test
  void leavesVariablesInTheEndpointForTheCallerToResolve() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setWorkgroup("${WORKGROUP}");
    dbMeta.setAccountId("${ACCOUNT}");
    dbMeta.setAwsRegion("${REGION}");

    assertEquals(
        "jdbc:redshift://${WORKGROUP}.${ACCOUNT}.${REGION}.redshift-serverless.amazonaws.com:5439/dev",
        dbMeta.getURL("", "5439", "dev"));
  }

  /**
   * A custom domain name or a load balancer in front of the workgroup: the entered host wins over
   * the built one, which is the whole point of still offering the field.
   */
  @Test
  void aServerlessHostnameThatWasEnteredWinsOverTheBuiltOne() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setWorkgroup("workgroup");
    dbMeta.setAccountId("123456789012");
    dbMeta.setAwsRegion("eu-west-1");

    assertEquals(
        "jdbc:redshift://redshift.example.com:5439/dev",
        dbMeta.getURL("redshift.example.com", "5439", "dev"));
  }

  /**
   * And the workgroup still has to reach the driver, because behind a custom name it can no longer
   * work out which workgroup it is talking to.
   */
  @Test
  void stillNamesTheWorkgroupBehindACustomHostname() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_DEFAULT_CHAIN);
    dbMeta.setWorkgroup("workgroup");
    dbMeta.setAccountId("123456789012");
    dbMeta.setAwsRegion("eu-west-1");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("true", properties.get("isServerless"));
    assertEquals("workgroup", properties.get("serverlessWorkGroup"));
    assertEquals("eu-west-1", properties.get("Region"));
  }

  /**
   * The cluster's counterpart of serverlessWorkGroup: behind a custom domain name or a load
   * balancer the driver cannot read the cluster name out of the host name either.
   */
  @Test
  void namesTheClusterForProvisionedIam() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.PROVISIONED);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_DEFAULT_CHAIN);
    dbMeta.setClusterId("my-cluster");

    assertEquals("my-cluster", dbMeta.getConnectionProperties(variables).get("ClusterID"));
  }

  /** A cluster on user and password never asks AWS anything, so it has no cluster to name. */
  @Test
  void namesNoClusterForAProvisionedPasswordConnection() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.PROVISIONED);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.DATABASE);
    dbMeta.setClusterId("my-cluster");

    assertNull(dbMeta.getConnectionProperties(variables).get("ClusterID"));
  }

  /** A workgroup is not a cluster: the two identities must never both go out. */
  @Test
  void namesNoClusterForAServerlessConnection() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_DEFAULT_CHAIN);
    dbMeta.setWorkgroup("workgroup");
    // Left behind by an earlier choice.
    dbMeta.setClusterId("my-cluster");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertNull(properties.get("ClusterID"));
    assertEquals("workgroup", properties.get("serverlessWorkGroup"));
  }

  /** The region says nothing to a plain user and password connection on a cluster. */
  @Test
  void sendsNoRegionForAProvisionedPasswordConnection() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.PROVISIONED);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.DATABASE);
    // Left behind by an earlier choice: it must not travel with the connection.
    dbMeta.setAwsRegion("eu-west-1");

    assertNull(dbMeta.getConnectionProperties(variables).get("Region"));
  }

  /** It does matter for IAM on a cluster: it says which AWS to ask for credentials. */
  @Test
  void sendsTheRegionForProvisionedIamSoCredentialsAreFetchedFromTheRightPlace() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.PROVISIONED);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);
    dbMeta.setAwsRegion("eu-west-1");

    assertEquals("eu-west-1", dbMeta.getConnectionProperties(variables).get("Region"));
  }

  @Test
  void tellsTheDriverItIsServerlessSoItWorksBehindALoadBalancer() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setWorkgroup("workgroup");
    dbMeta.setAccountId("123456789012");
    dbMeta.setAwsRegion("eu-west-1");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("true", properties.get("isServerless"));
    assertEquals("workgroup", properties.get("serverlessWorkGroup"));
    assertEquals("123456789012", properties.get("serverlessAcctId"));
    assertEquals("eu-west-1", properties.get("Region"));
  }

  @Test
  void saysNothingAboutServerlessForAProvisionedCluster() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.PROVISIONED);

    assertNull(dbMeta.getConnectionProperties(variables).get("isServerless"));
  }

  // ------------------------------------------------------------------ credentials

  @Test
  void passesAnAccessKeyAsTheDriverPropertiesTheDriverExpects() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    dbMeta.setAwsAccessKeyId("AKIAEXAMPLE");
    dbMeta.setAwsSecretAccessKey("s3cr3t");
    dbMeta.setAwsSessionToken("token");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("AKIAEXAMPLE", properties.get("AccessKeyID"));
    assertEquals("s3cr3t", properties.get("SecretAccessKey"));
    assertEquals("token", properties.get("SessionToken"));
    assertNull(properties.get("Profile"));
  }

  /** Copying an access key out of the console very easily brings whitespace along with it. */
  @Test
  void trimsWhitespaceAroundTheCredentials() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    dbMeta.setAwsAccessKeyId("  AKIAEXAMPLE  ");
    dbMeta.setAwsSecretAccessKey("s3cr3t\n");
    dbMeta.setAwsRegion(" eu-west-1 ");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("AKIAEXAMPLE", properties.get("AccessKeyID"));
    assertEquals("s3cr3t", properties.get("SecretAccessKey"));
    assertEquals("eu-west-1", properties.get("Region"));
  }

  /** A field holding nothing but spaces is not a credential. */
  @Test
  void treatsAWhitespaceOnlyValueAsUnset() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    dbMeta.setAwsAccessKeyId("AKIAEXAMPLE");
    dbMeta.setAwsSecretAccessKey("s3cr3t");
    dbMeta.setAwsSessionToken("   ");

    assertNull(dbMeta.getConnectionProperties(variables).get("SessionToken"));
  }

  @Test
  void resolvesVariablesInTheCredentials() {
    variables.setVariable("KEY", "AKIAEXAMPLE");
    variables.setVariable("SECRET", "s3cr3t");
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    dbMeta.setAwsAccessKeyId("${KEY}");
    dbMeta.setAwsSecretAccessKey("${SECRET}");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("AKIAEXAMPLE", properties.get("AccessKeyID"));
    assertEquals("s3cr3t", properties.get("SecretAccessKey"));
  }

  @Test
  void passesAProfileByName() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);
    dbMeta.setAwsProfile("integration-test");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("integration-test", properties.get("Profile"));
    assertNull(properties.get("AccessKeyID"));
  }

  /** Saying nothing is precisely what makes the driver fall back to the AWS default chain. */
  @Test
  void passesNoCredentialsAtAllForTheDefaultChain() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_DEFAULT_CHAIN);
    // Left over from a previous choice: it must not leak into the connection.
    dbMeta.setAwsAccessKeyId("AKIAEXAMPLE");
    dbMeta.setAwsProfile("integration-test");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertNull(properties.get("AccessKeyID"));
    assertNull(properties.get("Profile"));
  }

  /** A stale access key must not travel with a plain database connection either. */
  @Test
  void passesNoCredentialsAtAllForDatabaseAuthentication() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.DATABASE);
    dbMeta.setAwsAccessKeyId("AKIAEXAMPLE");
    dbMeta.setAwsProfile("integration-test");
    dbMeta.setDbUser("analyst");

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertNull(properties.get("AccessKeyID"));
    assertNull(properties.get("Profile"));
    assertNull(properties.get("DbUser"));
  }

  // ------------------------------------------------------------------ database user

  @Test
  void fallsBackToTheConnectionUsernameForTheDatabaseUser() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);
    dbMeta.setUsername("analyst");

    assertEquals("analyst", dbMeta.getConnectionProperties(variables).get("DbUser"));
  }

  @Test
  void anExplicitDatabaseUserWinsOverTheConnectionUsername() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);
    dbMeta.setUsername("analyst");
    dbMeta.setDbUser("loader");

    assertEquals("loader", dbMeta.getConnectionProperties(variables).get("DbUser"));
  }

  /**
   * The serverless GetCredentials call takes a database and a workgroup, nothing else. Sending it a
   * database user only invites confusion about which identity the session actually runs as.
   */
  @Test
  void sendsNoDatabaseUserForAServerlessWorkgroup() {
    dbMeta.setDeploymentType(RedshiftDeploymentType.SERVERLESS);
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);
    dbMeta.setAwsProfile("integration-test");
    dbMeta.setUsername("admin");
    dbMeta.setDbUser("loader");
    dbMeta.setDbGroups("analysts");
    dbMeta.setAutoCreate(true);

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertNull(properties.get("DbUser"));
    assertNull(properties.get("DbGroups"));
    assertNull(properties.get("AutoCreate"));
    // The credentials themselves still have to get through.
    assertEquals("integration-test", properties.get("Profile"));
  }

  @Test
  void passesTheGroupsAndTheAutoCreateFlag() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);
    dbMeta.setDbGroups("analysts,loaders");
    dbMeta.setAutoCreate(true);

    Properties properties = dbMeta.getConnectionProperties(variables);

    assertEquals("analysts,loaders", properties.get("DbGroups"));
    assertEquals("true", properties.get("AutoCreate"));
  }

  @Test
  void leavesTheAutoCreateFlagOutWhenItIsOff() {
    dbMeta.setAuthenticationType(RedshiftAuthenticationType.IAM_PROFILE);

    assertNull(dbMeta.getConnectionProperties(variables).get("AutoCreate"));
  }
}
