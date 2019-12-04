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

package org.apache.hop.imp.rule;

import java.util.List;

import junit.framework.TestCase;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.plugins.ImportRulePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.imp.rules.DatabaseConfigurationImportRule;
import org.apache.hop.trans.TransMeta;

public class DatabaseConfigurationImportRuleIT extends TestCase {

  @Override
  protected void setUp() throws Exception {
    HopEnvironment.init();
  }

  public void testRule() throws Exception {

    // Assemble a new database.
    //
    String DBNAME = "test";
    String HOSTNAME = "localhost";
    String PORT = "3306";
    String USERNAME = "foo";
    String PASSWORD = "bar";
    DatabaseMeta verifyMeta =
      new DatabaseMeta( "LOGDB", "MYSQL", "JDBC", HOSTNAME, DBNAME, PORT, USERNAME, PASSWORD );

    // Create a transformation to test.
    //
    TransMeta transMeta = new TransMeta();
    transMeta.addDatabase( (DatabaseMeta) verifyMeta.clone() );

    // Load the plugin to test from the registry.
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface plugin = registry.findPluginWithId( ImportRulePluginType.class, "DatabaseConfiguration" );
    assertNotNull( "The 'database configuration' rule could not be found in the plugin registry!", plugin );
    DatabaseConfigurationImportRule rule = (DatabaseConfigurationImportRule) registry.loadClass( plugin );
    assertNotNull( "The 'database configuration' class could not be loaded by the plugin registry!", plugin );

    // Set the appropriate rule..
    //
    rule.setEnabled( true );

    List<ImportValidationFeedback> feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'database configuration'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected", feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );

    rule.setDatabaseMeta( verifyMeta );

    feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'transformation has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An approval ruling was expected",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.APPROVAL );

    // Create some errors...
    //
    verifyMeta.setDBName( "incorrect-test" );
    feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'transformation has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected validating the db name",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );
    verifyMeta.setDBName( DBNAME );

    verifyMeta.setHostname( "incorrect-hostname" );
    feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'transformation has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected validating the db hostname",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );
    verifyMeta.setHostname( HOSTNAME );

    verifyMeta.setPort( "incorrect-port" );
    feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'transformation has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected validating the db port",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );
    verifyMeta.setPort( PORT );

    verifyMeta.setUsername( "incorrect-username" );
    feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'transformation has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected validating the db username",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );
    verifyMeta.setUsername( USERNAME );

    verifyMeta.setPassword( "incorrect-password" );
    feedback = rule.verifyRule( transMeta );
    assertTrue( "We didn't get any feedback from the 'transformation has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected validating the db password",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );
    verifyMeta.setPassword( PASSWORD );

    // No feedback expected!
    //
    rule.setEnabled( false );

    feedback = rule.verifyRule( transMeta );
    assertTrue(
      "We didn't expect any feedback from the 'transformation has trans log table configured' since disabled",
      feedback.isEmpty() );
  }
}
