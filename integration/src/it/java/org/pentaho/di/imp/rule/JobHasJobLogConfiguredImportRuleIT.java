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

import junit.framework.TestCase;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.JobLogTable;
import org.apache.hop.core.plugins.ImportRulePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.imp.rules.JobHasJobLogConfiguredImportRule;
import org.apache.hop.job.JobMeta;

import java.util.List;

public class JobHasJobLogConfiguredImportRuleIT extends TestCase {

  @Override
  protected void setUp() throws Exception {
    HopEnvironment.init();
  }

  public void testRule() throws Exception {

    JobMeta jobMeta = new JobMeta();
    DatabaseMeta logDbMeta =
      new DatabaseMeta( "LOGDB", "MYSQL", "JDBC", "localhost", "test", "3306", "foo", "bar" );
    jobMeta.addDatabase( logDbMeta );
    JobLogTable logTable = jobMeta.getJobLogTable();

    PluginRegistry registry = PluginRegistry.getInstance();

    PluginInterface plugin = registry.findPluginWithId( ImportRulePluginType.class, "JobHasJobLogConfigured" );
    assertNotNull(
      "The 'job has job log table configured' rule could not be found in the plugin registry!", plugin );

    JobHasJobLogConfiguredImportRule rule = (JobHasJobLogConfiguredImportRule) registry.loadClass( plugin );
    assertNotNull(
      "The 'job has job log table configured' class could not be loaded by the plugin registry!", plugin );

    rule.setEnabled( true );

    List<ImportValidationFeedback> feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has job log table configured'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected", feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );

    logTable.setTableName( "SCHEMA" );
    logTable.setTableName( "LOGTABLE" );
    logTable.setConnectionName( logDbMeta.getName() );
    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An approval ruling was expected",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.APPROVAL );

    // Make the rules stricter!
    //
    rule.setTableName( "SCHEMA" );
    rule.setTableName( "LOGTABLE" );
    rule.setConnectionName( logDbMeta.getName() );
    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An approval ruling was expected",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.APPROVAL );

    // Break the rule
    //
    rule.setSchemaName( "INCORRECT_SCHEMA" );
    rule.setTableName( "LOGTABLE" );
    rule.setConnectionName( logDbMeta.getName() );
    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected", feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );

    rule.setSchemaName( "SCHEMA" );
    rule.setTableName( "INCORRECT_LOGTABLE" );
    rule.setConnectionName( logDbMeta.getName() );
    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected", feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );

    rule.setSchemaName( "SCHEMA" );
    rule.setTableName( "LOGTABLE" );
    rule.setConnectionName( "INCORRECT_DATABASE" );
    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has description rule'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected", feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );

    // No feedback expected!
    //
    rule.setEnabled( false );

    feedback = rule.verifyRule( jobMeta );
    assertTrue(
      "We didn't expect any feedback from the 'job has job log table configured' since the rule is not enabled",
      feedback.isEmpty() );
  }
}
