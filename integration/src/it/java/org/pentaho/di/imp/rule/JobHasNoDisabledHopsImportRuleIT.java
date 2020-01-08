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
import org.apache.hop.core.plugins.ImportRulePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.imp.rules.JobHasNoDisabledHopsImportRule;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.special.JobEntrySpecial;
import org.apache.hop.job.entry.JobEntryCopy;

import java.util.List;

public class JobHasNoDisabledHopsImportRuleIT extends TestCase {

  @Override
  protected void setUp() throws Exception {
    HopEnvironment.init();
  }

  public void testRule() throws Exception {

    // Create a job to test.
    //
    JobMeta jobMeta = new JobMeta();

    // Add 3 dummy steps connected with hops.
    //
    JobEntryCopy lastCopy = null;
    for ( int i = 0; i < 3; i++ ) {
      JobEntrySpecial dummy = new JobEntrySpecial();
      dummy.setDummy( true );
      dummy.setName( "dummy" + ( i + 1 ) );

      JobEntryCopy copy = new JobEntryCopy( dummy );
      copy.setLocation( 50 + i * 50, 50 );
      copy.setDrawn();
      jobMeta.addJobEntry( copy );

      if ( lastCopy != null ) {
        JobHopMeta hop = new JobHopMeta( lastCopy, copy );
        jobMeta.addJobHop( hop );
      }
      lastCopy = copy;
    }

    // Load the plugin to test from the registry.
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface plugin = registry.findPluginWithId( ImportRulePluginType.class, "JobHasNoDisabledHops" );
    assertNotNull( "The 'job has no disabled hops' rule could not be found in the plugin registry!", plugin );

    JobHasNoDisabledHopsImportRule rule = (JobHasNoDisabledHopsImportRule) registry.loadClass( plugin );
    assertNotNull( "The 'job has no disabled hops' class could not be loaded by the plugin registry!", plugin );

    rule.setEnabled( true );

    List<ImportValidationFeedback> feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has no disabled hops'", !feedback.isEmpty() );
    assertTrue(
      "An approval ruling was expected",
      feedback.get( 0 ).getResultType() == ImportValidationResultType.APPROVAL );

    jobMeta.getJobHop( 0 ).setEnabled( false );

    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't get any feedback from the 'job has no disabled hops'", !feedback.isEmpty() );
    assertTrue(
      "An error ruling was expected", feedback.get( 0 ).getResultType() == ImportValidationResultType.ERROR );

    rule.setEnabled( false );

    feedback = rule.verifyRule( jobMeta );
    assertTrue( "We didn't expect any feedback from the 'job has no disabled hops' while disabled", feedback
      .isEmpty() );
  }
}
