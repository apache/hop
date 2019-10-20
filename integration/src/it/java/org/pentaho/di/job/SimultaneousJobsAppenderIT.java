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

package org.apache.hop.job;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingBuffer;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.parameters.UnknownParamException;

public class SimultaneousJobsAppenderIT {

  private int howMany = 3; // number of simultaneously running jobs
  private int prevJobBuffer = 0;
  private static String jobPath = "one hundred lines.kjb";
  private static String PKG = "org.apache.hop/job/";

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  public void testAppendersBuffer() throws HopXMLException, IOException, URISyntaxException, UnknownParamException {
    Job[] jobs = new Job[howMany];
    for ( int i = 0; i < jobs.length; i++ ) {
      JobMeta jm = new JobMeta( new File( SimultaneousJobsAppenderIT.class.getClassLoader().getResource( PKG + jobPath ).toURI() ).getCanonicalPath(), null );
      jm.setName( "Job number " + i );
      Job job = new Job( null, jm );
      // adjust the log level
      job.setLogLevel( LogLevel.BASIC );
      jobs[i] = job;
    }

    for (Job job : jobs) {
      job.start();
    }

    for (Job job : jobs) {
      job.waitUntilFinished();
    }

    LoggingBuffer appender = HopLogStore.getAppender();

    for (int i = 0; i < jobs.length; i++) {
      if ( prevJobBuffer != 0 ) {
        Assert.assertEquals( "Uncorrect buffer size, job: " + i, prevJobBuffer, appender.getBuffer( jobs[i].getLogChannelId(), false ).length() );
      }
      prevJobBuffer = appender.getBuffer( jobs[i].getLogChannelId(), false ).length();
    }
  }
}