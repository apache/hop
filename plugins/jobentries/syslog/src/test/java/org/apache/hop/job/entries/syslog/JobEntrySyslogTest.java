/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.job.entries.syslog;

import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JobEntrySyslogTest extends JobEntryLoadSaveTestSupport<JobEntrySyslog> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntrySyslog> getJobEntryClass() {
    return JobEntrySyslog.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "serverName",
      "port",
      "message",
      "facility",
      "priority",
      "datePattern",
      "addTimestamp",
      "addHostname" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "serverName", "getServerName",
      "port", "getPort",
      "message", "getMessage",
      "facility", "getFacility",
      "priority", "getPriority",
      "datePattern", "getDatePattern",
      "addTimestamp", "isAddTimestamp",
      "addHostname", "isAddHostName" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "serverName", "setServerName",
      "port", "setPort",
      "message", "setMessage",
      "facility", "setFacility",
      "priority", "setPriority",
      "datePattern", "setDatePattern",
      "addTimestamp", "addTimestamp",
      "addHostname", "addHostName" );
  }

}
