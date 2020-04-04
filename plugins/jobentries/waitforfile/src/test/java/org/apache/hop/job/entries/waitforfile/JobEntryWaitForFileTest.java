/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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
package org.apache.hop.job.entries.waitforfile;

import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JobEntryWaitForFileTest extends JobEntryLoadSaveTestSupport<JobEntryWaitForFile> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntryWaitForFile> getJobEntryClass() {
    return JobEntryWaitForFile.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "filename",
      "maximumTimeout",
      "checkCycleTime",
      "successOnTimeout",
      "fileSizeCheck",
      "addFilenameToResult" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "filename", "getFilename",
      "maximumTimeout", "getMaximumTimeout",
      "checkCycleTime", "getCheckCycleTime",
      "successOnTimeout", "isSuccessOnTimeout",
      "fileSizeCheck", "isFileSizeCheck",
      "addFilenameToResult", "isAddFilenameToResult" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "filename", "setFilename",
      "maximumTimeout", "setMaximumTimeout",
      "checkCycleTime", "setCheckCycleTime",
      "successOnTimeout", "setSuccessOnTimeout",
      "fileSizeCheck", "setFileSizeCheck",
      "addFilenameToResult", "setAddFilenameToResult" );
  }

}
