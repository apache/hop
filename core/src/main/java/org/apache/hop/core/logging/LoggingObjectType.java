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

package org.apache.hop.core.logging;

public enum LoggingObjectType {
  // Runtime...
  //
  PIPELINE, TRANSFORM, JOB, JOBENTRY, DATABASE,

  // Metadata...
  //
  PIPELINE_META, TRANSFORM_META, JOB_META,

  // User Interface...
  //
  HOPUI, TRANSFORM_DIALOG, JOBENTRYDIALOG,

  // Web server + HttpServlet...
  //
  CARTE, SERVLET,

  // General
  //
  GENERAL,
}
