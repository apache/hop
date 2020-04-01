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

package org.apache.hop.pipeline.steps;

import org.apache.hop.pipeline.Pipeline;

import javax.servlet.http.HttpServletResponse;

public class PipelineStepUtil {

  public static void initServletConfig( Pipeline srcPipeline, Pipeline distPipeline ) {
    // Also pass servlet information (if any)
    distPipeline.setServletPrintWriter( srcPipeline.getServletPrintWriter() );

    HttpServletResponse response = srcPipeline.getServletResponse();
    if ( response != null ) {
      distPipeline.setServletReponse( response );
    }

    distPipeline.setServletRequest( srcPipeline.getServletRequest() );
  }

}
