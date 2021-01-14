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

package org.apache.hop.pipeline.transforms;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import javax.servlet.http.HttpServletResponse;

// TODO: refactor this with proper services
@Deprecated
public class PipelineTransformUtil {

  public static void initServletConfig( IPipelineEngine srcPipeline, IPipelineEngine distPipeline ) {

    if (!(srcPipeline instanceof Pipeline)) {
      return;
    }
    if (!(distPipeline instanceof Pipeline)) {
      return;
    }

    Pipeline sPipeline = (Pipeline) srcPipeline;
    Pipeline dPipeline = (Pipeline) distPipeline;

    // Also pass servlet information (if any)
    dPipeline.setServletPrintWriter( sPipeline.getServletPrintWriter() );

    HttpServletResponse response = sPipeline.getServletResponse();
    if ( response != null ) {
      dPipeline.setServletReponse( response );
    }

    dPipeline.setServletRequest( sPipeline.getServletRequest() );
  }

}
