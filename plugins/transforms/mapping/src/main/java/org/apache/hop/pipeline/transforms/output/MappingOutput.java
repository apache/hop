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

package org.apache.hop.pipeline.transforms.output;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Do nothing. Pass all input data to the next transforms.
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class MappingOutput
  extends BaseTransform<MappingOutputMeta, MappingOutputData>
  implements ITransform<MappingOutputMeta, MappingOutputData> {
  private static final Class<?> PKG = MappingOutputMeta.class; // For Translator

  public MappingOutput( TransformMeta transformMeta, MappingOutputMeta meta, MappingOutputData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // No more input to be expected... Tell the next transforms.
      //
      setOutputDone();
      return false;
    }

    // Copy row to possible alternate rowset(s).
    // Rowsets where added for all the possible targets in the setter for data.targetTransforms...
    //
    putRow( getInputRowMeta(), r );

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "MappingOutput.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

}
