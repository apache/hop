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

package org.apache.hop.pipeline.transforms.input;

import org.apache.hop.core.exception.HopException;
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
public class MappingInput
  extends BaseTransform<MappingInputMeta, MappingInputData>
  implements ITransform<MappingInputMeta, MappingInputData> {

  private static final Class<?> PKG = MappingInputMeta.class; // For Translator

  public MappingInput( TransformMeta transformMeta, MappingInputMeta meta, MappingInputData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  // ProcessRow is not doing anything
  // It's a place holder for accepting rows from the parent pipeline...
  // So, basically, this is a glorified Dummy with a little bit of meta-data
  //
  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // The Input RowMetadata is not the same as the output row meta-data.
      // The difference is described in the data interface
      //
      // String[] data.sourceFieldname
      // String[] data.targetFieldname
      //
      // --> getInputRowMeta() is not corresponding to what we're outputting.
      // In essence, we need to rename a couple of fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();

      // Fill the output row meta with the processed fields
      // This calculates renames and everything
      //
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    }

    putRow( data.outputRowMeta, row );

    return true;
  }

  public boolean init() {
    return super.init();
  }


}
