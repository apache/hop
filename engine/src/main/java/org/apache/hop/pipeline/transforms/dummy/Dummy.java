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

package org.apache.hop.pipeline.transforms.dummy;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Do nothing. Pass all input data to the next transforms.
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class Dummy extends BaseTransform<DummyMeta, DummyData> implements ITransform<DummyMeta, DummyData> {

  private static final Class<?> PKG = DummyMeta.class; // For Translator

  public Dummy( TransformMeta transformMeta, DummyMeta meta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // get row, set busy!
    // no more input to be expected...
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "Dummy.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }
}
