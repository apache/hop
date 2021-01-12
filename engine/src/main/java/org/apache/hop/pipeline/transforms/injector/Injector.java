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

package org.apache.hop.pipeline.transforms.injector;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Executor class to allow a java program to inject rows of data into a pipeline. This transform can be used as a
 * starting point in such a "headless" pipeline.
 *
 * @since 22-jun-2006
 */
public class Injector extends BaseTransform<InjectorMeta, InjectorData> implements ITransform<InjectorMeta, InjectorData> {

  private static final Class<?> PKG = InjectorMeta.class; // For Translator

  public Injector( TransformMeta transformMeta, InjectorMeta meta, InjectorData data, int copyNr, PipelineMeta pipelineMeta,
                   Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {
    // Get a row from the previous transform OR from an extra IRowSet
    //
    Object[] row = getRow();

    // Nothing more to be had from any input rowset
    //
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    putRow( getInputRowMeta(), row ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "Injector.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }
}
