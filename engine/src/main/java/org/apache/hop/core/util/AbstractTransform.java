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

package org.apache.hop.core.util;

import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

public abstract class AbstractTransform<Meta extends ITransformMeta, Data extends ITransformData> extends BaseTransform<Meta, Data> {

  /**
   * Constant for unexpected error.
   */
  public static final String UNEXPECTED_ERROR = "Unexpected error";

  public static final long DEFAULT_ERROR_CODE = 1L;

  /**
   * Constructor.
   *
   * @param transformMeta the transformMeta.
   * @param meta the transform specific metadata
   * @param data the transform data.
   * @param copyNr            the copyNr.
   * @param pipelineMeta         the pipelineMeta.
   * @param pipeline             the transaction.
   */
  public AbstractTransform( final TransformMeta transformMeta, final Meta meta, final Data data, final int copyNr,
                            final PipelineMeta pipelineMeta, final Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Log exception.
   *
   * @param exception exception to log.
   */
  public void logUnexpectedError( final Throwable exception ) {
    this.logError( UNEXPECTED_ERROR, exception );
  }

  /**
   * Set default error code.
   */
  public void setDefaultError() {
    this.setErrors( DEFAULT_ERROR_CODE );
  }

}
