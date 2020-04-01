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

package org.apache.hop.core.listeners;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.job.Job;
import org.apache.hop.pipeline.Pipeline;

public class SubComponentExecutionAdapter implements SubComponentExecutionListener {

  @Override
  public void beforePipelineExecution( Pipeline pipeline ) throws HopException {
  }

  @Override
  public void afterPipelineExecution( Pipeline pipeline ) throws HopException {
  }

  @Override
  public void beforeJobExecution( Job job ) throws HopException {
  }

  @Override
  public void afterJobExecution( Job job ) throws HopException {
  }

}
