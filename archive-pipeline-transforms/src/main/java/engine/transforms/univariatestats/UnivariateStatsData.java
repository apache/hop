/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.univariatestats;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Holds temporary data and has routines for computing derived statistics.
 *
 * @author Mark Hall (mhall{[at]}pentaho.org)
 * @version 1.0
 */
public class UnivariateStatsData extends BaseTransformData implements ITransformData {

  // this class contains intermediate results,
  // info about the input format, derived output
  // format etc.

  // the input data format
  protected IRowMeta mInputRowMeta;

  // the output data format
  protected IRowMeta mOutputRowMeta;

  /**
   * contains the FieldIndexs - one for each UnivariateStatsMetaFunction
   */
  protected FieldIndex[] mIndexes;

  /**
   * Creates a new <code>UnivariateStatsData</code> instance.
   */
  public UnivariateStatsData() {
    super();
  }

  /**
   * Set the FieldIndexes
   *
   * @param fis a <code>FieldIndex[]</code> value
   */
  public void setFieldIndexes( FieldIndex[] fis ) {
    mIndexes = fis;
  }

  /**
   * Get the fieldIndexes
   *
   * @return a <code>FieldIndex[]</code> value
   */
  public FieldIndex[] getFieldIndexes() {
    return mIndexes;
  }

  /**
   * Get the meta data for the input format
   *
   * @return a <code>IRowMeta</code> value
   */
  public IRowMeta getInputRowMeta() {
    return mInputRowMeta;
  }

  /**
   * Save the meta data for the input format. (I'm not sure that this is really needed)
   *
   * @param rmi a <code>IRowMeta</code> value
   */
  public void setInputRowMeta( IRowMeta rmi ) {
    mInputRowMeta = rmi;
  }

  /**
   * Get the meta data for the output format
   *
   * @return a <code>IRowMeta</code> value
   */
  public IRowMeta getOutputRowMeta() {
    return mOutputRowMeta;
  }

  /**
   * Set the meta data for the output format
   *
   * @param rmi a <code>IRowMeta</code> value
   */
  public void setOutputRowMeta( IRowMeta rmi ) {
    mOutputRowMeta = rmi;
  }
}
