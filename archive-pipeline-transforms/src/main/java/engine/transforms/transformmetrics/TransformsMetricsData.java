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

package org.apache.hop.pipeline.transforms.transformsmetrics;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Samatar
 * @since 16-06-2008
 */
public class TransformsMetricsData extends BaseTransformData implements ITransformData {

  boolean continueLoop;
  public ConcurrentHashMap<Integer, ITransform> transformInterfaces;
  /**
   * The metadata we send out
   */
  public IRowMeta outputRowMeta;

  public String realTransformNameField;
  public String realTransformIdField;
  public String realTransformLinesInputField;
  public String realTransformLinesOutputField;
  public String realTransformLinesReadField;
  public String realTransformLinesUpdatedField;
  public String realTransformLinesWrittenField;
  public String realTransformLinesErrorsField;
  public String realTransformSecondsField;

  public TransformsMetricsData() {
    super();
    continueLoop = true;

    realTransformNameField = null;
    realTransformIdField = null;
    realTransformLinesInputField = null;
    realTransformLinesOutputField = null;
    realTransformLinesReadField = null;
    realTransformLinesUpdatedField = null;
    realTransformLinesWrittenField = null;
    realTransformLinesErrorsField = null;
    realTransformSecondsField = null;
  }
}
