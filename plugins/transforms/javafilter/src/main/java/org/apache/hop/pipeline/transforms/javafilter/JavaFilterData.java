/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.javafilter;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.codehaus.janino.ExpressionEvaluator;


import java.util.List;

/**
 * @author Matt
 * @since 8-sep-2005
 */
public class JavaFilterData extends BaseTransformData implements ITransformData {
  public static final int RETURN_TYPE_STRING = 0;
  public static final int RETURN_TYPE_NUMBER = 1;
  public static final int RETURN_TYPE_INTEGER = 2;
  public static final int RETURN_TYPE_LONG = 3;
  public static final int RETURN_TYPE_DATE = 4;
  public static final int RETURN_TYPE_BIGDECIMAL = 5;
  public static final int RETURN_TYPE_BYTE_ARRAY = 6;
  public static final int RETURN_TYPE_BOOLEAN = 7;

  public IRowMeta outputRowMeta;
  public int[] returnType;
  public int[] replaceIndex;

  public ExpressionEvaluator expressionEvaluator;
  public List<Integer> argumentIndexes;
  public String trueTransformName;
  public String falseTransformName;
  public boolean chosesTargetTransforms;
  public IRowSet trueRowSet;
  public IRowSet falseRowSet;
  public Object[] argumentData;

  public JavaFilterData() {
    super();
  }

}
