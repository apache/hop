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

package org.apache.hop.pipeline.transforms.valuemapper;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.Hashtable;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class ValueMapperData extends BaseTransformData implements ITransformData {
  public IRowMeta previousMeta;
  public IRowMeta outputMeta;

  public int keynr;

  public Hashtable<String, String> hashtable;

  public int emptyFieldIndex;

  public IValueMeta stringMeta;
  public IValueMeta outputValueMeta;
  public IValueMeta sourceValueMeta;

  public ValueMapperData() {
    super();

    hashtable = null;

    stringMeta = new ValueMetaString( "string" );
  }

}
