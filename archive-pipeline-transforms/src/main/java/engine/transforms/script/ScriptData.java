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

package org.apache.hop.pipeline.transforms.script;

import org.apache.hop.compatibility.Value;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class ScriptData extends BaseTransformData implements ITransformData {
  public ScriptEngine cx;
  public Bindings scope;
  public CompiledScript script;

  public int[] fields_used;
  public Value[] values_used;

  public IRowMeta outputRowMeta;
  public int[] replaceIndex;

  public ScriptData() {
    super();
    cx = null;
    fields_used = null;
  }

  public void check( int i ) {
    System.out.println( i );
  }
}
