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
package org.apache.hop.core.injection.inheritance;

import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.util.List;

public class MetaBeanParent<T extends MetaBeanParentItem, A> extends BaseTransformMeta implements ITransformMeta<ITransform, ITransformData> {

  @InjectionDeep
  public List<T> items;

  @Injection( name = "A" )
  A obj;

  @InjectionDeep( prefix = "ITEM" )
  public T test1() {
    return null;
  }

  @InjectionDeep( prefix = "SUB" )
  public List<T> test2() {
    return null;
  }




  @Override public void setDefault() {
  }

  @Override public ITransform createTransform( TransformMeta transformMeta, ITransformData iTransformData, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return null;
  }

  @Override public ITransformData getTransformData() {
    return null;
  }
}
