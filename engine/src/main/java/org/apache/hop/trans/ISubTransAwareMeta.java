/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans;

import org.apache.hop.core.ObjectLocationSpecificationMethod;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.trans.step.StepMeta;

/**
 * A step "meta" that contains a reference to a sub-transformation.
 */
public interface ISubTransAwareMeta {

  ObjectLocationSpecificationMethod getSpecificationMethod();

  String getFileName();

  String getDirectoryPath();

  String getTransName();

  ObjectId getTransObjectId();

  StepMeta getParentStepMeta();

}
