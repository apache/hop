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

package org.apache.hop.pipeline.step;

import org.apache.hop.core.HopAttributeInterface;

import java.util.List;

public interface StepAttributesInterface {
  public HopAttributeInterface findParent( List<HopAttributeInterface> attributes, String parentId );

  public HopAttributeInterface findAttribute( String key );

  public String getXmlCode( String attributeKey );

  public String getDescription( String attributeKey );

  public String getTooltip( String attributeKey );
}
