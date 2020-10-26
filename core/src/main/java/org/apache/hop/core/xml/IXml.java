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

package org.apache.hop.core.xml;

import org.apache.hop.core.exception.HopException;

/**
 * Implementing classes of this interface know how to express themselves using XML They also can construct themselves
 * using XML.
 *
 * @author Matt
 * @since 29-jan-2004
 */
public interface IXml {
  /**
   * Describes the Object implementing this interface as XML
   *
   * @return the XML string for this object
   * @throws HopException in case there is an encoding problem.
   */
  String getXml() throws HopException;

}
