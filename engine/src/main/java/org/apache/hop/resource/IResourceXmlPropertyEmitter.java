/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.resource;

public interface IResourceXmlPropertyEmitter {

  /**
   * Allows injection of additional relevant properties in the to-xml of the Resource Reference.
   *
   * @param ref       The Resource Reference Holder (a transform, or a action)
   * @param indention If -1, then no indenting, otherwise, it's the indent level to indent the XML strings
   * @return String of injected XML
   */
  String getExtraResourceProperties( IResourceHolder ref, int indention );

}
