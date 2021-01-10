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

package org.apache.hop.pipeline.transforms.loadsave.initializer;

public interface IInitializer<T> {

  /**
   * Perform in-place modifications to the transformMeta before
   * IFieldLoadSaveValidator classes are called on the transformMeta
   *
   * @param object The transformMeta class
   *
   * @deprecated The transformMeta class should be updated so that
   * developers can instantiate the transformMeta, and immediately
   * call setter methods.  Commonly, this is used for transforms
   * that define an allocate method, which pre-populate
   * empty arrays
   */
  @Deprecated void modify( T object );
}
