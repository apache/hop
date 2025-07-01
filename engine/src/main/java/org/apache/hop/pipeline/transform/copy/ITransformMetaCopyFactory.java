/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transform.copy;

import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Factory interface for creating copies of transform metadata objects with proper state
 * preservation. This factory provides centralized control over cloning behavior and ensures
 * consistent handling of changed states, references, and other metadata during copying operations.
 */
public interface ITransformMetaCopyFactory {

  /**
   * Creates a complete copy of a TransformMeta object using the default copy context.
   *
   * @param source The source TransformMeta to copy
   * @return A new TransformMeta that is a complete copy of the source
   */
  TransformMeta copy(TransformMeta source);

  /**
   * Creates a copy of a TransformMeta object using the specified copy context.
   *
   * @param source The source TransformMeta to copy
   * @param context The copy context that controls what gets copied and how
   * @return A new TransformMeta that is a copy of the source according to the context
   */
  TransformMeta copy(TransformMeta source, CopyContext context);

  /**
   * Creates a copy of an ITransformMeta object (the inner transform metadata).
   *
   * @param source The source ITransformMeta to copy
   * @return A new ITransformMeta that is a copy of the source
   */
  ITransformMeta copy(ITransformMeta source);

  /**
   * Creates a copy of an ITransformMeta object using the specified copy context.
   *
   * @param source The source ITransformMeta to copy
   * @param context The copy context that controls what gets copied and how
   * @return A new ITransformMeta that is a copy of the source according to the context
   */
  ITransformMeta copy(ITransformMeta source, CopyContext context);
}
