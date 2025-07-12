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

/**
 * Context object that controls how transform metadata copying is performed. This allows for
 * different copying strategies and fine-grained control over what state and references are
 * preserved during copying operations.
 */
public class CopyContext {

  /** Whether to preserve the changed state from the original object */
  private final boolean preserveChangedState;

  /** Whether to perform deep copying of nested objects */
  private final boolean deepCopy;

  /** Whether to preserve parent references in the copied object */
  private final boolean preserveParentReferences;

  /** Whether to copy transform-specific error handling metadata */
  private final boolean copyErrorHandling;

  /** Whether to copy partitioning metadata */
  private final boolean copyPartitioning;

  /** Whether to copy attributes map */
  private final boolean copyAttributes;

  /** Default context for standard cloning operations */
  public static final CopyContext DEFAULT =
      new CopyContext.Builder()
          .preserveChangedState(true)
          .deepCopy(true)
          .preserveParentReferences(false)
          .copyErrorHandling(true)
          .copyPartitioning(true)
          .copyAttributes(true)
          .build();

  /** Context for lightweight copying (minimal state preservation) */
  public static final CopyContext LIGHTWEIGHT =
      new CopyContext.Builder()
          .preserveChangedState(false)
          .deepCopy(false)
          .preserveParentReferences(false)
          .copyErrorHandling(false)
          .copyPartitioning(false)
          .copyAttributes(false)
          .build();

  /** Context for same-pipeline copying (preserves references) */
  public static final CopyContext SAME_PIPELINE =
      new CopyContext.Builder()
          .preserveChangedState(true)
          .deepCopy(true)
          .preserveParentReferences(true)
          .copyErrorHandling(true)
          .copyPartitioning(true)
          .copyAttributes(true)
          .build();

  private CopyContext(Builder builder) {
    this.preserveChangedState = builder.preserveChangedState;
    this.deepCopy = builder.deepCopy;
    this.preserveParentReferences = builder.preserveParentReferences;
    this.copyErrorHandling = builder.copyErrorHandling;
    this.copyPartitioning = builder.copyPartitioning;
    this.copyAttributes = builder.copyAttributes;
  }

  public boolean isPreserveChangedState() {
    return preserveChangedState;
  }

  public boolean isDeepCopy() {
    return deepCopy;
  }

  public boolean isPreserveParentReferences() {
    return preserveParentReferences;
  }

  public boolean isCopyErrorHandling() {
    return copyErrorHandling;
  }

  public boolean isCopyPartitioning() {
    return copyPartitioning;
  }

  public boolean isCopyAttributes() {
    return copyAttributes;
  }

  /** Builder for creating CopyContext instances with specific configurations. */
  public static class Builder {
    private boolean preserveChangedState = true;
    private boolean deepCopy = true;
    private boolean preserveParentReferences = false;
    private boolean copyErrorHandling = true;
    private boolean copyPartitioning = true;
    private boolean copyAttributes = true;

    public Builder preserveChangedState(boolean preserveChangedState) {
      this.preserveChangedState = preserveChangedState;
      return this;
    }

    public Builder deepCopy(boolean deepCopy) {
      this.deepCopy = deepCopy;
      return this;
    }

    public Builder preserveParentReferences(boolean preserveParentReferences) {
      this.preserveParentReferences = preserveParentReferences;
      return this;
    }

    public Builder copyErrorHandling(boolean copyErrorHandling) {
      this.copyErrorHandling = copyErrorHandling;
      return this;
    }

    public Builder copyPartitioning(boolean copyPartitioning) {
      this.copyPartitioning = copyPartitioning;
      return this;
    }

    public Builder copyAttributes(boolean copyAttributes) {
      this.copyAttributes = copyAttributes;
      return this;
    }

    public CopyContext build() {
      return new CopyContext(this);
    }
  }

  @Override
  public String toString() {
    return "CopyContext{"
        + "preserveChangedState="
        + preserveChangedState
        + ", deepCopy="
        + deepCopy
        + ", preserveParentReferences="
        + preserveParentReferences
        + ", copyErrorHandling="
        + copyErrorHandling
        + ", copyPartitioning="
        + copyPartitioning
        + ", copyAttributes="
        + copyAttributes
        + '}';
  }
}
