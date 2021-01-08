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

package org.apache.hop.pipeline.engine;

public class PipelineEngineCapabilities {

  private boolean supportingPreview;
  private boolean supportingDebug;
  private boolean supportingSniffing;
  private boolean supportingPause;

  public PipelineEngineCapabilities() {
  }

  public PipelineEngineCapabilities( boolean supportingPreview, boolean supportingDebug, boolean supportingSniffing, boolean supportingPause ) {
    this.supportingPreview = supportingPreview;
    this.supportingDebug = supportingDebug;
    this.supportingSniffing = supportingSniffing;
    this.supportingPause = supportingPause;
  }

  /**
   * Gets supportingPreview
   *
   * @return value of supportingPreview
   */
  public boolean isSupportingPreview() {
    return supportingPreview;
  }

  /**
   * @param supportingPreview The supportingPreview to set
   */
  public void setSupportingPreview( boolean supportingPreview ) {
    this.supportingPreview = supportingPreview;
  }

  /**
   * Gets supportingDebug
   *
   * @return value of supportingDebug
   */
  public boolean isSupportingDebug() {
    return supportingDebug;
  }

  /**
   * @param supportingDebug The supportingDebug to set
   */
  public void setSupportingDebug( boolean supportingDebug ) {
    this.supportingDebug = supportingDebug;
  }

  /**
   * Gets supportingSniffing
   *
   * @return value of supportingSniffing
   */
  public boolean isSupportingSniffing() {
    return supportingSniffing;
  }

  /**
   * @param supportingSniffing The supportingSniffing to set
   */
  public void setSupportingSniffing( boolean supportingSniffing ) {
    this.supportingSniffing = supportingSniffing;
  }

  /**
   * Gets supportingPause
   *
   * @return value of supportingPause
   */
  public boolean isSupportingPause() {
    return supportingPause;
  }

  /**
   * @param supportingPause The supportingPause to set
   */
  public void setSupportingPause( boolean supportingPause ) {
    this.supportingPause = supportingPause;
  }
}
