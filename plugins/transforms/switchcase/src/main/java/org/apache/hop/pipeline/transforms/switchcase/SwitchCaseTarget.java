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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Utility class that contains the case value, the target transform name and the resolved target
 * transform
 */
public class SwitchCaseTarget implements Cloneable {
  /** The value to switch over */
  @HopMetadataProperty(
      key = "value",
      injectionKey = "SWITCH_CASE_TARGET.CASE_VALUE",
      injectionKeyDescription = "SwitchCaseMeta.Injection.CASE_VALUE")
  private String caseValue;

  /** The case target transform name (only used during serialization) */
  @HopMetadataProperty(
      key = "target_transform",
      injectionKey = "SWITCH_CASE_TARGET.CASE_TARGET_TRANSFORM_NAME",
      injectionKeyDescription = "SwitchCaseMeta.Injection.CASE_TARGET_TRANSFORM_NAME")
  private String caseTargetTransformName;

  public SwitchCaseTarget() {}

  public SwitchCaseTarget(SwitchCaseTarget t) {
    this.caseValue = t.caseValue;
    this.caseTargetTransformName = t.caseTargetTransformName;
  }

  @Override
  public SwitchCaseTarget clone() {
    return new SwitchCaseTarget(this);
  }

  /**
   * Gets caseValue
   *
   * @return value of caseValue
   */
  public String getCaseValue() {
    return caseValue;
  }

  /** @param caseValue The caseValue to set */
  public void setCaseValue(String caseValue) {
    this.caseValue = caseValue;
  }

  /**
   * Gets caseTargetTransformName
   *
   * @return value of caseTargetTransformName
   */
  public String getCaseTargetTransformName() {
    return caseTargetTransformName;
  }

  /** @param caseTargetTransformName The caseTargetTransformName to set */
  public void setCaseTargetTransformName(String caseTargetTransformName) {
    this.caseTargetTransformName = caseTargetTransformName;
  }
}
