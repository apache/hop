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

package org.apache.hop.pipeline.transforms.numberrange;

import java.util.Objects;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Contains one rule for a number range */
public class NumberRangeRule {

  /** Lower bound for which the rule matches (lowerBound <= x) */
  @HopMetadataProperty(
      key = "lower_bound",
      injectionKey = "LOWER_BOUND",
      injectionKeyDescription = "NumberRangeMeta.Injection.LOWER_BOUND")
  private String lowerBound;

  /** Upper bound for which the rule matches (x < upperBound) */
  @HopMetadataProperty(
      key = "upper_bound",
      injectionKey = "UPPER_BOUND",
      injectionKeyDescription = "NumberRangeMeta.Injection.UPPER_BOUND")
  private String upperBound;

  /** Value that is returned if the number to be tested is within the range */
  @HopMetadataProperty(
      key = "value",
      injectionKey = "VALUE",
      injectionKeyDescription = "NumberRangeMeta.Injection.VALUE")
  private String value;

  private double lowerBoundValue;
  private double upperBoundValue;

  public NumberRangeRule() {}

  public NumberRangeRule(String lowerBound, String upperBound, String value) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.value = value;
  }

  public void init() {
    try {
      // Empty value is equal to minimal possible value
      if (Utils.isEmpty(this.lowerBound)) this.lowerBoundValue = -Double.MAX_VALUE;
      else this.lowerBoundValue = Double.parseDouble(lowerBound);

      // Empty value is equal to maximal possible value
      if (Utils.isEmpty(this.upperBound)) this.upperBoundValue = Double.MAX_VALUE;
      else this.upperBoundValue = Double.parseDouble(upperBound);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Bounds of this rule are not numeric: lowerBound="
              + lowerBound
              + ", upperBound="
              + upperBound
              + ", value="
              + value,
          e);
    }
  }

  /**
   * Evaluates if the current value is within the range. If so, it returns the value. Otherwise it
   * returns null.
   */
  public String evaluate(double compareValue) {
    // Check if the value is within the range
    if ((compareValue >= lowerBoundValue) && (compareValue < upperBoundValue)) {
      return value;
    }

    // Default value is null
    return null;
  }

  public void setLowerBound(String value) {
    this.lowerBound = value;
  }

  public String getLowerBound() {
    return lowerBound;
  }

  public void setUpperBound(String value) {
    this.upperBound = value;
    ;
  }

  public String getUpperBound() {
    return upperBound;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    } else {
      NumberRangeRule target = (NumberRangeRule) obj;
      return getLowerBound().equals(target.getLowerBound())
          && getUpperBound().equals(target.getUpperBound())
          && getValue().equals(target.getValue());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(lowerBound, upperBound, value);
  }
}
