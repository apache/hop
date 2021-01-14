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

import org.apache.hop.core.exception.HopException;

import java.util.List;

/**
 * This class assigns numbers into ranges
 *
 */
public class NumberRangeSet {

  public static final String MULTI_VALUE_SEPARATOR = ",";

  /**
   * List of all rules that have to be considered
   */
  private List<NumberRangeRule> rules;

  /**
   * Value that is returned if no rule matches
   */
  private String fallBackValue;

  public NumberRangeSet( List<NumberRangeRule> rules, String fallBackValue ) {
    this.rules = rules;
    this.fallBackValue = fallBackValue;
  }

  /**
   * Evaluates a value against all rules
   */
  protected String evaluateDouble( double value ) {
    StringBuilder result = new StringBuilder();

    // Execute all rules
    for ( NumberRangeRule rule : rules ) {
      String ruleResult = rule.evaluate( value );

      // If rule matched -> add value to the result
      if ( ruleResult != null ) {
        // Add value separator if multiple values are available
        if ( result.length() > 0 ) {
          result.append( getMultiValueSeparator() );
        }

        result.append( ruleResult );
      }
    }

    return result.toString();
  }

  /**
   * Returns separator that is added if a value matches multiple ranges.
   */
  public static String getMultiValueSeparator() {
    return MULTI_VALUE_SEPARATOR;
  }

  /**
   * Evaluates a value against all rules. Return empty value if input is not numeric.
   */
  public String evaluate( String strValue ) throws HopException {
    if ( strValue != null ) {
      // Try to parse value to double
      try {
        double doubleValue = Double.parseDouble( strValue );
        return evaluate( doubleValue );
      } catch ( Exception e ) {
        throw new HopException( e );
      }
    }
    return fallBackValue;
  }

  /**
   * Evaluates a value against all rules. Return empty value if input is not numeric.
   */
  public String evaluate( Double value ) throws HopException {
    if ( value != null ) {

      String result = evaluateDouble( value );
      if ( !"".equals( result ) ) {
        return result;
      }

    }

    return fallBackValue;
  }

}
