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

package org.apache.hop.workflow.action.validator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.GenericValidator;
import org.apache.commons.validator.util.ValidatorUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;

import java.util.List;
import java.util.Map;

/**
 * Methods in this class are referenced in validator definitions within the validator resources file (e.g.
 * <tt>validator.xml</tt>).
 *
 * <p>
 * Modeled after <code>org.apache.struts.validator.FieldChecks</code> and
 * <code>org.apache.commons.collections.PredicateUtils</code>.
 * </p>
 *
 * @author mlowery
 */
public class ActionValidatorUtils {

  public static final int LEVEL_FAILURE_DEFAULT = ICheckResult.TYPE_RESULT_ERROR;

  /**
   * Returns a AndValidator singleton.
   */
  public static AndValidator andValidator() {
    return AndValidator.INSTANCE;
  }

  /**
   * Returns a NotBlankValidator singleton.
   */
  public static NotBlankValidator notBlankValidator() {
    return NotBlankValidator.INSTANCE;
  }

  /**
   * Returns a NotNullValidator singleton.
   */
  public static NotNullValidator notNullValidator() {
    return NotNullValidator.INSTANCE;
  }

  /**
   * Returns a FileExistsValidator singleton.
   */
  public static FileExistsValidator fileExistsValidator() {
    return FileExistsValidator.INSTANCE;
  }

  /**
   * Returns a IntegerValidator singleton.
   */
  public static IntegerValidator integerValidator() {
    return IntegerValidator.INSTANCE;
  }

  /**
   * Returns a LongValidator singleton.
   */
  public static LongValidator longValidator() {
    return LongValidator.INSTANCE;
  }

  /**
   * Returns a FileDoesNotExistValidator singleton.
   */
  public static FileDoesNotExistValidator fileDoesNotExistValidator() {
    return FileDoesNotExistValidator.INSTANCE;
  }

  /**
   * Returns a EmailValidator singleton.
   */
  public static EmailValidator emailValidator() {
    return EmailValidator.INSTANCE;
  }

  /**
   * Gets the <code>levelOnFail</code> type for given <code>validatorName</code>. If that is not found, returns generic
   * <code>levelOnFail</code> type. If that fails, returns <code>ICheckResult.TYPE_RESULT_ERROR</code>.
   */
  public static int getLevelOnFail( ValidatorContext context, String validatorName ) {
    final String key = getKeyLevelOnFail( validatorName );
    if ( context.containsKey( key ) ) {
      return (Integer) context.get( key );
    } else if ( context.containsKey( IActionValidator.KEY_LEVEL_ON_FAIL ) ) {
      return (Integer) context.get( IActionValidator.KEY_LEVEL_ON_FAIL );
    } else {
      return ICheckResult.TYPE_RESULT_ERROR;
    }
  }

  public static void putLevelOnFail( Map<String, Object> map, String validatorName, int levelOnFail ) {
    final String key = getKeyLevelOnFail( validatorName );
    map.put( key, levelOnFail );
  }

  public static void putLevelOnFail( Map<String, Object> map, int levelOnFail ) {
    map.put( IActionValidator.KEY_LEVEL_ON_FAIL, levelOnFail );
  }

  public static String getKeyLevelOnFail( String validatorName ) {
    return validatorName + "-" + IActionValidator.KEY_LEVEL_ON_FAIL;
  }

  /**
   * Fails if a field's value does not match the given mask.
   */
  public static boolean validateMask( ICheckResultSource source, String propertyName,
                                      List<ICheckResult> remarks, String mask, int levelOnFail ) {
    // TODO review.  Appears unused, and would cause stackoverflow if it _was_ used.  commenting out.
    //return validateMask( source, propertyName, remarks, mask, LEVEL_FAILURE_DEFAULT );
    throw new UnsupportedOperationException();
  }

  /**
   * Fails if a field's value does not match the given mask.
   */
  public static boolean validateMask( ICheckResultSource source, String propertyName, int levelOnFail,
                                      List<ICheckResult> remarks, String mask ) {
    final String VALIDATOR_NAME = "matches";
    String value = null;

    value = ValidatorUtils.getValueAsString( source, propertyName );

    try {
      if ( null == mask ) {
        addGeneralRemark(
          source, propertyName, VALIDATOR_NAME, remarks, "errors.missingVar",
          ICheckResult.TYPE_RESULT_ERROR );
        return false;
      }

      if ( StringUtils.isNotBlank( value ) && !GenericValidator.matchRegexp( value, mask ) ) {
        addFailureRemark( source, propertyName, VALIDATOR_NAME, remarks, levelOnFail );
        return false;
      } else {
        return true;
      }
    } catch ( Exception e ) {
      addExceptionRemark( source, propertyName, VALIDATOR_NAME, remarks, e );
      return false;
    }
  }

  public static void addFailureRemark( ICheckResultSource source, String propertyName,
                                       String validatorName, List<ICheckResult> remarks, int level ) {
    String key = "messages.failed." + validatorName;
    remarks.add( new CheckResult( level, ValidatorMessages.getString( key, propertyName ), source ) );
  }

  public static void addExceptionRemark( ICheckResultSource source, String propertyName,
                                         String validatorName, List<ICheckResult> remarks, Exception e ) {
    String key = "messages.failed.unableToValidate";
    remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_ERROR, ValidatorMessages.getString(
      key, propertyName, e.getClass().getSimpleName() + ": " + e.getLocalizedMessage() ), source ) );
  }

  public static void addGeneralRemark( ICheckResultSource source, String propertyName,
                                       String validatorName, List<ICheckResult> remarks, String key, int level ) {
    remarks.add( new CheckResult(
      ICheckResult.TYPE_RESULT_ERROR, ValidatorMessages.getString( key ), source ) );
  }

  public static void addOkRemark( ICheckResultSource source, String propertyName,
                                  List<ICheckResult> remarks ) {
    final int SUBSTRING_LENGTH = 20;
    String value = ValidatorUtils.getValueAsString( source, propertyName );
    String substr = null;
    if ( value != null ) {
      substr = value.substring( 0, Math.min( SUBSTRING_LENGTH, value.length() ) );
      if ( value.length() > SUBSTRING_LENGTH ) {
        substr += "...";
      }
    }
    remarks.add( new CheckResult( ICheckResult.TYPE_RESULT_OK, ValidatorMessages.getString(
      "messages.passed", propertyName, substr ), source ) );
  }

}
