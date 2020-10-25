/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.workflow.action.validator;

import org.apache.commons.validator.GenericValidator;
import org.apache.commons.validator.util.ValidatorUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;

import java.util.List;

/**
 * Fails if the field's value is either <code>null</code>, an empty string, or a string containing only whitespace.
 *
 * @author mlowery
 */
public class NotBlankValidator implements IActionValidator {

  public static final NotBlankValidator INSTANCE = new NotBlankValidator();

  private static final String VALIDATOR_NAME = "notBlank";

  public boolean validate( ICheckResultSource source, String propertyName,
                           List<ICheckResult> remarks, ValidatorContext context ) {
    String value = ValidatorUtils.getValueAsString( source, propertyName );
    if ( GenericValidator.isBlankOrNull( value ) ) {
      ActionValidatorUtils.addFailureRemark(
        source, propertyName, VALIDATOR_NAME, remarks, ActionValidatorUtils.getLevelOnFail(
          context, VALIDATOR_NAME ) );
      return false;
    } else {
      return true;
    }
  }

  public String getName() {
    return VALIDATOR_NAME;
  }

}
