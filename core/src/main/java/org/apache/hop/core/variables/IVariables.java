/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.variables;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;

import java.util.Map;

/**
 * Interface to implement variable sensitive objects.
 *
 * @author Sven Boden
 */
public interface IVariables {
  /**
   * Initialize variable variables using the defaults, copy over the variables from the parent (using copyVariablesFrom()),
   * after this the "injected" variables should be inserted (injectVariables()).
   * <p>
   * The parent is set as parent variable variables.
   *
   * @param parent the parent to start from, or null if root.
   */
  void initializeVariablesFrom( IVariables parent );

  /**
   * Copy the variables from another variables, without initializing with the defaults. This does not affect any parent
   * relationship.
   *
   * @param variables the variables to copy the variables from.
   */
  void copyVariablesFrom( IVariables variables );

  /**
   * Share a variable variables from another variable variables. This means that the object should take over the variables used as
   * argument.
   *
   * @param variables Variable variables to be shared.
   */
  void shareVariablesWith( IVariables variables );

  /**
   * Get the parent of the variable variables.
   *
   * @return the parent.
   */
  IVariables getParentVariableSpace();

  /**
   * Set the parent variable variables
   *
   * @param parent The parent variable variables to set
   */
  void setParentVariableSpace( IVariables parent );

  /**
   * Sets a variable in the Hop Variables list.
   *
   * @param variableName  The name of the variable to set
   * @param variableValue The value of the variable to set. If the variableValue is null, the variable is cleared from the list.
   */
  void setVariable( String variableName, String variableValue );

  /**
   * Get the value of a variable with a default in case the variable is not found.
   *
   * @param variableName The name of the variable
   * @param defaultValue The default value in case the variable could not be found
   * @return the String value of a variable
   */
  String getVariable( String variableName, String defaultValue );

  /**
   * Get the value of a variable.
   *
   * @param variableName The name of the variable
   * @return the String value of a variable or null in case the variable could not be found.
   */
  String getVariable( String variableName );

  /**
   * This method returns a boolean for the new variable check boxes. If the variable name is not set or the variable
   * name is not specified, this method simply returns the default value. If not, it convert the variable value to a
   * boolean. "Y", "YES" and "TRUE" all convert to true. (case insensitive)
   *
   * @param variableName The variable to look up.
   * @param defaultValue The default value to return.
   * @return
   */
  boolean getBooleanValueOfVariable( String variableName, boolean defaultValue );

  /**
   * List the variables (not the values) that are currently in the variable variables.
   *
   * @return Array of String variable names.
   */
  String[] listVariables();

  /**
   * Substitute the string using the current variable variables.
   *
   * @param aString The string to substitute.
   * @return The substituted string.
   */
  String environmentSubstitute( String aString );

  /**
   * Replaces environment variables in an array of strings.
   * <p>
   * See also: environmentSubstitute(String string)
   *
   * @param string The array of strings that wants its variables to be replaced.
   * @return the array with the environment variables replaced.
   */
  public String[] environmentSubstitute( String[] string );

  /**
   * Inject variables. The behaviour should be that the properties object will be stored and at the time the
   * IVariables is initialized (or upon calling this method if the variables is already initialized). After injecting the
   * link of the properties object should be removed.
   *
   * @param prop Properties object containing key-value pairs.
   */
  void injectVariables( Map<String, String> prop );

  /**
   * Substitutes field values in <code>aString</code>. Field values are of the form "?{<field name>}". The values are
   * retrieved from the specified row. Please note that the getString() method is used to convert to a String, for all
   * values in the row.
   *
   * @param aString the string on which to apply the substitution.
   * @param rowMeta The row metadata to use.
   * @param rowData The row data to use
   * @return the string with the substitution applied.
   * @throws HopValueException In case there is a String conversion error
   */
  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData ) throws HopValueException;
}
