/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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
package org.apache.hop.databases.cassandra.util;

/** @author Tatsiana_Kasiankova */
/** A representation of a selector in a selection list of a select clause. */
public class Selector {

  private static final String OPEN_BRACKET = "(";

  private String columnName;

  private String alias;

  private CQLFunctions function;

  private boolean isFunction;

  /** @param columnName */
  public Selector(String columnName) {
    this(columnName, null);
  }

  /**
   * @param columnName
   * @param alias
   */
  public Selector(String columnName, String alias) {
    this(columnName, alias, null);
  }

  /**
   * @param columnName
   * @param alias
   * @param function
   */
  public Selector(String columnName, String alias, String function) {
    super();
    this.columnName = columnName;
    this.alias = alias;
    this.function = CQLFunctions.getFromString(function);
    this.isFunction = this.function != null;
  }

  /**
   * Returns the alias of the selector
   *
   * @return alias the alias of the selector
   */
  public String getAlias() {
    return alias;
  }

  /**
   * Returns the column name for the selector if this is a simple Cassandra column. If the selector
   * is a function, returns the function with arguments and besides the function name will be
   * normalized.
   *
   * @return the column name in the selector
   */
  public String getColumnName() {
    return isFunction()
        ? getNomalizedFunctionName(columnName, getFunction().isCaseSensitive())
        : columnName;
  }

  /**
   * Returns the function for the selector
   *
   * @return function the function for the selector
   */
  public CQLFunctions getFunction() {
    return function;
  }

  /**
   * Indicates if the selector is a function or not
   *
   * @return isFunction the indicator if the selector is a function or not
   */
  public boolean isFunction() {
    return isFunction;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Selector [columnName="
        + columnName
        + ", alias="
        + alias
        + ", function="
        + function
        + ", isFunction="
        + isFunction
        + "]";
  }

  /**
   * Depending on indicator <code>isCaseSensetive</code>
   *
   * <p>Converts the name of the function to lower case if <code>isCaseSensetive = false</code>,
   * nothing changes if <code>isCaseSensetive = true.</code>
   *
   * @param function the function whose name is to be changed
   * @param isCaseSensetive the indicator to define if the name of the function should be processed
   *     as case sensitive or not.
   * @return the function with normalized name
   */
  private String getNomalizedFunctionName(String function, boolean isCaseSensetive) {
    String nName = null;
    if (function != null) {
      nName = isCaseSensetive ? function : function.toLowerCase();
      StringBuffer newName = new StringBuffer(function.length());
      int ind = function.indexOf(OPEN_BRACKET);
      if (ind != -1) {
        nName = function.substring(0, ind).trim();
        nName =
            newName
                .append(isCaseSensetive ? nName : nName.toLowerCase())
                .append(function.substring(ind))
                .toString();
      }
    }
    return nName;
  }
}
