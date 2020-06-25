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

package org.apache.hop.core.variables;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is an implementation of IVariables
 *
 * @author Sven Boden
 */
public class Variables implements IVariables {
  private Map<String, String> properties;

  private IVariables parent;

  private Map<String, String> injection;

  private boolean initialized;

  public Variables() {
    properties = Collections.synchronizedMap(new HashMap<>() );
    parent = null;
    injection = null;
    initialized = false;
  }

  @Override
  public void copyVariablesFrom( IVariables variables ) {
    if ( variables != null && this != variables ) {
      // If space is not null and this variable is not already
      // the same object as the argument.
      String[] variableNames = variables.listVariables();
      for ( int idx = 0; idx < variableNames.length; idx++ ) {
        properties.put( variableNames[ idx ], variables.getVariable( variableNames[ idx ] ) );
      }
    }
  }

  @Override
  public IVariables getParentVariableSpace() {
    return parent;
  }

  @Override
  public void setParentVariableSpace( IVariables parent ) {
    this.parent = parent;
  }

  @Override
  public String getVariable( String variableName, String defaultValue ) {
    String var = properties.get( variableName );
    if ( var == null ) {
      return defaultValue;
    }
    return var;
  }

  @Override
  public String getVariable( String variableName ) {
    return properties.get( variableName );
  }

  @Override
  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaBase.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  @Override
  public void initializeVariablesFrom( IVariables parent ) {
    this.parent = parent;

    // Clone the system properties to avoid ConcurrentModificationException while iterating
    // and then add all of them to properties variable.
    //
    Set<String> systemPropertiesNames = System.getProperties().stringPropertyNames();
    for ( String key : systemPropertiesNames ) {
      getProperties().put( key, System.getProperties().getProperty( key ) );
    }

    List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
    for ( DescribedVariable describedVariable : describedVariables ) {
      getProperties().put( describedVariable.getName(), describedVariable.getValue());
    }

    if ( parent != null ) {
      copyVariablesFrom( parent );
    }
    if ( injection != null ) {
      getProperties().putAll( injection );
      injection = null;
    }
    initialized = true;
  }

  @Override
  public String[] listVariables() {
    Set<String> keySet = properties.keySet();
    return keySet.toArray( new String[ 0 ] );
  }

  @Override
  public synchronized void setVariable( String variableName, String variableValue ) {
    if ( variableValue != null ) {
      properties.put( variableName, variableValue );
    } else {
      properties.remove( variableName );
    }
  }

  @Override
  public synchronized String environmentSubstitute( String aString ) {
    if ( aString == null || aString.length() == 0 ) {
      return aString;
    }

    return StringUtil.environmentSubstitute( aString, properties );
  }

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
  @Override
  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData )
    throws HopValueException {
    if ( aString == null || aString.length() == 0 ) {
      return aString;
    }

    return StringUtil.substituteField( aString, rowMeta, rowData );
  }

  @Override
  public String[] environmentSubstitute( String[] string ) {
    String[] retval = new String[ string.length ];
    for ( int i = 0; i < string.length; i++ ) {
      retval[ i ] = environmentSubstitute( string[ i ] );
    }
    return retval;
  }

  @Override
  public void shareVariablesWith( IVariables variables ) {
    // not implemented in here... done by pointing to the same IVariables
    // implementation
  }

  @Override
  public void injectVariables( Map<String, String> prop ) {
    if ( initialized ) {
      // variables are already initialized
      if ( prop != null ) {
        for ( String key : prop.keySet() ) {
          String value = prop.get( key );
          if ( !Utils.isEmpty( key ) ) {
            properties.put( key, Const.NVL( value, "" ) );
          }
        }
        injection = null;
      }
    } else {
      // We have our own personal copy, so changes afterwards
      // to the input properties don't affect us.
      injection = new Hashtable<String, String>();
      for ( String key : prop.keySet() ) {
        String value = prop.get( key );
        if ( !Utils.isEmpty( key ) ) {
          injection.put( key, Const.NVL( value, "" ) );
        }
      }
    }
  }

  /**
   * Get a default variable space as a placeholder. Every time you will get a new instance.
   *
   * @return a default variable space.
   */
  public static synchronized IVariables getADefaultVariableSpace() {
    IVariables variables = new Variables();

    variables.initializeVariablesFrom( null );

    return variables;
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  Map<String, String> getProperties() {
    return properties;
  }

}
