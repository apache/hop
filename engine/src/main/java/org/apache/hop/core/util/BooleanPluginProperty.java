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

package org.apache.hop.core.util;

import org.apache.hop.core.xml.XMLHandler;
import org.w3c.dom.Node;

import java.util.prefs.Preferences;

/**
 * @author <a href="mailto:thomas.hoedl@aschauer-edv.at">Thomas Hoedl(asc042)</a>
 */
public class BooleanPluginProperty extends KeyValue<Boolean> implements IPluginProperty {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = -2990345692552430357L;

  /**
   * Constructor. Value is null.
   *
   * @param key key to set.
   * @throws IllegalArgumentException if key is invalid.
   */
  public BooleanPluginProperty( final String key ) throws IllegalArgumentException {
    super( key, DEFAULT_BOOLEAN_VALUE );
  }

  /**
   * {@inheritDoc}
   */
  public boolean evaluate() {
    return Boolean.TRUE.equals( this.getValue() );
  }

  /**
   *
   */
  public void appendXml( final StringBuilder builder ) {
    builder.append( XMLHandler.addTagValue( this.getKey(), this.getValue() ) );
  }

  /**
   *
   */
  public void loadXml( final Node node ) {
    final String stringValue = XMLHandler.getTagValue( node, this.getKey() );
    this.setValue( BOOLEAN_STRING_TRUE.equalsIgnoreCase( stringValue ) );
  }

  /**
   *
   */
  public void saveToPreferences( final Preferences node ) {
    node.putBoolean( this.getKey(), this.getValue() );
  }

  /**
   *
   */
  public void readFromPreferences( final Preferences node ) {
    this.setValue( node.getBoolean( this.getKey(), this.getValue() ) );
  }

}
