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

package org.apache.hop.core.xml;

import org.w3c.dom.Node;

/**
 * This is an entry in an XmlHandlerCache
 *
 * @author Matt
 * @since 22-Apr-2006
 */
public class XMlHandlerCacheEntry {
  private Node parentNode;
  private String tag;

  /**
   * @param parentNode The parent node
   * @param tag        The tag
   */
  public XMlHandlerCacheEntry( Node parentNode, String tag ) {
    this.parentNode = parentNode;
    this.tag = tag;
  }

  /**
   * @return Returns the parentNode.
   */
  public Node getParentNode() {
    return parentNode;
  }

  /**
   * @param parentNode The parentNode to set.
   */
  public void setParentNode( Node parentNode ) {
    this.parentNode = parentNode;
  }

  /**
   * @return Returns the tag.
   */
  public String getTag() {
    return tag;
  }

  /**
   * @param tag The tag to set.
   */
  public void setTag( String tag ) {
    this.tag = tag;
  }

  @Override
  public boolean equals( Object object ) {
    if ( this == object ) {
      return true;
    }
    if ( object == null || getClass() != object.getClass() ) {
      return false;
    }
    XMlHandlerCacheEntry entry = (XMlHandlerCacheEntry) object;

    return parentNode.equals( entry.getParentNode() ) && tag.equals( entry.getTag() );
  }

  @Override
  public int hashCode() {
    return parentNode.hashCode() ^ tag.hashCode();
  }

}
