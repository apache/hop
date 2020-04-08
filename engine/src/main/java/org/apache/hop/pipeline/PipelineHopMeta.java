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

package org.apache.hop.pipeline;

import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Objects;

/*
 * Created on 19-jun-2003
 *
 */

/**
 * Defines a link between 2 transforms in a pipeline
 */
public class PipelineHopMeta extends BaseHopMeta<TransformMeta> implements Comparable<PipelineHopMeta> {
  private static Class<?> PKG = Pipeline.class; // for i18n purposes, needed by Translator!!

  public static final String XML_HOP_TAG = "hop";
  public static final String XML_FROM_TAG = "from";
  public static final String XML_TO_TAG = "to";

  public PipelineHopMeta( TransformMeta from, TransformMeta to, boolean en ) {
    this.from = from;
    this.to = to;
    enabled = en;
  }

  public PipelineHopMeta( TransformMeta from, TransformMeta to ) {
    this.from = from;
    this.to = to;
    enabled = true;
  }

  public PipelineHopMeta() {
    this( null, null, false );
  }

  public PipelineHopMeta( Node hopnode, List<TransformMeta> transforms ) throws HopXmlException {
    try {
      this.from = searchTransform( transforms, XmlHandler.getTagValue( hopnode, PipelineHopMeta.XML_FROM_TAG ) );
      this.to = searchTransform( transforms, XmlHandler.getTagValue( hopnode, PipelineHopMeta.XML_TO_TAG ) );
      String en = XmlHandler.getTagValue( hopnode, "enabled" );

      if ( en == null ) {
        enabled = true;
      } else {
        enabled = en.equalsIgnoreCase( "Y" );
      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "PipelineHopMeta.Exception.UnableToLoadHopInfo" ), e );
    }
  }

  public void setFromTransform( TransformMeta from ) {
    this.from = from;
  }

  public void setToTransform( TransformMeta to ) {
    this.to = to;
  }

  public TransformMeta getFromTransform() {
    return this.from;
  }

  public TransformMeta getToTransform() {
    return this.to;
  }

  private TransformMeta searchTransform( List<TransformMeta> transforms, String name ) {
    for ( TransformMeta transformMeta : transforms ) {
      if ( transformMeta.getName().equalsIgnoreCase( name ) ) {
        return transformMeta;
      }
    }

    return null;
  }

  public boolean equals( Object obj ) {
    PipelineHopMeta other = (PipelineHopMeta) obj;
    if ( this.from == null || this.to == null ) {
      return false;
    }
    return this.from.equals( other.getFromTransform() ) && this.to.equals( other.getToTransform() );
  }

  public int hashCode() {
    return Objects.hash( to, from );
  }

  /**
   * Compare 2 hops.
   */
  public int compareTo( PipelineHopMeta obj ) {
    return toString().compareTo( obj.toString() );
  }

  public void flip() {
    TransformMeta dummy = this.from;
    this.from = this.to;
    this.to = dummy;
  }

  public String toString() {
    String str_fr = ( this.from == null ) ? "(empty)" : this.from.getName();
    String str_to = ( this.to == null ) ? "(empty)" : this.to.getName();
    return str_fr + " --> " + str_to + " (" + ( enabled ? "enabled" : "disabled" ) + ")";
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    if ( this.from != null && this.to != null ) {
      retval.append( "    " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
      retval.append( "      " ).append( XmlHandler.addTagValue( PipelineHopMeta.XML_FROM_TAG, this.from.getName() ) );
      retval.append( "      " ).append( XmlHandler.addTagValue( PipelineHopMeta.XML_TO_TAG, this.to.getName() ) );
      retval.append( "      " ).append( XmlHandler.addTagValue( "enabled", enabled ) );
      retval.append( "    " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );
    }

    return retval.toString();
  }
}
