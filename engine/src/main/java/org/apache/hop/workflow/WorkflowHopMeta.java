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

package org.apache.hop.workflow;

import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.action.ActionMeta;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This class defines a hop from one action copy to another.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class WorkflowHopMeta extends BaseHopMeta<ActionMeta> implements Cloneable {
  private static final Class<?> PKG = WorkflowHopMeta.class; // For Translator

  public static final String XML_FROM_TAG = "from";
  public static final String XML_TO_TAG = "to";

  private boolean evaluation;
  private boolean unconditional;

  public WorkflowHopMeta() {
    super( false, null, null, true, true, false );
  }

  public WorkflowHopMeta( WorkflowHopMeta hop ) {
    super( hop.isSplit(), hop.getFromAction(), hop.getToAction(), hop.isEnabled(), hop.hasChanged(), hop.isErrorHop() );
    evaluation = hop.evaluation;
    unconditional = hop.unconditional;
  }

  public WorkflowHopMeta( ActionMeta from, ActionMeta to ) {
    this.from = from;
    this.to = to;
    enabled = true;
    split = false;
    evaluation = true;
    unconditional = false;

    if ( from != null && from.isStart() ) {
      setUnconditional();
    }
  }

  public WorkflowHopMeta( Node hopNode, List<ActionMeta> actions ) throws HopXmlException {
    try {
      this.from = searchAction( actions, XmlHandler.getTagValue( hopNode, WorkflowHopMeta.XML_FROM_TAG ) );
      this.to = searchAction( actions, XmlHandler.getTagValue( hopNode, WorkflowHopMeta.XML_TO_TAG ) );
      String en = XmlHandler.getTagValue( hopNode, "enabled" );

      if ( en == null ) {
        enabled = true;
      } else {
        enabled = en.equalsIgnoreCase( "Y" );
      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "WorkflowHopMeta.Exception.UnableToLoadHopInfo" ), e );
    }
  }

  @Override public WorkflowHopMeta clone() {
    return new WorkflowHopMeta( this );
  }

  @Override public String toString() {
    String strFrom = ( this.from == null ) ? "(empty)" : this.from.getName();
    String strTo = ( this.to == null ) ? "(empty)" : this.to.getName();
    String strEnabled = enabled ? "enabled" : "disabled";
    String strEvaluation = unconditional ? "unconditional" : evaluation ? "success" : "failure";
    return strFrom + " --> " + strTo + " [" + strEnabled + ", " + strEvaluation + ")";
  }

  private ActionMeta searchAction( List<ActionMeta> actions, String name ) {
    for ( ActionMeta action : actions ) {
      if ( action.getName().equalsIgnoreCase( name ) ) {
        return action;
      }
    }
    return null;
  }

  public WorkflowHopMeta( Node hopNode, WorkflowMeta workflow ) throws HopXmlException {
    try {
      String fromName = XmlHandler.getTagValue( hopNode, XML_FROM_TAG );
      String toName = XmlHandler.getTagValue( hopNode, XML_TO_TAG );
      String sEnabled = XmlHandler.getTagValue( hopNode, "enabled" );
      String sEvaluation = XmlHandler.getTagValue( hopNode, "evaluation" );
      String sUnconditional = XmlHandler.getTagValue( hopNode, "unconditional" );

      this.from = workflow.findAction( fromName );
      this.to = workflow.findAction( toName );

      if ( sEnabled == null ) {
        enabled = true;
      } else {
        enabled = "Y".equalsIgnoreCase( sEnabled );
      }
      if ( sEvaluation == null ) {
        evaluation = true;
      } else {
        evaluation = "Y".equalsIgnoreCase( sEvaluation );
      }
      unconditional = "Y".equalsIgnoreCase( sUnconditional );
    } catch ( Exception e ) {
      throw new HopXmlException(
        BaseMessages.getString( PKG, "WorkflowHopMeta.Exception.UnableToLoadHopInfoXML" ), e );
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );
    if ( ( null != this.from ) && ( null != this.to ) ) {
      retval.append( "    " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
      retval.append( "      " ).append( XmlHandler.addTagValue( XML_FROM_TAG, this.from.getName() ) );
      retval.append( "      " ).append( XmlHandler.addTagValue( XML_TO_TAG, this.to.getName() ) );
      retval.append( "      " ).append( XmlHandler.addTagValue( "enabled", enabled ) );
      retval.append( "      " ).append( XmlHandler.addTagValue( "evaluation", evaluation ) );
      retval.append( "      " ).append( XmlHandler.addTagValue( "unconditional", unconditional ) );
      retval.append( "    " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );
    }

    return retval.toString();
  }

  public boolean getEvaluation() {
    return evaluation;
  }

  public void setEvaluation() {
    if ( !evaluation ) {
      setChanged();
    }
    setEvaluation( true );
  }

  public void setEvaluation( boolean e ) {
    if ( evaluation != e ) {
      setChanged();
    }
    evaluation = e;
  }

  public void setUnconditional() {
    if ( !unconditional ) {
      setChanged();
    }
    unconditional = true;
  }

  public void setConditional() {
    if ( unconditional ) {
      setChanged();
    }
    unconditional = false;
  }

  public boolean isUnconditional() {
    return unconditional;
  }

  public String getDescription() {
    if ( isUnconditional() ) {
      return BaseMessages.getString( PKG, "WorkflowHopMeta.Msg.ExecNextActionUncondition" );
    } else {
      if ( getEvaluation() ) {
        return BaseMessages.getString( PKG, "WorkflowHopMeta.Msg.ExecNextActionFlawLess" );
      } else {
        return BaseMessages.getString( PKG, "WorkflowHopMeta.Msg.ExecNextActionFailed" );
      }
    }
  }


  public ActionMeta getFromAction() {
    return this.from;
  }

  public void setFromAction( ActionMeta fromAction ) {
    this.from = fromAction;
    changed = true;
  }

  public ActionMeta getToAction() {
    return this.to;
  }

  public void setToAction( ActionMeta toAction ) {
    this.to = toAction;
    changed = true;
  }

  /**
   * @param unconditional the unconditional to set
   */
  public void setUnconditional( boolean unconditional ) {
    if ( this.unconditional != unconditional ) {
      setChanged();
    }
    this.unconditional = unconditional;
  }

}
