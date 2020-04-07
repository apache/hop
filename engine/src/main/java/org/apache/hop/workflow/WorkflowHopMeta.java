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

package org.apache.hop.workflow;

import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.action.ActionCopy;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This class defines a hop from one action copy to another.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class WorkflowHopMeta extends BaseHopMeta<ActionCopy> {
  private static Class<?> PKG = WorkflowHopMeta.class; // for i18n purposes, needed by Translator!!

  public static final String XML_FROM_TAG = "from";
  public static final String XML_TO_TAG = "to";

  private boolean evaluation;
  private boolean unconditional;

  public WorkflowHopMeta() {
    this( (ActionCopy) null, (ActionCopy) null );
  }

  public WorkflowHopMeta( ActionCopy from, ActionCopy to ) {
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

  public WorkflowHopMeta( Node hopNode, List<ActionCopy> actions ) throws HopXMLException {
    try {
      this.from = searchEntry( actions, XMLHandler.getTagValue( hopNode, WorkflowHopMeta.XML_FROM_TAG ) );
      this.to = searchEntry( actions, XMLHandler.getTagValue( hopNode, WorkflowHopMeta.XML_TO_TAG ) );
      String en = XMLHandler.getTagValue( hopNode, "enabled" );

      if ( en == null ) {
        enabled = true;
      } else {
        enabled = en.equalsIgnoreCase( "Y" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "WorkflowHopMeta.Exception.UnableToLoadHopInfo" ), e );
    }
  }

  private ActionCopy searchEntry( List<ActionCopy> actions, String name ) {
    for ( ActionCopy action : actions ) {
      if ( action.getName().equalsIgnoreCase( name ) ) {
        return action;
      }
    }
    return null;
  }

  public WorkflowHopMeta( Node hopnode, WorkflowMeta workflow ) throws HopXMLException {
    try {
      String from_name = XMLHandler.getTagValue( hopnode, XML_FROM_TAG );
      String to_name = XMLHandler.getTagValue( hopnode, XML_TO_TAG );
      String sfrom_nr = XMLHandler.getTagValue( hopnode, "from_nr" );
      String sto_nr = XMLHandler.getTagValue( hopnode, "to_nr" );
      String senabled = XMLHandler.getTagValue( hopnode, "enabled" );
      String sevaluation = XMLHandler.getTagValue( hopnode, "evaluation" );
      String sunconditional = XMLHandler.getTagValue( hopnode, "unconditional" );

      int from_nr, to_nr;
      from_nr = Const.toInt( sfrom_nr, 0 );
      to_nr = Const.toInt( sto_nr, 0 );

      this.from = workflow.findAction( from_name, from_nr );
      this.to = workflow.findAction( to_name, to_nr );

      if ( senabled == null ) {
        enabled = true;
      } else {
        enabled = "Y".equalsIgnoreCase( senabled );
      }
      if ( sevaluation == null ) {
        evaluation = true;
      } else {
        evaluation = "Y".equalsIgnoreCase( sevaluation );
      }
      unconditional = "Y".equalsIgnoreCase( sunconditional );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "WorkflowHopMeta.Exception.UnableToLoadHopInfoXML" ), e );
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );
    if ( ( null != this.from ) && ( null != this.to ) ) {
      retval.append( "    " ).append( XMLHandler.openTag( XML_TAG ) ).append( Const.CR );
      retval.append( "      " ).append( XMLHandler.addTagValue( XML_FROM_TAG, this.from.getName() ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( XML_TO_TAG, this.to.getName() ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "from_nr", this.from.getNr() ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "to_nr", this.to.getNr() ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "enabled", enabled ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "evaluation", evaluation ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "unconditional", unconditional ) );
      retval.append( "    " ).append( XMLHandler.closeTag( XML_TAG ) ).append( Const.CR );
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

  public void setSplit( boolean split ) {
    if ( this.split != split ) {
      setChanged();
    }
    this.split = split;
  }

  public boolean isSplit() {
    return split;
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

  public String toString() {
    return getDescription();
    // return from_entry.getName()+"."+from_entry.getNr()+" --> "+to_entry.getName()+"."+to_entry.getNr();
  }

  public ActionCopy getFromEntry() {
    return this.from;
  }

  public void setFromEntry( ActionCopy fromEntry ) {
    this.from = fromEntry;
    changed = true;
  }

  public ActionCopy getToEntry() {
    return this.to;
  }

  public void setToEntry( ActionCopy toEntry ) {
    this.to = toEntry;
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
