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

package org.apache.hop.workflow.action;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.base.IBaseMeta;
import org.apache.hop.core.IAttributes;
import org.apache.hop.core.Const;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.changed.IChanged;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.IGUIPosition;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class describes the fact that a single Action can be used multiple times in the same Workflow. Therefore it contains
 * a link to a Action, a position, a number, etc.
 *
 * @author Matt
 * @since 01-10-2003
 */

public class ActionCopy implements Cloneable, IXml, IGUIPosition, IChanged,
  IAttributes, IBaseMeta {
  public static final String XML_TAG = "entry";

  private static final String XML_ATTRIBUTE_JOB_ENTRY_COPY = AttributesUtil.XML_TAG + "_kjc";

  private IAction entry;

  private String suggestion = "";

  private int nr; // Copy nr. 0 is the base copy...

  private boolean selected;

  private boolean isDeprecated;

  private Point location;

  /**
   * Flag to indicate that the actions following this one are launched in parallel
   */
  private boolean launchingInParallel;

  private WorkflowMeta parentWorkflowMeta;

  private Map<String, Map<String, String>> attributesMap;

  public ActionCopy() {
    clear();
  }

  public ActionCopy( IAction entry ) {
    this();
    setEntry( entry );
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( "    " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
    entry.setParentWorkflowMeta( parentWorkflowMeta );  // Attempt to set the WorkflowMeta for entries that need it
    retval.append( entry.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "parallel", launchingInParallel ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nr", nr ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "xloc", location.x ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "yloc", location.y ) );

    retval.append( AttributesUtil.getAttributesXml( attributesMap, XML_ATTRIBUTE_JOB_ENTRY_COPY ) );

    retval.append( "    " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );
    return retval.toString();
  }


  public ActionCopy( Node entrynode, IMetaStore metaStore ) throws HopXmlException {
    try {
      String stype = XmlHandler.getTagValue( entrynode, "type" );
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin jobPlugin = registry.findPluginWithId( ActionPluginType.class, stype, true );
      if ( jobPlugin == null ) {
        String name = XmlHandler.getTagValue( entrynode, "name" );
        entry = new MissingAction( name, stype );
      } else {
        entry = registry.loadClass( jobPlugin, IAction.class );
      }
      // Get an empty Action of the appropriate class...
      if ( entry != null ) {
        if ( jobPlugin != null ) {
          entry.setPluginId( jobPlugin.getIds()[ 0 ] );
        }
        entry.setMetaStore( metaStore ); // inject metastore
        entry.loadXml( entrynode, metaStore );

        // Handle GUI information: nr & location?
        setNr( Const.toInt( XmlHandler.getTagValue( entrynode, "nr" ), 0 ) );
        setLaunchingInParallel( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "parallel" ) ) );
        int x = Const.toInt( XmlHandler.getTagValue( entrynode, "xloc" ), 0 );
        int y = Const.toInt( XmlHandler.getTagValue( entrynode, "yloc" ), 0 );
        setLocation( x, y );

        Node jobEntryCopyAttributesNode = XmlHandler.getSubNode( entrynode, XML_ATTRIBUTE_JOB_ENTRY_COPY );
        if ( jobEntryCopyAttributesNode != null ) {
          attributesMap = AttributesUtil.loadAttributes( jobEntryCopyAttributesNode );
        } else {
          // [PDI-17345] If the appropriate attributes node wasn't found, this must be an old file (prior to this fix).
          // Before this fix it was very probable to exist two attributes groups. While this is not very valid, in some
          // scenarios the Workflow worked as expected; so by trying to load the LAST one into the ActionCopy, we
          // simulate that behaviour.
          attributesMap =
            AttributesUtil.loadAttributes( XmlHandler.getLastSubNode( entrynode, AttributesUtil.XML_TAG ) );
        }

        setDeprecationAndSuggestedJobEntry();
      }
    } catch ( Throwable e ) {
      String message = "Unable to read Workflow Entry copy info from XML node : " + e.toString();
      throw new HopXmlException( message, e );
    }
  }

  public void clear() {
    location = null;
    entry = null;
    nr = 0;
    launchingInParallel = false;
    attributesMap = new HashMap<>();
  }

  public ActionCopy clone() {
    ActionCopy ge = new ActionCopy();
    ge.replaceMeta( this );

    for ( final Map.Entry<String, Map<String, String>> attribute : attributesMap.entrySet() ) {
      ge.attributesMap.put( attribute.getKey(), attribute.getValue() );
    }

    return ge;
  }

  public void replaceMeta( ActionCopy actionCopy ) {
    entry = (IAction) actionCopy.entry.clone();
    nr = actionCopy.nr; // Copy nr. 0 is the base copy...

    selected = actionCopy.selected;
    if ( actionCopy.location != null ) {
      location = new Point( actionCopy.location.x, actionCopy.location.y );
    }
    launchingInParallel = actionCopy.launchingInParallel;

    setChanged();
  }

  public Object clone_deep() {
    ActionCopy ge = (ActionCopy) clone();

    // Copy underlying object as well...
    ge.entry = (IAction) entry.clone();

    return ge;
  }

  public boolean equals( Object o ) {
    if ( o == null ) {
      return false;
    }
    ActionCopy je = (ActionCopy) o;
    return je.entry.getName().equalsIgnoreCase( entry.getName() ) && je.getNr() == getNr();
  }

  @Override
  public int hashCode() {
    return entry.getName().hashCode() ^ Integer.valueOf( getNr() ).hashCode();
  }

  public void setEntry( IAction je ) {
    entry = je;
    if ( entry != null ) {
      if ( entry.getPluginId() == null ) {
        entry.setPluginId( PluginRegistry.getInstance().getPluginId( ActionPluginType.class, entry ) );
      }
      setDeprecationAndSuggestedJobEntry();
    }
  }

  public IAction getEntry() {
    return entry;
  }

  /**
   * @return entry in IAction.typeCode[] for native workflows, entry.getTypeCode() for plugins
   */
  public String getTypeDesc() {
    IPlugin plugin =
      PluginRegistry.getInstance().findPluginWithId( ActionPluginType.class, entry.getPluginId() );
    return plugin.getDescription();
  }

  public void setLocation( int x, int y ) {
    int nx = ( x >= 0 ? x : 0 );
    int ny = ( y >= 0 ? y : 0 );

    Point loc = new Point( nx, ny );
    if ( !loc.equals( location ) ) {
      setChanged();
    }
    location = loc;
  }

  public void setLocation( Point loc ) {
    if ( loc != null && !loc.equals( location ) ) {
      setChanged();
    }
    location = loc;
  }

  public Point getLocation() {
    return location;
  }

  public void setChanged() {
    setChanged( true );
  }

  public void setChanged( boolean ch ) {
    entry.setChanged( ch );
  }

  public void clearChanged() {
    entry.setChanged( false );
  }

  public boolean hasChanged() {
    return entry.hasChanged();
  }

  public int getNr() {
    return nr;
  }

  public void setNr( int n ) {
    nr = n;
  }

  public void setLaunchingInParallel( boolean p ) {
    launchingInParallel = p;
  }

  public boolean isLaunchingInParallel() {
    return launchingInParallel;
  }

  public void setSelected( boolean sel ) {
    selected = sel;
  }

  public void flipSelected() {
    selected = !selected;
  }

  public boolean isSelected() {
    return selected;
  }

  public void setDescription( String description ) {
    entry.setDescription( description );
  }

  public String getDescription() {
    return entry.getDescription();
  }

  public boolean isStart() {
    return entry.isStart();
  }

  public boolean isDummy() {
    return entry.isDummy();
  }

  public boolean isMissing() {
    return entry instanceof MissingAction;
  }

  public boolean isPipeline() {
    return entry.isPipeline();
  }

  public boolean isJob() {
    return entry.isJob();
  }

  public boolean evaluates() {
    if ( entry != null ) {
      return entry.evaluates();
    }
    return false;
  }

  public boolean isUnconditional() {
    if ( entry != null ) {
      return entry.isUnconditional();
    }
    return true;
  }

  public boolean isEvaluation() {
    return entry.isEvaluation();
  }

  public boolean isMail() {
    return entry.isMail();
  }

  public boolean isSpecial() {
    return entry.isSpecial();
  }

  public String toString() {
    if ( entry != null ) {
      return entry.getName() + "." + getNr();
    } else {
      return "null." + getNr();
    }
  }

  public String getName() {
    if ( entry != null ) {
      return entry.getName();
    } else {
      return "null";
    }
  }

  public void setName( String name ) {
    entry.setName( name );
  }

  public boolean resetErrorsBeforeExecution() {
    return entry.resetErrorsBeforeExecution();
  }

  public WorkflowMeta getParentWorkflowMeta() {
    return parentWorkflowMeta;
  }

  public void setParentWorkflowMeta( WorkflowMeta parentWorkflowMeta ) {
    this.parentWorkflowMeta = parentWorkflowMeta;
    this.entry.setParentWorkflowMeta( parentWorkflowMeta );
  }

  @Override
  public void setAttributesMap( Map<String, Map<String, String>> attributesMap ) {
    this.attributesMap = attributesMap;
  }

  @Override
  public Map<String, Map<String, String>> getAttributesMap() {
    return attributesMap;
  }

  @Override
  public void setAttribute( String groupName, String key, String value ) {
    Map<String, String> attributes = getAttributes( groupName );
    if ( attributes == null ) {
      attributes = new HashMap<>();
      attributesMap.put( groupName, attributes );
    }
    attributes.put( key, value );
  }

  @Override
  public void setAttributes( String groupName, Map<String, String> attributes ) {
    attributesMap.put( groupName, attributes );
  }

  @Override
  public Map<String, String> getAttributes( String groupName ) {
    return attributesMap.get( groupName );
  }

  @Override
  public String getAttribute( String groupName, String key ) {
    Map<String, String> attributes = attributesMap.get( groupName );
    if ( attributes == null ) {
      return null;
    }
    return attributes.get( key );
  }

  public boolean isDeprecated() {
    return isDeprecated;
  }

  public String getSuggestion() {
    return suggestion;
  }

  private void setDeprecationAndSuggestedJobEntry() {
    PluginRegistry registry = PluginRegistry.getInstance();
    final List<IPlugin> deprecatedJobEntries = registry.getPluginsByCategory( ActionPluginType.class,
      BaseMessages.getString( WorkflowMeta.class, "ActionCategory.Category.Deprecated" ) );
    for ( IPlugin p : deprecatedJobEntries ) {
      String[] ids = p.getIds();
      if ( !ArrayUtils.isEmpty( ids ) && ids[ 0 ].equals( this.entry != null ? this.entry.getPluginId() : "" ) ) {
        this.isDeprecated = true;
        this.suggestion = registry.findPluginWithId( ActionPluginType.class, this.entry.getPluginId() ) != null
          ? registry.findPluginWithId( ActionPluginType.class, this.entry.getPluginId() ).getSuggestion() : "";
        break;
      }
    }
  }
}
