// CHECKSTYLE:FileLength:OFF
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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.Props;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.reflection.StringSearchResult;
import org.apache.hop.core.reflection.StringSearcher;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlFormatter;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.apache.hop.workflow.actions.special.ActionSpecial;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceExport;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * The definition of a PDI workflow is represented by a WorkflowMeta object. It is typically loaded from a .hwf file or it is generated dynamically.
 * The declared parameters of the workflow definition are then queried using
 * listParameters() and assigned values using calls to setParameterValue(..). WorkflowMeta provides methods to load, save,
 * verify, etc.
 *
 * @author Matt
 * @since 11-08-2003
 */
public class WorkflowMeta extends AbstractMeta implements Cloneable, Comparable<WorkflowMeta>,
  IXml, IResourceExport, ILoggingObject, IHasFilename {

  private static Class<?> PKG = WorkflowMeta.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG = "workflow";

  static final int BORDER_INDENT = 20;

  protected String workflowVersion;

  protected int workflowStatus;

  protected List<ActionCopy> actionCopies;

  protected List<WorkflowHopMeta> workflowHops;

  protected String[] arguments;

  protected boolean changedActions, changedHops;

  protected String startCopyName;

  protected boolean expandingRemoteWorkflow;

  /**
   * The log channel interface.
   */
  protected ILogChannel log;

  /**
   * Constant = "SPECIAL"
   **/
  public static final String STRING_SPECIAL = "SPECIAL";

  /**
   * Constant = "START"
   **/
  public static final String STRING_SPECIAL_START = "START";

  /**
   * Constant = "DUMMY"
   **/
  public static final String STRING_SPECIAL_DUMMY = "DUMMY";

  /**
   * Constant = "OK"
   **/
  public static final String STRING_SPECIAL_OK = "OK";

  /**
   * Constant = "ERROR"
   **/
  public static final String STRING_SPECIAL_ERROR = "ERROR";

  /**
   * The loop cache.
   */
  protected Map<String, Boolean> loopCache;

  /**
   * List of booleans indicating whether or not to remember the size and position of the different windows...
   */
  public boolean[] max = new boolean[ 1 ];

  protected boolean batchIdPassed;

  protected static final String XML_TAG_PARAMETERS = "parameters";

  private List<MissingAction> missingActions;

  /**
   * Instantiates a new workflow meta.
   */
  public WorkflowMeta() {
    clear();
    initializeVariablesFrom( null );
  }

  /**
   * Clears or reinitializes many of the WorkflowMeta properties.
   */
  @Override
  public void clear() {
    actionCopies = new ArrayList<>();
    workflowHops = new ArrayList<>();

    arguments = null;

    super.clear();
    loopCache = new HashMap<String, Boolean>();
    addDefaults();
    workflowStatus = -1;
    workflowVersion = null;

    // setInternalHopVariables(); Don't clear the internal variables for
    // ad-hoc workflows, it's ruins the previews
    // etc.

    log = LogChannel.GENERAL;
  }

  /**
   * Adds the defaults.
   */
  public void addDefaults() {
    /*
     * addStart(); // Add starting point! addDummy(); // Add dummy! addOK(); // errors == 0 evaluation addError(); //
     * errors != 0 evaluation
     */

    clearChanged();
  }

  /**
   * Creates the start action.
   *
   * @return the action copy
   */
  public static final ActionCopy createStartAction() {
    ActionSpecial jobEntrySpecial = new ActionSpecial( BaseMessages.getString( PKG, "WorkflowMeta.StartAction.Name" ), true, false );
    ActionCopy jobEntry = new ActionCopy();
    jobEntry.setEntry( jobEntrySpecial );
    jobEntry.setLocation( 50, 50 );
    jobEntry.setDescription( BaseMessages.getString( PKG, "WorkflowMeta.StartAction.Description" ) );
    return jobEntry;

  }

  /**
   * Creates the dummy action.
   *
   * @return the action copy
   */
  public static final ActionCopy createDummyEntry() {
    ActionSpecial jobEntrySpecial = new ActionSpecial( BaseMessages.getString( PKG, "WorkflowMeta.DummyAction.Name" ), false, true );
    ActionCopy jobEntry = new ActionCopy();
    jobEntry.setEntry( jobEntrySpecial );
    jobEntry.setLocation( 50, 50 );
    jobEntry.setDescription( BaseMessages.getString( PKG, "WorkflowMeta.DummyAction.Description" ) );
    return jobEntry;
  }

  /**
   * Gets the start.
   *
   * @return the start
   */
  public ActionCopy getStart() {
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy cge = getAction( i );
      if ( cge.isStart() ) {
        return cge;
      }
    }
    return null;
  }

  /**
   * Gets the dummy.
   *
   * @return the dummy
   */
  public ActionCopy getDummy() {
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy cge = getAction( i );
      if ( cge.isDummy() ) {
        return cge;
      }
    }
    return null;
  }

  /**
   * Compares two workflow on name, filename, etc.
   * The comparison algorithm is as follows:<br/>
   * <ol>
   * <li>The first workflow's filename is checked first; if it has none, the workflow is created.
   * If the second workflow does not come from a repository, -1 is returned.</li>
   * <li>If the workflows are both created, the workflows' names are compared. If the first
   * workflow has no name and the second one does, a -1 is returned.
   * If the opposite is true, a 1 is returned.</li>
   * <li>If they both have names they are compared as strings. If the result is non-zero it is returned.</li>
   * </ol>
   *
   * @param j1 the first workflow to compare
   * @param j2 the second workflow to compare
   * @return 0 if the two workflows are equal, 1 or -1 depending on the values (see description above)
   */
  public int compare( WorkflowMeta j1, WorkflowMeta j2 ) {
    return super.compare( j1, j2 );
  }

  /**
   * Compares this workflow's meta-data to the specified workflow's meta-data. This method simply calls compare(this, o)
   *
   * @param o the o
   * @return the int
   * @see #compare(WorkflowMeta, WorkflowMeta)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo( WorkflowMeta o ) {
    return compare( this, o );
  }

  /**
   * Checks whether this workflow's meta-data object is equal to the specified object. If the specified object is not an
   * instance of WorkflowMeta, false is returned. Otherwise the method returns whether a call to compare() indicates equality
   * (i.e. compare(this, (WorkflowMeta)obj)==0).
   *
   * @param obj the obj
   * @return true, if successful
   * @see #compare(WorkflowMeta, WorkflowMeta)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals( Object obj ) {
    if ( !( obj instanceof WorkflowMeta ) ) {
      return false;
    }

    return compare( this, (WorkflowMeta) obj ) == 0;
  }

  /**
   * Clones the workflow meta-data object.
   *
   * @return a clone of the workflow meta-data object
   * @see java.lang.Object#clone()
   */
  public Object clone() {
    return realClone( true );
  }

  /**
   * Perform a real clone of the workflow meta-data object, including cloning all lists and copying all values. If the
   * doClear parameter is true, the clone will be cleared of ALL values before the copy. If false, only the copied
   * fields will be cleared.
   *
   * @param doClear Whether to clear all of the clone's data before copying from the source object
   * @return a real clone of the calling object
   */
  public Object realClone( boolean doClear ) {
    try {
      WorkflowMeta workflowMeta = (WorkflowMeta) super.clone();
      if ( doClear ) {
        workflowMeta.clear();
      } else {
        workflowMeta.actionCopies = new ArrayList<ActionCopy>();
        workflowMeta.workflowHops = new ArrayList<WorkflowHopMeta>();
        workflowMeta.notes = new ArrayList<NotePadMeta>();
        workflowMeta.namedParams = new NamedParamsDefault();
      }

      for ( ActionCopy action : actionCopies ) {
        workflowMeta.actionCopies.add( (ActionCopy) action.clone_deep() );
      }
      for ( WorkflowHopMeta action : workflowHops ) {
        workflowMeta.workflowHops.add( (WorkflowHopMeta) action.clone() );
      }
      for ( NotePadMeta action : notes ) {
        workflowMeta.notes.add( (NotePadMeta) action.clone() );
      }

      for ( String key : listParameters() ) {
        workflowMeta.addParameterDefinition( key, getParameterDefault( key ), getParameterDescription( key ) );
      }
      return workflowMeta;
    } catch ( Exception e ) {
      return null;
    }
  }

  /**
   * Clears the different changed flags of the workflow.
   */
  @Override
  public void clearChanged() {
    changedActions = false;
    changedHops = false;

    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy action = getAction( i );
      action.setChanged( false );
    }
    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops
      hi.setChanged( false );
    }
    super.clearChanged();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.changed.ChangedFlag#hasChanged()
   */
  @Override
  public boolean hasChanged() {
    if ( super.hasChanged() ) {
      return true;
    }

    if ( haveActionsChanged() ) {
      return true;
    }
    if ( haveWorkflowHopsChanged() ) {
      return true;
    }

    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.xml.IXml#getXml()
   */
  public String getXml() {

    Props props = null;
    if ( Props.isInitialized() ) {
      props = Props.getInstance();
    }

    StringBuilder retval = new StringBuilder( 500 );

    retval.append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );

    retval.append( "  " ).append( XmlHandler.addTagValue( "name", getName() ) );

    retval.append( "  " ).append( XmlHandler.addTagValue( "description", description ) );
    retval.append( "  " ).append( XmlHandler.addTagValue( "extended_description", extendedDescription ) );
    retval.append( "  " ).append( XmlHandler.addTagValue( "workflow_version", workflowVersion ) );
    if ( workflowStatus >= 0 ) {
      retval.append( "  " ).append( XmlHandler.addTagValue( "workflow_status", workflowStatus ) );
    }

    retval.append( "  " ).append( XmlHandler.addTagValue( "created_user", createdUser ) );
    retval.append( "  " ).append( XmlHandler.addTagValue( "created_date", XmlHandler.date2string( createdDate ) ) );
    retval.append( "  " ).append( XmlHandler.addTagValue( "modified_user", modifiedUser ) );
    retval.append( "  " ).append( XmlHandler.addTagValue( "modified_date", XmlHandler.date2string( modifiedDate ) ) );

    retval.append( "    " ).append( XmlHandler.openTag( XML_TAG_PARAMETERS ) ).append( Const.CR );
    String[] parameters = listParameters();
    for ( int idx = 0; idx < parameters.length; idx++ ) {
      retval.append( "      " ).append( XmlHandler.openTag( "parameter" ) ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "name", parameters[ idx ] ) );
      try {
        retval.append( "        " )
          .append( XmlHandler.addTagValue( "default_value", getParameterDefault( parameters[ idx ] ) ) );
        retval.append( "        " )
          .append( XmlHandler.addTagValue( "description", getParameterDescription( parameters[ idx ] ) ) );
      } catch ( UnknownParamException e ) {
        // skip the default value and/or description. This exception should never happen because we use listParameters()
        // above.
      }
      retval.append( "      " ).append( XmlHandler.closeTag( "parameter" ) ).append( Const.CR );
    }
    retval.append( "    " ).append( XmlHandler.closeTag( XML_TAG_PARAMETERS ) ).append( Const.CR );


    retval.append( "   " ).append( XmlHandler.addTagValue( "pass_batchid", batchIdPassed ) );

    retval.append( "  " ).append( XmlHandler.openTag( "actions" ) ).append( Const.CR );
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy jge = getAction( i );
      retval.append( jge.getXml() );
    }
    retval.append( "  " ).append( XmlHandler.closeTag( "actions" ) ).append( Const.CR );

    retval.append( "  " ).append( XmlHandler.openTag( "hops" ) ).append( Const.CR );
    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops
      retval.append( hi.getXml() );
    }
    retval.append( "  " ).append( XmlHandler.closeTag( "hops" ) ).append( Const.CR );

    retval.append( "  " ).append( XmlHandler.openTag( "notepads" ) ).append( Const.CR );
    for ( int i = 0; i < nrNotes(); i++ ) {
      NotePadMeta ni = getNote( i );
      retval.append( ni.getXml() );
    }
    retval.append( "  " ).append( XmlHandler.closeTag( "notepads" ) ).append( Const.CR );

    // Also store the attribute groups
    //
    retval.append( AttributesUtil.getAttributesXml( attributesMap ) );

    retval.append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return XmlFormatter.format( retval.toString() );
  }

  /**
   * Instantiates a new workflow meta.
   *
   * @param fname the fname
   * @throws HopXmlException the kettle xml exception
   */
  public WorkflowMeta( String fname ) throws HopXmlException {
    this( null, fname, null );
  }

  /**
   * Load the workflow from the XML file specified
   *
   * @param parentSpace
   * @param fname
   * @param metaStore
   * @throws HopXmlException
   */
  public WorkflowMeta( IVariables parentSpace, String fname, IMetaStore metaStore ) throws HopXmlException {
    this.initializeVariablesFrom( parentSpace );
    this.metaStore = metaStore;
    try {
      // OK, try to load using the VFS stuff...
      Document doc = XmlHandler.loadXmlFile( HopVfs.getFileObject( fname, this ) );
      if ( doc != null ) {
        // The jobnode
        Node jobnode = XmlHandler.getSubNode( doc, XML_TAG );

        loadXml( jobnode, fname, metaStore );
      } else {
        throw new HopXmlException(
          BaseMessages.getString( PKG, "WorkflowMeta.Exception.ErrorReadingFromXMLFile" ) + fname );
      }
    } catch ( Exception e ) {
      throw new HopXmlException(
        BaseMessages.getString( PKG, "WorkflowMeta.Exception.UnableToLoadJobFromXMLFile" ) + fname + "]", e );
    }
  }

  /**
   * Instantiates a new workflow meta.
   *
   * @param inputStream the input stream
   * @throws HopXmlException the kettle xml exception
   */
  public WorkflowMeta( InputStream inputStream ) throws HopXmlException {
    this();
    Document doc = XmlHandler.loadXmlFile( inputStream, null, false, false );
    Node subNode = XmlHandler.getSubNode( doc, WorkflowMeta.XML_TAG );
    loadXml( subNode, null );
  }

  /**
   * Create a new WorkflowMeta object by loading it from a a DOM node.
   *
   * @param workflowNode The node to load from
   * @throws HopXmlException
   */
  public WorkflowMeta( Node workflowNode ) throws HopXmlException {
    this();
    loadXml( workflowNode, null );
  }


  /**
   * Load xml.
   *
   * @param jobnode the jobnode
   * @param fname   The filename
   * @throws HopXmlException the kettle xml exception
   */
  public void loadXml( Node jobnode, String fname )
    throws HopXmlException {
    loadXml( jobnode, fname, null );
  }

  /**
   * Load a block of XML from an DOM node.
   *
   * @param jobnode   The node to load from
   * @param fname     The filename
   * @param metaStore the MetaStore to use
   * @throws HopXmlException
   */
  public void loadXml( Node jobnode, String fname, IMetaStore metaStore ) throws HopXmlException {
    Props props = null;
    if ( Props.isInitialized() ) {
      props = Props.getInstance();
    }

    try {
      // clear the workflows;
      clear();

      setFilename( fname );

      // get workflow info:
      //
      setName( XmlHandler.getTagValue( jobnode, "name" ) );

      // description
      description = XmlHandler.getTagValue( jobnode, "description" );

      // extended description
      extendedDescription = XmlHandler.getTagValue( jobnode, "extended_description" );

      // workflow version
      workflowVersion = XmlHandler.getTagValue( jobnode, "workflow_version" );

      // workflow status
      workflowStatus = Const.toInt( XmlHandler.getTagValue( jobnode, "workflow_status" ), -1 );

      // Created user/date
      createdUser = XmlHandler.getTagValue( jobnode, "created_user" );
      String createDate = XmlHandler.getTagValue( jobnode, "created_date" );

      if ( createDate != null ) {
        createdDate = XmlHandler.stringToDate( createDate );
      }

      // Changed user/date
      modifiedUser = XmlHandler.getTagValue( jobnode, "modified_user" );
      String modDate = XmlHandler.getTagValue( jobnode, "modified_date" );
      if ( modDate != null ) {
        modifiedDate = XmlHandler.stringToDate( modDate );
      }

      // Read the named parameters.
      Node paramsNode = XmlHandler.getSubNode( jobnode, XML_TAG_PARAMETERS );
      int nrParams = XmlHandler.countNodes( paramsNode, "parameter" );

      for ( int i = 0; i < nrParams; i++ ) {
        Node paramNode = XmlHandler.getSubNodeByNr( paramsNode, "parameter", i );

        String paramName = XmlHandler.getTagValue( paramNode, "name" );
        String defValue = XmlHandler.getTagValue( paramNode, "default_value" );
        String descr = XmlHandler.getTagValue( paramNode, "description" );

        addParameterDefinition( paramName, defValue, descr );
      }

      batchIdPassed = "Y".equalsIgnoreCase( XmlHandler.getTagValue( jobnode, "pass_batchid" ) );

      /*
       * read the actions...
       */
      Node entriesnode = XmlHandler.getSubNode( jobnode, "actions" );
      int tr = XmlHandler.countNodes( entriesnode, "action" );
      for ( int i = 0; i < tr; i++ ) {
        Node entrynode = XmlHandler.getSubNodeByNr( entriesnode, "action", i );
        ActionCopy je = new ActionCopy( entrynode, metaStore );

        if ( je.isSpecial() && je.isMissing() ) {
          addMissingAction( (MissingAction) je.getEntry() );
        }
        ActionCopy prev = findAction( je.getName(), 0 );
        if ( prev != null ) {
          // See if the #0 (root action) already exists!
          //
          if ( je.getNr() == 0 ) {

            // Replace previous version with this one: remove it first
            //
            int idx = indexOfAction( prev );
            removeAction( idx );

          } else if ( je.getNr() > 0 ) {

            // Use previously defined Action info!
            //
            je.setEntry( prev.getEntry() );

            // See if action already exists...
            prev = findAction( je.getName(), je.getNr() );
            if ( prev != null ) {
              // remove the old one!
              //
              int idx = indexOfAction( prev );
              removeAction( idx );
            }
          }
        }
        // Add the ActionCopy...
        addAction( je );
      }

      Node hopsnode = XmlHandler.getSubNode( jobnode, "hops" );
      int ho = XmlHandler.countNodes( hopsnode, "hop" );
      for ( int i = 0; i < ho; i++ ) {
        Node hopnode = XmlHandler.getSubNodeByNr( hopsnode, "hop", i );
        WorkflowHopMeta hi = new WorkflowHopMeta( hopnode, this );
        workflowHops.add( hi );
      }

      // Read the notes...
      Node notepadsnode = XmlHandler.getSubNode( jobnode, "notepads" );
      int nrnotes = XmlHandler.countNodes( notepadsnode, "notepad" );
      for ( int i = 0; i < nrnotes; i++ ) {
        Node notepadnode = XmlHandler.getSubNodeByNr( notepadsnode, "notepad", i );
        NotePadMeta ni = new NotePadMeta( notepadnode );
        notes.add( ni );
      }

      // Load the attribute groups map
      //
      attributesMap = AttributesUtil.loadAttributes( XmlHandler.getSubNode( jobnode, AttributesUtil.XML_TAG ) );

      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.WorkflowMetaLoaded.id, this );

      clearChanged();
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "WorkflowMeta.Exception.UnableToLoadJobFromXMLNode" ), e );
    } finally {
      setInternalHopVariables();
    }
  }

  /**
   * Gets the action copy.
   *
   * @param x        the x
   * @param y        the y
   * @param iconsize the iconsize
   * @return the action copy
   */
  public ActionCopy getAction( int x, int y, int iconsize ) {
    int i, s;
    s = nrActions();
    for ( i = s - 1; i >= 0; i-- ) {
      // Back to front because drawing goes from start to end

      ActionCopy je = getAction( i );
      Point p = je.getLocation();
      if ( p != null ) {
        if ( x >= p.x && x <= p.x + iconsize && y >= p.y && y <= p.y + iconsize ) {
          return je;
        }
      }
    }
    return null;
  }

  /**
   * Nr actions.
   *
   * @return the int
   */
  public int nrActions() {
    return actionCopies.size();
  }

  /**
   * Nr workflow hops.
   *
   * @return the int
   */
  public int nrWorkflowHops() {
    return workflowHops.size();
  }

  /**
   * Gets the workflow hop.
   *
   * @param i the i
   * @return the workflow hop
   */
  public WorkflowHopMeta getWorkflowHop( int i ) {
    return workflowHops.get( i );
  }

  /**
   * Gets the action.
   *
   * @param i the i
   * @return the action
   */
  public ActionCopy getAction( int i ) {
    return actionCopies.get( i );
  }

  /**
   * Adds the action.
   *
   * @param je the je
   */
  public void addAction( ActionCopy je ) {
    actionCopies.add( je );
    je.setParentWorkflowMeta( this );
    setChanged();
  }

  /**
   * Adds the workflow hop.
   *
   * @param hi the hi
   */
  public void addWorkflowHop( WorkflowHopMeta hi ) {
    workflowHops.add( hi );
    setChanged();
  }

  /**
   * Adds the action.
   *
   * @param p  the p
   * @param si the si
   */
  public void addAction( int p, ActionCopy si ) {
    actionCopies.add( p, si );
    changedActions = true;
  }

  /**
   * Adds the workflow hop.
   *
   * @param p  the p
   * @param hi the hi
   */
  public void addWorkflowHop( int p, WorkflowHopMeta hi ) {
    try {
      workflowHops.add( p, hi );
    } catch ( IndexOutOfBoundsException e ) {
      workflowHops.add( hi );
    }
    changedHops = true;
  }

  /**
   * Removes the action.
   *
   * @param i the i
   */
  public void removeAction( int i ) {
    ActionCopy deleted = actionCopies.remove( i );
    if ( deleted != null ) {
      // give transform a chance to cleanup
      deleted.setParentWorkflowMeta( null );
      if ( deleted.getEntry() instanceof MissingAction ) {
        removeMissingAction( (MissingAction) deleted.getEntry() );
      }
    }
    setChanged();
  }

  /**
   * Removes the workflow hop.
   *
   * @param i the i
   */
  public void removeWorkflowHop( int i ) {
    workflowHops.remove( i );
    setChanged();
  }

  /**
   * Removes a hop from the pipeline. Also marks that the
   * pipeline's hops have changed.
   *
   * @param hop The hop to remove from the list of hops
   */
  public void removeWorkflowHop( WorkflowHopMeta hop ) {
    workflowHops.remove( hop );
    setChanged();
  }

  /**
   * Index of workflow hop.
   *
   * @param he the he
   * @return the int
   */
  public int indexOfWorkflowHop( WorkflowHopMeta he ) {
    return workflowHops.indexOf( he );
  }

  /**
   * Index of action.
   *
   * @param ge the ge
   * @return the int
   */
  public int indexOfAction( ActionCopy ge ) {
    return actionCopies.indexOf( ge );
  }

  /**
   * Sets the action.
   *
   * @param idx the idx
   * @param jec the jec
   */
  public void setAction( int idx, ActionCopy jec ) {
    actionCopies.set( idx, jec );
  }

  /**
   * Find an existing ActionCopy by it's name and number
   *
   * @param name The name of the action copy
   * @param nr   The number of the action copy
   * @return The ActionCopy or null if nothing was found!
   */
  public ActionCopy findAction( String name, int nr) {
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy jec = getAction( i );
      if ( jec.getName().equalsIgnoreCase( name ) && jec.getNr() == nr ) {
        return jec;
      }
    }
    return null;
  }

  /**
   * Find action.
   *
   * @param full_name_nr the full_name_nr
   * @return the action copy
   */
  public ActionCopy findAction( String full_name_nr ) {
    int i;
    for ( i = 0; i < nrActions(); i++ ) {
      ActionCopy jec = getAction( i );
      IAction je = jec.getEntry();
      if ( je.toString().equalsIgnoreCase( full_name_nr ) ) {
        return jec;
      }
    }
    return null;
  }

  /**
   * Find workflow hop.
   *
   * @param name the name
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHop( String name ) {
    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.toString().equalsIgnoreCase( name ) ) {
        return hi;
      }
    }
    return null;
  }

  /**
   * Find workflow hop from.
   *
   * @param jge the jge
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHopFrom( ActionCopy jge ) {
    if ( jge != null ) {
      for ( WorkflowHopMeta hi : workflowHops ) {

        // Return the first we find!
        //
        if ( hi != null && ( hi.getFromEntry() != null ) && hi.getFromEntry().equals( jge ) ) {
          return hi;
        }
      }
    }
    return null;
  }

  /**
   * Find workflow hop.
   *
   * @param from the from
   * @param to   the to
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHop( ActionCopy from, ActionCopy to ) {
    return findWorkflowHop( from, to, false );
  }

  /**
   * Find workflow hop.
   *
   * @param from            the from
   * @param to              the to
   * @param includeDisabled the include disabled
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHop( ActionCopy from, ActionCopy to, boolean includeDisabled ) {
    for ( WorkflowHopMeta hi : workflowHops ) {
      if ( hi.isEnabled() || includeDisabled ) {
        if ( hi != null && hi.getFromEntry() != null && hi.getToEntry() != null && hi.getFromEntry().equals( from )
          && hi.getToEntry().equals( to ) ) {
          return hi;
        }
      }
    }
    return null;
  }

  /**
   * Find workflow hop to.
   *
   * @param jge the jge
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHopTo( ActionCopy jge ) {
    for ( WorkflowHopMeta hi : workflowHops ) {
      if ( hi != null && hi.getToEntry() != null && hi.getToEntry().equals( jge ) ) {
        // Return the first!
        return hi;
      }
    }
    return null;
  }

  /**
   * Find nr prev actions.
   *
   * @param from the from
   * @return the int
   */
  public int findNrPrevActions( ActionCopy from ) {
    return findNrPrevActions( from, false );
  }

  /**
   * Find prev action.
   *
   * @param to the to
   * @param nr the nr
   * @return the action copy
   */
  public ActionCopy findPrevAction( ActionCopy to, int nr ) {
    return findPrevAction( to, nr, false );
  }

  /**
   * Find nr prev actions.
   *
   * @param to   the to
   * @param info the info
   * @return the int
   */
  public int findNrPrevActions( ActionCopy to, boolean info ) {
    int count = 0;

    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.isEnabled() && hi.getToEntry().equals( to ) ) {
        count++;
      }
    }
    return count;
  }

  /**
   * Find prev action.
   *
   * @param to   the to
   * @param nr   the nr
   * @param info the info
   * @return the action copy
   */
  public ActionCopy findPrevAction( ActionCopy to, int nr, boolean info ) {
    int count = 0;

    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.isEnabled() && hi.getToEntry().equals( to ) ) {
        if ( count == nr ) {
          return hi.getFromEntry();
        }
        count++;
      }
    }
    return null;
  }

  /**
   * Find nr next actions.
   *
   * @param from the from
   * @return the int
   */
  public int findNrNextActions( ActionCopy from ) {
    int count = 0;
    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.isEnabled() && ( hi.getFromEntry() != null ) && hi.getFromEntry().equals( from ) ) {
        count++;
      }
    }
    return count;
  }

  /**
   * Find next action.
   *
   * @param from the from
   * @param cnt  the cnt
   * @return the action copy
   */
  public ActionCopy findNextAction( ActionCopy from, int cnt ) {
    int count = 0;

    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.isEnabled() && ( hi.getFromEntry() != null ) && hi.getFromEntry().equals( from ) ) {
        if ( count == cnt ) {
          return hi.getToEntry();
        }
        count++;
      }
    }
    return null;
  }

  /**
   * Checks for loop.
   *
   * @param action the action
   * @return true, if successful
   */
  public boolean hasLoop( ActionCopy action ) {
    clearLoopCache();
    return hasLoop( action, null );
  }

  /**
   * @deprecated use {@link #hasLoop(ActionCopy, ActionCopy)}}
   */
  @Deprecated
  public boolean hasLoop( ActionCopy action, ActionCopy lookup, boolean info ) {
    return hasLoop( action, lookup );
  }

  /**
   * Checks for loop.
   *
   * @param action  the action
   * @param lookup the lookup
   * @return true, if successful
   */

  public boolean hasLoop( ActionCopy action, ActionCopy lookup ) {
    return hasLoop( action, lookup, new HashSet<ActionCopy>() );
  }

  /**
   * Checks for loop.
   *
   * @param action  the action
   * @param lookup the lookup
   * @return true, if successful
   */
  private boolean hasLoop( ActionCopy action, ActionCopy lookup, HashSet<ActionCopy> checkedEntries ) {
    String cacheKey =
      action.getName() + " - " + ( lookup != null ? lookup.getName() : "" );

    Boolean hasLoop = loopCache.get( cacheKey );

    if ( hasLoop != null ) {
      return hasLoop;
    }

    hasLoop = false;

    checkedEntries.add( action );

    int nr = findNrPrevActions( action );
    for ( int i = 0; i < nr; i++ ) {
      ActionCopy prevWorkflowMeta = findPrevAction( action, i );
      if ( prevWorkflowMeta != null && ( prevWorkflowMeta.equals( lookup )
        || ( !checkedEntries.contains( prevWorkflowMeta ) && hasLoop( prevWorkflowMeta, lookup == null ? action : lookup, checkedEntries ) ) ) ) {
        hasLoop = true;
        break;
      }
    }

    loopCache.put( cacheKey, hasLoop );
    return hasLoop;
  }

  /**
   * Clears the loop cache.
   */
  private void clearLoopCache() {
    loopCache.clear();
  }

  /**
   * Checks if is action used in hops.
   *
   * @param jge the jge
   * @return true, if is action used in hops
   */
  public boolean isEntryUsedInHops( ActionCopy jge ) {
    WorkflowHopMeta fr = findWorkflowHopFrom( jge );
    WorkflowHopMeta to = findWorkflowHopTo( jge );
    if ( fr != null || to != null ) {
      return true;
    }
    return false;
  }

  /**
   * Count actions.
   *
   * @param name the name
   * @return the int
   */
  public int countEntries( String name ) {
    int count = 0;
    int i;
    for ( i = 0; i < nrActions(); i++ ) {
      // Look at all the hops;

      ActionCopy je = getAction( i );
      if ( je.getName().equalsIgnoreCase( name ) ) {
        count++;
      }
    }
    return count;
  }

  /**
   * Find unused nr.
   *
   * @param name the name
   * @return the int
   */
  public int findUnusedNr( String name ) {
    int nr = 1;
    ActionCopy je = findAction( name, nr );
    while ( je != null ) {
      nr++;
      // log.logDebug("findUnusedNr()", "Trying unused nr: "+nr);
      je = findAction( name, nr );
    }
    return nr;
  }

  /**
   * Find max nr.
   *
   * @param name the name
   * @return the int
   */
  public int findMaxNr( String name ) {
    int max = 0;
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy je = getAction( i );
      if ( je.getName().equalsIgnoreCase( name ) ) {
        if ( je.getNr() > max ) {
          max = je.getNr();
        }
      }
    }
    return max;
  }

  /**
   * Proposes an alternative action name when the original already exists...
   *
   * @param entryname The action name to find an alternative for..
   * @return The alternative transformName.
   */
  public String getAlternativeJobentryName( String entryname ) {
    String newname = entryname;
    ActionCopy jec = findAction( newname );
    int nr = 1;
    while ( jec != null ) {
      nr++;
      newname = entryname + " " + nr;
      jec = findAction( newname );
    }

    return newname;
  }

  /**
   * Gets the all workflow graph actions.
   *
   * @param name the name
   * @return the all workflow graph actions
   */
  public ActionCopy[] getAllJobGraphEntries( String name ) {
    int count = 0;
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy je = getAction( i );
      if ( je.getName().equalsIgnoreCase( name ) ) {
        count++;
      }
    }
    ActionCopy[] retval = new ActionCopy[ count ];

    count = 0;
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy je = getAction( i );
      if ( je.getName().equalsIgnoreCase( name ) ) {
        retval[ count ] = je;
        count++;
      }
    }
    return retval;
  }

  /**
   * Gets the all workflow hops using.
   *
   * @param name the name
   * @return the all workflow hops using
   */
  public WorkflowHopMeta[] getAllWorkflowHopsUsing( String name ) {
    List<WorkflowHopMeta> hops = new ArrayList<WorkflowHopMeta>();

    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.getFromEntry() != null && hi.getToEntry() != null ) {
        if ( hi.getFromEntry().getName().equalsIgnoreCase( name ) || hi.getToEntry().getName()
          .equalsIgnoreCase( name ) ) {
          hops.add( hi );
        }
      }
    }
    return hops.toArray( new WorkflowHopMeta[ hops.size() ] );
  }

  public boolean isPathExist( IAction from, IAction to ) {
    for ( WorkflowHopMeta hi : workflowHops ) {
      if ( hi.getFromEntry() != null && hi.getToEntry() != null ) {
        if ( hi.getFromEntry().getName().equalsIgnoreCase( from.getName() ) ) {
          if ( hi.getToEntry().getName().equalsIgnoreCase( to.getName() ) ) {
            return true;
          }
          if ( isPathExist( hi.getToEntry().getEntry(), to ) ) {
            return true;
          }
        }
      }
    }

    return false;
  }

  /**
   * Select all.
   */
  public void selectAll() {
    int i;
    for ( i = 0; i < nrActions(); i++ ) {
      ActionCopy ce = getAction( i );
      ce.setSelected( true );
    }
    for ( i = 0; i < nrNotes(); i++ ) {
      NotePadMeta ni = getNote( i );
      ni.setSelected( true );
    }
    setChanged();
    notifyObservers( "refreshGraph" );
  }

  /**
   * Unselect all.
   */
  public void unselectAll() {
    int i;
    for ( i = 0; i < nrActions(); i++ ) {
      ActionCopy ce = getAction( i );
      ce.setSelected( false );
    }
    for ( i = 0; i < nrNotes(); i++ ) {
      NotePadMeta ni = getNote( i );
      ni.setSelected( false );
    }
  }

  /**
   * Gets the maximum.
   *
   * @return the maximum
   */
  public Point getMaximum() {
    int maxx = 0, maxy = 0;
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy action = getAction( i );
      Point loc = action.getLocation();
      if ( loc.x > maxx ) {
        maxx = loc.x;
      }
      if ( loc.y > maxy ) {
        maxy = loc.y;
      }
    }
    for ( int i = 0; i < nrNotes(); i++ ) {
      NotePadMeta ni = getNote( i );
      Point loc = ni.getLocation();
      if ( loc.x + ni.width > maxx ) {
        maxx = loc.x + ni.width;
      }
      if ( loc.y + ni.height > maxy ) {
        maxy = loc.y + ni.height;
      }
    }

    return new Point( maxx + 100, maxy + 100 );
  }

  /**
   * Get the minimum point on the canvas of a workflow
   *
   * @return Minimum coordinate of a transform in the workflow
   */
  public Point getMinimum() {
    int minx = Integer.MAX_VALUE;
    int miny = Integer.MAX_VALUE;
    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy actionCopy = getAction( i );
      Point loc = actionCopy.getLocation();
      if ( loc.x < minx ) {
        minx = loc.x;
      }
      if ( loc.y < miny ) {
        miny = loc.y;
      }
    }
    for ( int i = 0; i < nrNotes(); i++ ) {
      NotePadMeta notePadMeta = getNote( i );
      Point loc = notePadMeta.getLocation();
      if ( loc.x < minx ) {
        minx = loc.x;
      }
      if ( loc.y < miny ) {
        miny = loc.y;
      }
    }

    if ( minx > BORDER_INDENT && minx != Integer.MAX_VALUE ) {
      minx -= BORDER_INDENT;
    } else {
      minx = 0;
    }
    if ( miny > BORDER_INDENT && miny != Integer.MAX_VALUE ) {
      miny -= BORDER_INDENT;
    } else {
      miny = 0;
    }

    return new Point( minx, miny );
  }

  /**
   * Gets the selected locations.
   *
   * @return the selected locations
   */
  public Point[] getSelectedLocations() {
    List<ActionCopy> selectedEntries = getSelectedEntries();
    Point[] retval = new Point[ selectedEntries.size() ];
    for ( int i = 0; i < retval.length; i++ ) {
      ActionCopy si = selectedEntries.get( i );
      Point p = si.getLocation();
      retval[ i ] = new Point( p.x, p.y ); // explicit copy of location
    }
    return retval;
  }

  /**
   * Get all the selected note locations
   *
   * @return The selected transform and notes locations.
   */
  public Point[] getSelectedNoteLocations() {
    List<Point> points = new ArrayList<Point>();

    for ( NotePadMeta ni : getSelectedNotes() ) {
      Point p = ni.getLocation();
      points.add( new Point( p.x, p.y ) ); // explicit copy of location
    }

    return points.toArray( new Point[ points.size() ] );
  }

  /**
   * Gets the selected actions.
   *
   * @return the selected actions
   */
  public List<ActionCopy> getSelectedEntries() {
    List<ActionCopy> selection = new ArrayList<ActionCopy>();
    for ( ActionCopy je : actionCopies ) {
      if ( je.isSelected() ) {
        selection.add( je );
      }
    }
    return selection;
  }

  /**
   * Gets the action indexes.
   *
   * @param actions the actions
   * @return the action indexes
   */
  public int[] getEntryIndexes( List<ActionCopy> actions ) {
    int[] retval = new int[ actions.size() ];

    for ( int i = 0; i < actions.size(); i++ ) {
      retval[ i ] = indexOfAction( actions.get( i ) );
    }

    return retval;
  }

  /**
   * Find start.
   *
   * @return the action copy
   */
  public ActionCopy findStart() {
    for ( int i = 0; i < nrActions(); i++ ) {
      if ( getAction( i ).isStart() ) {
        return getAction( i );
      }
    }
    return null;
  }

  /**
   * Gets a textual representation of the workflow. If its name has been set, it will be returned, otherwise the classname is
   * returned.
   *
   * @return the textual representation of the workflow.
   */
  public String toString() {
    if ( !Utils.isEmpty( filename ) ) {
      if ( Utils.isEmpty( name ) ) {
        return filename;
      } else {
        return filename + " : " + name;
      }
    }

    if ( name != null ) {
      return name;
    } else {
      return WorkflowMeta.class.getName();
    }
  }

  /**
   * Gets the boolean value of batch id passed.
   *
   * @return Returns the batchIdPassed.
   */
  public boolean isBatchIdPassed() {
    return batchIdPassed;
  }

  /**
   * Sets the batch id passed.
   *
   * @param batchIdPassed The batchIdPassed to set.
   */
  public void setBatchIdPassed( boolean batchIdPassed ) {
    this.batchIdPassed = batchIdPassed;
  }

  public List<SqlStatement> getSqlStatements( IProgressMonitor monitor )
    throws HopException {
    return getSqlStatements( null, monitor );
  }

  /**
   * Builds a list of all the SQL statements that this pipeline needs in order to work properly.
   *
   * @return An ArrayList of SqlStatement objects.
   */
  public List<SqlStatement> getSqlStatements( IMetaStore metaStore,
                                              IProgressMonitor monitor ) throws HopException {
    if ( monitor != null ) {
      monitor
        .beginTask( BaseMessages.getString( PKG, "WorkflowMeta.Monitor.GettingSQLNeededForThisWorkflow" ), nrActions() + 1 );
    }
    List<SqlStatement> stats = new ArrayList<SqlStatement>();

    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy copy = getAction( i );
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "WorkflowMeta.Monitor.GettingSQLForActionCopy" ) + copy + "]" );
      }
      stats.addAll( copy.getEntry().getSqlStatements( metaStore, this ) );
      if ( monitor != null ) {
        monitor.worked( 1 );
      }
    }

    if ( monitor != null ) {
      monitor.worked( 1 );
    }
    if ( monitor != null ) {
      monitor.done();
    }

    return stats;
  }

  /**
   * Gets the arguments used for this workflow.
   *
   * @return Returns the arguments.
   * @deprecated Moved to the Workflow class
   */
  @Deprecated
  public String[] getArguments() {
    return arguments;
  }

  /**
   * Sets the arguments.
   *
   * @param arguments The arguments to set.
   * @deprecated moved to the workflow class
   */
  @Deprecated
  public void setArguments( String[] arguments ) {
    this.arguments = arguments;
  }

  /**
   * Get a list of all the strings used in this workflow.
   *
   * @return A list of StringSearchResult with strings used in the workflow
   */
  public List<StringSearchResult> getStringList( boolean searchTransforms, boolean searchDatabases, boolean searchNotes ) {
    List<StringSearchResult> stringList = new ArrayList<StringSearchResult>();

    if ( searchTransforms ) {
      // Loop over all transforms in the pipeline and see what the used
      // vars are...
      for ( int i = 0; i < nrActions(); i++ ) {
        ActionCopy entryMeta = getAction( i );
        stringList.add( new StringSearchResult( entryMeta.getName(), entryMeta, this,
          BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.ActionName" ) ) );
        if ( entryMeta.getDescription() != null ) {
          stringList.add( new StringSearchResult( entryMeta.getDescription(), entryMeta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.ActionDescription" ) ) );
        }
        IAction metaInterface = entryMeta.getEntry();
        StringSearcher.findMetaData( metaInterface, 1, stringList, entryMeta, this );
      }
    }

    // Loop over all transforms in the pipeline and see what the used vars
    // are...
    if ( searchDatabases ) {
      for ( DatabaseMeta meta : getDatabases() ) {
        stringList.add( new StringSearchResult( meta.getName(), meta, this,
          BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabaseConnectionName" ) ) );
        if ( meta.getHostname() != null ) {
          stringList.add( new StringSearchResult( meta.getHostname(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabaseHostName" ) ) );
        }
        if ( meta.getDatabaseName() != null ) {
          stringList.add( new StringSearchResult( meta.getDatabaseName(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabaseName" ) ) );
        }
        if ( meta.getUsername() != null ) {
          stringList.add( new StringSearchResult( meta.getUsername(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabaseUsername" ) ) );
        }
        if ( meta.getPluginId() != null ) {
          stringList.add( new StringSearchResult( meta.getPluginId(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabaseTypeDescription" ) ) );
        }
        if ( meta.getPort() != null ) {
          stringList.add( new StringSearchResult( meta.getPort(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabasePort" ) ) );
        }
        if ( meta.getServername() != null ) {
          stringList.add( new StringSearchResult( meta.getServername(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabaseServer" ) ) );
        }
        // if ( includePasswords )
        // {
        if ( meta.getPassword() != null ) {
          stringList.add( new StringSearchResult( meta.getPassword(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.DatabasePassword" ) ) );
          // }
        }
      }
    }

    // Loop over all transforms in the pipeline and see what the used vars
    // are...
    if ( searchNotes ) {
      for ( int i = 0; i < nrNotes(); i++ ) {
        NotePadMeta meta = getNote( i );
        if ( meta.getNote() != null ) {
          stringList.add( new StringSearchResult( meta.getNote(), meta, this,
            BaseMessages.getString( PKG, "WorkflowMeta.SearchMetadata.NotepadText" ) ) );
        }
      }
    }

    return stringList;
  }

  /**
   * Gets the used variables.
   *
   * @return the used variables
   */
  public List<String> getUsedVariables() {
    // Get the list of Strings.
    List<StringSearchResult> stringList = getStringList( true, true, false );

    List<String> varList = new ArrayList<>();

    // Look around in the strings, see what we find...
    for ( StringSearchResult result : stringList ) {
      StringUtil.getUsedVariables( result.getString(), varList, false );
    }

    return varList;
  }

  /**
   * Have actions changed.
   *
   * @return true, if successful
   */
  public boolean haveActionsChanged() {
    if ( changedActions ) {
      return true;
    }

    for ( int i = 0; i < nrActions(); i++ ) {
      ActionCopy action = getAction( i );
      if ( action.hasChanged() ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Have workflow hops changed.
   *
   * @return true, if successful
   */
  public boolean haveWorkflowHopsChanged() {
    if ( changedHops ) {
      return true;
    }

    for ( WorkflowHopMeta hi : workflowHops ) {
      // Look at all the hops

      if ( hi.hasChanged() ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the version of the workflow.
   *
   * @return The version of the workflow
   */
  public String getJobversion() {
    return workflowVersion;
  }

  /**
   * Gets the status of the workflow.
   *
   * @return the status of the workflow
   */
  public int getWorkflowStatus() {
    return workflowStatus;
  }

  /**
   * Set the version of the workflow.
   *
   * @param jobVersion The new version description of the workflow
   */
  public void setJobversion( String jobVersion ) {
    this.workflowVersion = jobVersion;
  }

  /**
   * Set the status of the workflow.
   *
   * @param jobStatus The new status description of the workflow
   */
  public void setWorkflowStatus( int jobStatus ) {
    this.workflowStatus = jobStatus;
  }

  /**
   * This method sets various internal kettle variables that can be used by the pipeline.
   */
  @Override
  public void setInternalHopVariables( IVariables var ) {
    setInternalFilenameHopVariables( var );
    setInternalNameHopVariable( var );

    variables.getVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY );
    updateCurrentDir();
  }

  // changed to protected for testing purposes
  //
  protected void updateCurrentDir() {
    String prevCurrentDir = variables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY );
    String currentDir = variables.getVariable(
      filename != null
        ? Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY
        : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY );
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, currentDir );
    fireCurrentDirectoryChanged( prevCurrentDir, currentDir );
  }

  /**
   * Sets the internal name kettle variable.
   *
   * @param var the new internal name kettle variable
   */
  @Override
  protected void setInternalNameHopVariable( IVariables var ) {
    // The name of the workflow
    variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_NAME, Const.NVL( name, "" ) );
  }

  /**
   * Sets the internal filename kettle variables.
   *
   * @param var the new internal filename kettle variables
   */
  @Override
  protected void setInternalFilenameHopVariables( IVariables var ) {
    if ( filename != null ) {
      // we have a filename that's defined.
      try {
        FileObject fileObject = HopVfs.getFileObject( filename, var );
        FileName fileName = fileObject.getName();

        // The filename of the workflow
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, fileName.getBaseName() );

        // The directory of the workflow
        FileName fileDir = fileName.getParent();
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, fileDir.getURI() );
      } catch ( Exception e ) {
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, "" );
        variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "" );
      }
    } else {
      variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, "" );
      variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "" );
    }

    setInternalEntryCurrentDirectory();

  }

  protected void setInternalEntryCurrentDirectory() {
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, variables.getVariable(
      filename != null
        ? Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY
        : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }

  @Deprecated
  public void checkActions( List<ICheckResult> remarks, boolean only_selected,
                            IProgressMonitor monitor ) {
    checkActions( remarks, only_selected, monitor, this, null );
  }

  /**
   * Check all actions within the workflow. Each Workflow Entry has the opportunity to check their own settings.
   *
   * @param remarks       List of CheckResult remarks inserted into by each Action
   * @param only_selected true if you only want to check the selected workflows
   * @param monitor       Progress monitor (not presently in use)
   */
  public void checkActions( List<ICheckResult> remarks, boolean only_selected,
                            IProgressMonitor monitor, IVariables variables, IMetaStore metaStore ) {
    remarks.clear(); // Empty remarks
    if ( monitor != null ) {
      monitor.beginTask( BaseMessages.getString( PKG, "WorkflowMeta.Monitor.VerifyingThisActionTask.Title" ),
        actionCopies.size() + 2 );
    }
    boolean stop_checking = false;
    for ( int i = 0; i < actionCopies.size() && !stop_checking; i++ ) {
      ActionCopy copy = actionCopies.get( i ); // get the action copy
      if ( ( !only_selected ) || ( only_selected && copy.isSelected() ) ) {
        IAction action = copy.getEntry();
        if ( action != null ) {
          if ( monitor != null ) {
            monitor
              .subTask( BaseMessages.getString( PKG, "WorkflowMeta.Monitor.VerifyingAction.Title", action.getName() ) );
          }
          action.check( remarks, this, variables, metaStore );
          if ( monitor != null ) {
            monitor.worked( 1 ); // progress bar...
            if ( monitor.isCanceled() ) {
              stop_checking = true;
            }
          }
        }
      }
      if ( monitor != null ) {
        monitor.worked( 1 );
      }
    }
    if ( monitor != null ) {
      monitor.done();
    }
  }

  /**
   * Gets the resource dependencies.
   *
   * @return the resource dependencies
   */
  public List<ResourceReference> getResourceDependencies() {
    List<ResourceReference> resourceReferences = new ArrayList<ResourceReference>();
    ActionCopy copy = null;
    IAction action = null;
    for ( int i = 0; i < actionCopies.size(); i++ ) {
      copy = actionCopies.get( i ); // get the action copy
      action = copy.getEntry();
      resourceReferences.addAll( action.getResourceDependencies( this ) );
    }

    return resourceReferences;
  }

  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming namingInterface, IMetaStore metaStore ) throws HopException {
    String resourceName = null;
    try {
      // Handle naming for XML bases resources...
      //
      String baseName;
      String originalPath;
      String fullname;
      String extension = "hwf";
      if ( StringUtils.isNotEmpty( getFilename() ) ) {
        FileObject fileObject = HopVfs.getFileObject( variables.environmentSubstitute( getFilename() ), variables );
        originalPath = fileObject.getParent().getName().getPath();
        baseName = fileObject.getName().getBaseName();
        fullname = fileObject.getName().getPath();

        resourceName = namingInterface.nameResource( baseName, originalPath, extension, IResourceNaming.FileNamingType.JOB );
        ResourceDefinition definition = definitions.get( resourceName );
        if ( definition == null ) {
          // If we do this once, it will be plenty :-)
          //
          WorkflowMeta workflowMeta = (WorkflowMeta) this.realClone( false );

          // Add used resources, modify pipelineMeta accordingly
          // Go through the list of transforms, etc.
          // These critters change the transforms in the cloned PipelineMeta
          // At the end we make a new XML version of it in "exported"
          // format...

          // loop over transforms, databases will be exported to XML anyway.
          //
          for ( ActionCopy jobEntry : workflowMeta.actionCopies ) {
            jobEntry.getEntry().exportResources( workflowMeta, definitions, namingInterface, metaStore );
          }

          // Set a number of parameters for all the data files referenced so far...
          //
          Map<String, String> directoryMap = namingInterface.getDirectoryMap();
          if ( directoryMap != null ) {
            for ( String directory : directoryMap.keySet() ) {
              String parameterName = directoryMap.get( directory );
              workflowMeta.addParameterDefinition( parameterName, directory, "Data file path discovered during export" );
            }
          }

          // At the end, add ourselves to the map...
          //
          String jobMetaContent = workflowMeta.getXml();

          definition = new ResourceDefinition( resourceName, jobMetaContent );

          // Also remember the original filename (if any), including variables etc.
          //
          if ( Utils.isEmpty( this.getFilename() ) ) {
            definition.setOrigin( fullname );
          } else {
            definition.setOrigin( this.getFilename() );
          }

          definitions.put( fullname, definition );
        }
      }
    } catch ( FileSystemException e ) {
      throw new HopException(
        BaseMessages.getString( PKG, "WorkflowMeta.Exception.AnErrorOccuredReadingWorkflow", getFilename() ), e );
    } catch ( HopFileException e ) {
      throw new HopException(
        BaseMessages.getString( PKG, "WorkflowMeta.Exception.AnErrorOccuredReadingWorkflow", getFilename() ), e );
    }

    return resourceName;
  }


  /**
   * See if the name of the supplied action copy doesn't collide with any other action copy in the workflow.
   *
   * @param je The action copy to verify the name for.
   */
  public void renameActionIfNameCollides( ActionCopy je ) {
    // First see if the name changed.
    // If so, we need to verify that the name is not already used in the
    // workflow.
    //
    String newname = je.getName();

    // See if this name exists in the other actions
    //
    boolean found;
    int nr = 1;
    do {
      found = false;
      for ( ActionCopy copy : actionCopies ) {
        if ( copy != je && copy.getName().equalsIgnoreCase( newname ) && copy.getNr() == 0 ) {
          found = true;
        }
      }
      if ( found ) {
        nr++;
        newname = je.getName() + " (" + nr + ")";
      }
    } while ( found );

    // Rename if required.
    //
    je.setName( newname );
  }

  /**
   * Gets the workflow copies.
   *
   * @return the workflow copies
   */
  public List<ActionCopy> getJobCopies() {
    return actionCopies;
  }

  /**
   * Gets the jobhops.
   *
   * @return the jobhops
   */
  public List<WorkflowHopMeta> getWorkflowHops() {
    return workflowHops;
  }

  /**
   * Gets the log channel.
   *
   * @return the log channel
   */
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Create a unique list of action interfaces
   *
   * @return
   */
  public List<IAction> composeActionList() {
    List<IAction> list = new ArrayList<IAction>();

    for ( ActionCopy copy : actionCopies ) {
      if ( !list.contains( copy.getEntry() ) ) {
        list.add( copy.getEntry() );
      }
    }

    return list;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getLogChannelId()
   */
  public String getLogChannelId() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectType()
   */
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.WORKFLOW_META;
  }
  
  /**
   * Returns whether or not the workflow is gathering metrics. For a WorkflowMeta this is always false.
   *
   * @return is gathering metrics = false;
   */
  @Override
  public boolean isGatheringMetrics() {
    return false;
  }

  /**
   * Sets whether or not the workflow is gathering metrics. This is a stub with not executable code.
   */
  @Override
  public void setGatheringMetrics( boolean gatheringMetrics ) {
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return false;
  }

  @Override
  public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
  }

  public boolean containsJobCopy( ActionCopy actionCopy ) {
    return actionCopies.contains( actionCopy );
  }

  public List<MissingAction> getMissingActions() {
    return missingActions;
  }

  public void addMissingAction( MissingAction missingAction ) {
    if ( missingActions == null ) {
      missingActions = new ArrayList<MissingAction>();
    }
    missingActions.add( missingAction );
  }

  public void removeMissingAction( MissingAction missingAction ) {
    if ( missingActions != null && missingAction != null && missingActions.contains( missingAction ) ) {
      missingActions.remove( missingAction );
    }
  }

  public boolean hasMissingPlugins() {
    return missingActions != null && !missingActions.isEmpty();
  }

  public String getStartCopyName() {
    return startCopyName;
  }

  public void setStartCopyName( String startCopyName ) {
    this.startCopyName = startCopyName;
  }

  public boolean isExpandingRemoteWorkflow() {
    return expandingRemoteWorkflow;
  }

  public void setExpandingRemoteWorkflow( boolean expandingRemoteWorkflow ) {
    this.expandingRemoteWorkflow = expandingRemoteWorkflow;
  }
}
