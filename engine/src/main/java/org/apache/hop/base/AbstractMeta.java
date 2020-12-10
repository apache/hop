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

package org.apache.hop.base;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.IAttributes;
import org.apache.hop.core.IEngineMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.changed.ChangedFlag;
import org.apache.hop.core.changed.IChanged;
import org.apache.hop.core.changed.IHopObserver;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.IUndo;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.listeners.ICurrentDirectoryChangedListener;
import org.apache.hop.core.listeners.IFilenameChangedListener;
import org.apache.hop.core.listeners.INameChangedListener;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractMeta implements IChanged, IUndo, IEngineMeta, INamedParameterDefinitions, IAttributes, ILoggingObject {

  /**
   * Constant = 1
   **/
  public static final int TYPE_UNDO_CHANGE = 1;

  /**
   * Constant = 2
   **/
  public static final int TYPE_UNDO_NEW = 2;

  /**
   * Constant = 3
   **/
  public static final int TYPE_UNDO_DELETE = 3;

  /**
   * Constant = 4
   **/
  public static final int TYPE_UNDO_POSITION = 4;

  protected String containerObjectId;

  protected String name;

  protected boolean nameSynchronizedWithFilename;

  protected String description;

  protected String extendedDescription;

  protected String filename;

  protected Set<INameChangedListener> nameChangedListeners = Collections.newSetFromMap( new ConcurrentHashMap<>() );

  protected Set<IFilenameChangedListener> filenameChangedListeners = Collections.newSetFromMap( new ConcurrentHashMap<>() );

  protected Set<IContentChangedListener> contentChangedListeners = Collections.newSetFromMap( new ConcurrentHashMap<>() );

  protected Set<ICurrentDirectoryChangedListener> currentDirectoryChangedListeners = Collections.newSetFromMap( new ConcurrentHashMap<>() );

  protected List<NotePadMeta> notes;

  protected boolean changedNotes;

  protected List<ChangeAction> undo;

  protected Map<String, Map<String, String>> attributesMap;

  protected INamedParameters namedParams = new NamedParameters();

  protected LogLevel logLevel = DefaultLogLevel.getLogLevel();

  protected IHopMetadataProvider metadataProvider;

  protected String createdUser, modifiedUser;

  protected Date createdDate, modifiedDate;

  protected final ChangedFlag changedFlag = new ChangedFlag();

  protected int maxUndo;

  protected int undoPosition;

  protected RunOptions runOptions = new RunOptions();

  private boolean showDialog = true;
  private boolean alwaysShowRunOptions = true;

  private Boolean versioningEnabled;

  public boolean isShowDialog() {
    return showDialog;
  }

  public void setShowDialog( boolean showDialog ) {
    this.showDialog = showDialog;
  }

  public boolean isAlwaysShowRunOptions() {
    return alwaysShowRunOptions;
  }

  public void setAlwaysShowRunOptions( boolean alwaysShowRunOptions ) {
    this.alwaysShowRunOptions = alwaysShowRunOptions;
  }

  /**
   * Gets the container object id.
   *
   * @return the serverObjectId
   */
  @Override
  public String getContainerId() {
    return containerObjectId;
  }

  /**
   * Sets the carte object id.
   *
   * @param containerObjectId the execution container Object id to set
   */
  public void setCarteObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  protected abstract String getExtension();

  /**
   * Get the name of the pipeline. If the name is synchronized with the filename, we return the base filename.
   *
   * @return The name of the pipeline
   */
  @Override
  public String getName() {
    return extractNameFromFilename( nameSynchronizedWithFilename, name, filename, getExtension() );
  }

  public static final String extractNameFromFilename( boolean sync, String name, String filename, String extension ) {
    if ( filename == null ) {
      return name;
    } else {
      if ( sync ) {
        int lastExtIndex = filename.toLowerCase().lastIndexOf( extension );
        if ( lastExtIndex < 0 ) {
          lastExtIndex = filename.length();
        }

        // Get the last / or \ in a filename
        //
        int lastSlashIndex = filename.lastIndexOf( '/' );
        if ( lastSlashIndex < 0 ) {
          lastSlashIndex = filename.lastIndexOf( '\\' );
        }
        if ( lastSlashIndex < 0 ) {
          lastSlashIndex = -1;
        }

        return filename.substring( lastSlashIndex + 1, lastExtIndex );

      } else {
        return name;
      }
    }
  }

  /**
   * Set the name.
   *
   * @param newName The new name
   */
  public void setName( String newName ) {
    fireNameChangedListeners( this.name, newName );
    this.name = newName;
  }

  /**
   * Gets nameSynchronizedWithFilename
   *
   * @return value of nameSynchronizedWithFilename
   */
  public boolean isNameSynchronizedWithFilename() {
    return nameSynchronizedWithFilename;
  }

  /**
   * @param nameSynchronizedWithFilename The nameSynchronizedWithFilename to set
   */
  public void setNameSynchronizedWithFilename( boolean nameSynchronizedWithFilename ) {
    this.nameSynchronizedWithFilename = nameSynchronizedWithFilename;
  }

  /**
   * Gets the description of the workflow.
   *
   * @return The description of the workflow
   */
  public String getDescription() {
    return description;
  }

  /**
   * Set the description of the workflow.
   *
   * @param description The new description of the workflow
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets the extended description of the workflow.
   *
   * @return The extended description of the workflow
   */
  public String getExtendedDescription() {
    return extendedDescription;
  }

  /**
   * Set the description of the workflow.
   *
   * @param extendedDescription The new extended description of the workflow
   */
  public void setExtendedDescription( String extendedDescription ) {
    this.extendedDescription = extendedDescription;
  }

  /**
   * Builds a name - if no name is set, yet - from the filename
   */
  @Override
  public void nameFromFilename() {
    if ( !Utils.isEmpty( filename ) ) {
      setName( Const.createName( filename ) );
    }
  }

  /**
   * Gets the filename.
   *
   * @return filename
   * @see IEngineMeta#getFilename()
   */
  @Override
  public String getFilename() {
    return filename;
  }

  /**
   * Set the filename of the workflow
   *
   * @param newFilename The new filename of the workflow
   */
  @Override
  public void setFilename( String newFilename ) {
    fireFilenameChangedListeners( this.filename, newFilename );
    this.filename = newFilename;
  }


  /**
   * This method sets various internal hop variables.
   */
  public abstract void setInternalHopVariables( IVariables var );

  /**
   * Sets the internal filename hop variables.
   *
   * @param var the new internal filename hop variables
   */
  protected abstract void setInternalFilenameHopVariables( IVariables var );

  /**
   * Find a database connection by it's name
   *
   * @param name The database name to look for
   * @return The database connection or null if nothing was found.
   */
  public DatabaseMeta findDatabase( String name ) {
    if ( metadataProvider == null || StringUtils.isEmpty( name ) ) {
      return null;
    }
    try {
      DatabaseMeta databaseMeta = metadataProvider.getSerializer( DatabaseMeta.class ).load( name );
      return databaseMeta;
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to load database with name '" + name + "' from the metadata", e );
    }
  }

  public int nrDatabases() {
    try {
      return metadataProvider.getSerializer( DatabaseMeta.class ).listObjectNames().size();
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to load database with name '" + name + "' from the metadata", e );
    }
  }

  /**
   * Adds the name changed listener.
   *
   * @param listener the listener
   */
  public void addNameChangedListener( INameChangedListener listener ) {
    if ( listener != null ) {
      nameChangedListeners.add( listener );
    }
  }

  /**
   * Removes the name changed listener.
   *
   * @param listener the listener
   */
  public void removeNameChangedListener( INameChangedListener listener ) {
    if ( listener != null ) {
      nameChangedListeners.remove( listener );
    }
  }

  /**
   * Removes all the name changed listeners
   */
  public void clearNameChangedListeners() {
    nameChangedListeners.clear();
  }

  /**
   * Fire name changed listeners.
   *
   * @param oldName the old name
   * @param newName the new name
   */
  protected void fireNameChangedListeners( String oldName, String newName ) {
    if ( nameChanged( oldName, newName ) ) {
      for ( INameChangedListener listener : nameChangedListeners ) {
        listener.nameChanged( this, oldName, newName );
      }
    }
  }

  /**
   * Adds the filename changed listener.
   *
   * @param listener the listener
   */
  public void addFilenameChangedListener( IFilenameChangedListener listener ) {
    if ( listener != null ) {
      filenameChangedListeners.add( listener );
    }
  }

  /**
   * Removes the filename changed listener.
   *
   * @param listener the listener
   */
  public void removeFilenameChangedListener( IFilenameChangedListener listener ) {
    if ( listener != null ) {
      filenameChangedListeners.remove( listener );
    }
  }

  /**
   * Fire filename changed listeners.
   *
   * @param oldFilename the old filename
   * @param newFilename the new filename
   */
  protected void fireFilenameChangedListeners( String oldFilename, String newFilename ) {
    if ( nameChanged( oldFilename, newFilename ) ) {
      for ( IFilenameChangedListener listener : filenameChangedListeners ) {
        listener.filenameChanged( this, oldFilename, newFilename );
      }
    }
  }

  /**
   * Adds the passed IContentChangedListener to the list of listeners.
   *
   * @param listener
   */
  public void addContentChangedListener( IContentChangedListener listener ) {
    if ( listener != null ) {
      contentChangedListeners.add( listener );
    }
  }

  /**
   * Removes the passed IContentChangedListener from the list of listeners.
   *
   * @param listener
   */
  public void removeContentChangedListener( IContentChangedListener listener ) {
    if ( listener != null ) {
      contentChangedListeners.remove( listener );
    }
  }

  public List<IContentChangedListener> getContentChangedListeners() {
    return ImmutableList.copyOf( contentChangedListeners );
  }

  /**
   * Fire content changed listeners.
   */
  protected void fireContentChangedListeners() {
    fireContentChangedListeners( true );
  }

  protected void fireContentChangedListeners( boolean ch ) {
    if ( ch ) {
      for ( IContentChangedListener listener : contentChangedListeners ) {
        listener.contentChanged( this );
      }
    } else {
      for ( IContentChangedListener listener : contentChangedListeners ) {
        listener.contentSafe( this );
      }
    }
  }

  /**
   * Remove listener
   */
  public void addCurrentDirectoryChangedListener( ICurrentDirectoryChangedListener listener ) {
    if ( listener != null && !currentDirectoryChangedListeners.contains( listener ) ) {
      currentDirectoryChangedListeners.add( listener );
    }
  }

  /**
   * Add a listener to be notified of design-time changes to current directory variable
   */
  public void removeCurrentDirectoryChangedListener( ICurrentDirectoryChangedListener listener ) {
    if ( listener != null ) {
      currentDirectoryChangedListeners.remove( listener );
    }
  }

  /**
   * Notify listeners of a change in current directory.
   */
  protected void fireCurrentDirectoryChanged( String previous, String current ) {
    if ( nameChanged( previous, current ) ) {
      for ( ICurrentDirectoryChangedListener listener : currentDirectoryChangedListeners ) {
        listener.directoryChanged( this, previous, current );
      }
    }
  }


  /**
   * Find a hop server using the name
   *
   * @param serverString the name of the hop server
   * @return the hop server or null if we couldn't spot an approriate entry.
   */
  public HopServer findHopServer( String serverString ) {
    if ( metadataProvider == null || StringUtils.isEmpty( name ) ) {
      return null;
    }
    try {
      return metadataProvider.getSerializer( HopServer.class ).load( name );
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to load hop server with name '" + name + "' from the metadata", e );
    }
  }

  /**
   * Gets an array of hop server names.
   *
   * @return An array list hop server names
   */
  public String[] getHopServerNames() {
    try {
      List<String> names = metadataProvider.getSerializer( HopServer.class ).listObjectNames();
      Collections.sort( names );
      return names.toArray( new String[ 0 ] );
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to get hop server names from the metadata", e );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#addUndo(java.lang.Object[], java.lang.Object[], int[],
   * org.apache.hop.core.gui.Point[], org.apache.hop.core.gui.Point[], int, boolean)
   */
  @Override
  public void addUndo( Object[] from, Object[] to, int[] pos, Point[] prev, Point[] curr, int typeOfChange,
                       boolean nextAlso ) {
    // First clean up after the current position.
    // Example: position at 3, size=5
    // 012345
    // ^
    // remove 34
    // Add 4
    // 01234

    while ( undo.size() > undoPosition + 1 && undo.size() > 0 ) {
      int last = undo.size() - 1;
      undo.remove( last );
    }

    ChangeAction ta = new ChangeAction();
    switch ( typeOfChange ) {
      case TYPE_UNDO_CHANGE:
        ta.setChanged( from, to, pos );
        break;
      case TYPE_UNDO_DELETE:
        ta.setDelete( from, pos );
        break;
      case TYPE_UNDO_NEW:
        ta.setNew( from, pos );
        break;
      case TYPE_UNDO_POSITION:
        ta.setPosition( from, pos, prev, curr );
        break;
      default:
        break;
    }
    undo.add( ta );
    undoPosition++;

    if ( undo.size() > maxUndo ) {
      undo.remove( 0 );
      undoPosition--;
    }
  }

  /**
   * Clear undo.
   */
  public void clearUndo() {
    undo = new ArrayList<>();
    undoPosition = -1;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#nextUndo()
   */
  @Override
  public ChangeAction nextUndo() {
    int size = undo.size();
    if ( size == 0 || undoPosition >= size - 1 ) {
      return null; // no redo left...
    }

    undoPosition++;

    ChangeAction retval = undo.get( undoPosition );

    return retval;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#viewNextUndo()
   */
  @Override
  public ChangeAction viewNextUndo() {
    int size = undo.size();
    if ( size == 0 || undoPosition >= size - 1 ) {
      return null; // no redo left...
    }

    ChangeAction retval = undo.get( undoPosition + 1 );

    return retval;
  }

  // get previous undo, change position
  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#previousUndo()
   */
  @Override
  public ChangeAction previousUndo() {
    if ( undo.isEmpty() || undoPosition < 0 ) {
      return null; // No undo left!
    }

    ChangeAction retval = undo.get( undoPosition );

    undoPosition--;

    return retval;
  }

  /**
   * View current undo, don't change undo position
   *
   * @return The current undo transaction
   */
  @Override
  public ChangeAction viewThisUndo() {
    if ( undo.isEmpty() || undoPosition < 0 ) {
      return null; // No undo left!
    }

    ChangeAction retval = undo.get( undoPosition );

    return retval;
  }

  // View previous undo, don't change position
  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#viewPreviousUndo()
   */
  @Override
  public ChangeAction viewPreviousUndo() {
    if ( undo.isEmpty() || undoPosition < 0 ) {
      return null; // No undo left!
    }

    ChangeAction retval = undo.get( undoPosition );

    return retval;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#getMaxUndo()
   */
  @Override
  public int getMaxUndo() {
    return maxUndo;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.gui.IUndo#setMaxUndo(int)
   */
  @Override
  public void setMaxUndo( int mu ) {
    maxUndo = mu;
    while ( undo.size() > mu && undo.size() > 0 ) {
      undo.remove( 0 );
    }
  }

  /**
   * Gets the undo size.
   *
   * @return the undo size
   */
  public int getUndoSize() {
    if ( undo == null ) {
      return 0;
    }
    return undo.size();
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

  /**
   * Add a new note at a certain location (i.e. the specified index). Also marks that the notes have changed.
   *
   * @param p  The index into the notes list
   * @param ni The note to be added.
   */
  public void addNote( int p, NotePadMeta ni ) {
    notes.add( p, ni );
    changedNotes = true;
  }

  /**
   * Add a new note. Also marks that the notes have changed.
   *
   * @param ni The note to be added.
   */
  public void addNote( NotePadMeta ni ) {
    notes.add( ni );
    changedNotes = true;
  }

  /**
   * Find the note that is located on a certain point on the canvas.
   *
   * @param x the x-coordinate of the point queried
   * @param y the y-coordinate of the point queried
   * @return The note information if a note is located at the point. Otherwise, if nothing was found: null.
   */
  public NotePadMeta getNote( int x, int y ) {
    int i, s;
    s = notes.size();
    for ( i = s - 1; i >= 0; i-- ) {
      // Back to front because drawing goes from start to end

      NotePadMeta ni = notes.get( i );
      Point loc = ni.getLocation();
      Point p = new Point( loc.x, loc.y );
      if ( x >= p.x && x <= p.x + ni.width + 2 * Const.NOTE_MARGIN && y >= p.y
        && y <= p.y + ni.height + 2 * Const.NOTE_MARGIN ) {
        return ni;
      }
    }
    return null;
  }

  /**
   * Gets the note.
   *
   * @param i the i
   * @return the note
   */
  public NotePadMeta getNote( int i ) {
    return notes.get( i );
  }

  /**
   * Gets the notes.
   *
   * @return the notes
   */
  public List<NotePadMeta> getNotes() {
    return notes;
  }

  /**
   * Gets a list of all selected notes.
   *
   * @return A list of all the selected notes.
   */
  public List<NotePadMeta> getSelectedNotes() {
    List<NotePadMeta> selection = new ArrayList<>();
    for ( NotePadMeta note : notes ) {
      if ( note.isSelected() ) {
        selection.add( note );
      }
    }
    return selection;
  }

  /**
   * Finds the location (index) of the specified note.
   *
   * @param ni The note queried
   * @return The location of the note, or -1 if nothing was found.
   */
  public int indexOfNote( NotePadMeta ni ) {
    return notes.indexOf( ni );
  }

  /**
   * Lowers a note to the "bottom" of the list by removing the note at the specified index and re-inserting it at the
   * front. Also marks that the notes have changed.
   *
   * @param p the index into the notes list.
   */
  public void lowerNote( int p ) {
    // if valid index and not first index
    if ( ( p > 0 ) && ( p < notes.size() ) ) {
      NotePadMeta note = notes.remove( p );
      notes.add( 0, note );
      changedNotes = true;
    }
  }

  /**
   * Gets the number of notes.
   *
   * @return The number of notes.
   */
  public int nrNotes() {
    return notes.size();
  }

  /**
   * Raises a note to the "top" of the list by removing the note at the specified index and re-inserting it at the end.
   * Also marks that the notes have changed.
   *
   * @param p the index into the notes list.
   */
  public void raiseNote( int p ) {
    // if valid index and not last index
    if ( ( p >= 0 ) && ( p < notes.size() - 1 ) ) {
      NotePadMeta note = notes.remove( p );
      notes.add( note );
      changedNotes = true;
    }
  }

  /**
   * Removes a note at a certain location (i.e. the specified index). Also marks that the notes have changed.
   *
   * @param i The index into the notes list
   */
  public void removeNote( int i ) {
    if ( i < 0 || i >= notes.size() ) {
      return;
    }
    notes.remove( i );
    changedNotes = true;
  }

  /**
   * Checks whether or not any of the notes have been changed.
   *
   * @return true if the notes have been changed, false otherwise
   */
  public boolean haveNotesChanged() {
    if ( changedNotes ) {
      return true;
    }

    for ( int i = 0; i < nrNotes(); i++ ) {
      NotePadMeta note = getNote( i );
      if ( note.hasChanged() ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get an array of the locations of an array of notes
   *
   * @param notes An array of notes
   * @return an array of the locations of an array of notes
   */
  public int[] getNoteIndexes( List<NotePadMeta> notes ) {
    int[] retval = new int[ notes.size() ];

    for ( int i = 0; i < notes.size(); i++ ) {
      retval[ i ] = indexOfNote( notes.get( i ) );
    }

    return retval;
  }

  /**
   * Returns a list of the databases.
   *
   * @return Returns the databases.
   */
  public List<DatabaseMeta> getDatabases() {
    try {
      return metadataProvider.getSerializer( DatabaseMeta.class ).loadAll();
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to load databases from the metadata", e );
    }
  }

  /**
   * Gets the database names.
   *
   * @return the database names
   */
  public String[] getDatabaseNames() {
    try {
      List<String> names = metadataProvider.getSerializer( DatabaseMeta.class ).listObjectNames();
      Collections.sort( names );
      return names.toArray( new String[ 0 ] );
    } catch ( HopException e ) {
      throw new RuntimeException( "Unable to get database names from the metadata", e );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#addParameterDefinition(java.lang.String, java.lang.String,
   * java.lang.String)
   */
  @Override
  public void addParameterDefinition( String key, String defValue, String description ) throws DuplicateParamException {
    namedParams.addParameterDefinition( key, defValue, description );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDescription(java.lang.String)
   */
  @Override
  public String getParameterDescription( String key ) throws UnknownParamException {
    return namedParams.getParameterDescription( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#getParameterDefault(java.lang.String)
   */
  @Override
  public String getParameterDefault( String key ) throws UnknownParamException {
    return namedParams.getParameterDefault( key );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#listParameters()
   */
  @Override
  public String[] listParameters() {
    return namedParams.listParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.parameters.INamedParameters#eraseParameters()
   */
  @Override
  public void removeAllParameters() {
    namedParams.removeAllParameters();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getLogLevel()
   */
  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * Sets the log level.
   *
   * @param logLevel the new log level
   */
  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }


  /**
   * Sets the internal name hop variable.
   *
   * @param var the new internal name hop variable
   */
  protected abstract void setInternalNameHopVariable( IVariables var );

  /**
   * Gets the date the pipeline was created.
   *
   * @return the date the pipeline was created.
   */
  @Override
  public Date getCreatedDate() {
    return createdDate;
  }

  /**
   * Sets the date the pipeline was created.
   *
   * @param createdDate The creation date to set.
   */
  @Override
  public void setCreatedDate( Date createdDate ) {
    this.createdDate = createdDate;
  }

  /**
   * Sets the user by whom the pipeline was created.
   *
   * @param createdUser The user to set.
   */
  @Override
  public void setCreatedUser( String createdUser ) {
    this.createdUser = createdUser;
  }

  /**
   * Gets the user by whom the pipeline was created.
   *
   * @return the user by whom the pipeline was created.
   */
  @Override
  public String getCreatedUser() {
    return createdUser;
  }

  /**
   * Sets the date the pipeline was modified.
   *
   * @param modifiedDate The modified date to set.
   */
  @Override
  public void setModifiedDate( Date modifiedDate ) {
    this.modifiedDate = modifiedDate;
  }

  /**
   * Gets the date the pipeline was modified.
   *
   * @return the date the pipeline was modified.
   */
  @Override
  public Date getModifiedDate() {
    return modifiedDate;
  }

  /**
   * Sets the user who last modified the pipeline.
   *
   * @param modifiedUser The user name to set.
   */
  @Override
  public void setModifiedUser( String modifiedUser ) {
    this.modifiedUser = modifiedUser;
  }

  /**
   * Gets the user who last modified the pipeline.
   *
   * @return the user who last modified the pipeline.
   */
  @Override
  public String getModifiedUser() {
    return modifiedUser;
  }

  public void clear() {
    setName( null );
    setFilename( null );
    notes = new ArrayList<>();
    attributesMap = new HashMap<>();
    maxUndo = Const.MAX_UNDO;
    clearUndo();
    clearChanged();
    setChanged( false );

    createdUser = "-";
    createdDate = new Date();

    modifiedUser = "-";
    modifiedDate = new Date();
    description = null;
    extendedDescription = null;
  }

  @Override
  public void clearChanged() {
    changedNotes = false;
    for ( int i = 0; i < nrNotes(); i++ ) {
      getNote( i ).setChanged( false );
    }
    changedFlag.clearChanged();
    fireContentChangedListeners( false );
  }

  @Override
  public void setChanged() {
    changedFlag.setChanged();
    fireContentChangedListeners( true );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.changed.ChangedFlag#setChanged(boolean)
   */
  @Override
  public final void setChanged( boolean ch ) {
    if ( ch ) {
      setChanged();
    } else {
      clearChanged();
    }
  }

  public void addObserver( IHopObserver o ) {
    changedFlag.addObserver( o );
  }

  public void deleteObserver( IHopObserver o ) {
    changedFlag.deleteObserver( o );
  }

  public void notifyObservers( Object arg ) {
    changedFlag.notifyObservers( arg );
  }

  /**
   * Checks whether the workflow can be saved. For WorkflowMeta, this method always returns true
   *
   * @return true
   * @see IEngineMeta#canSave()
   */
  @Override
  public boolean canSave() {
    return true;
  }

  @Override
  public boolean hasChanged() {
    if ( changedFlag.hasChanged() ) {
      return true;
    }
    if ( haveNotesChanged() ) {
      return true;
    }
    return false;
  }

  /**
   * Gets the registration date for the pipeline. For AbstractMeta, this method always returns null.
   *
   * @return null
   */
  @Override
  public Date getRegistrationDate() {
    return null;
  }

  /**
   * Gets the interface to the parent log object. For AbstractMeta, this method always returns null.
   *
   * @return null
   * @see ILoggingObject#getParent()
   */
  @Override
  public ILoggingObject getParent() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectName()
   */
  @Override
  public String getObjectName() {
    return getName();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectCopy()
   */
  @Override
  public String getObjectCopy() {
    return null;
  }

  /**
   * Checks whether the specified name has changed (i.e. is different from the specified old name). If both names are
   * null, false is returned. If the old name is null and the new new name is non-null, true is returned. Otherwise, if
   * the name strings are equal then true is returned; false is returned if the name strings are not equal.
   *
   * @param oldName the old name
   * @param newName the new name
   * @return true if the names have changed, false otherwise
   */
  private boolean nameChanged( String oldName, String newName ) {
    if ( oldName == null && newName == null ) {
      return false;
    }
    if ( oldName == null && newName != null ) {
      return true;
    }
    return !oldName.equals( newName );
  }

  public boolean hasMissingPlugins() {
    return false;
  }

  protected int compare( AbstractMeta meta1, AbstractMeta meta2 ) {
    // If we don't have a filename...
    //
    if ( StringUtils.isEmpty( meta1.getFilename() ) && StringUtils.isNotEmpty( meta2.getFilename() ) ) {
      return -1;
    }
    if ( StringUtils.isNotEmpty( meta1.getFilename() ) && StringUtils.isEmpty( meta2.getFilename() ) ) {
      return 1;
    }
    if ( ( StringUtils.isEmpty( meta1.getFilename() ) && StringUtils.isEmpty( meta2.getFilename() )
      || ( meta1.getFilename().equals( meta2.getFilename() ) ) )
    ) {
      // Compare names...
      //
      if ( Utils.isEmpty( meta1.getName() ) && !Utils.isEmpty( meta2.getName() ) ) {
        return -1;
      }
      if ( !Utils.isEmpty( meta1.getName() ) && Utils.isEmpty( meta2.getName() ) ) {
        return 1;
      }
      int cmpName = meta1.getName().compareTo( meta2.getName() );
      return cmpName;
    } else {
      return meta1.getFilename().compareTo( meta2.getFilename() );
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash( filename, name );
  }

  private static class RunOptions {
    boolean clearingLog;
    boolean safeModeEnabled;

    RunOptions() {
      clearingLog = true;
      safeModeEnabled = false;
    }
  }

  public boolean isClearingLog() {
    return runOptions.clearingLog;
  }

  public void setClearingLog( boolean clearingLog ) {
    this.runOptions.clearingLog = clearingLog;
  }

  public boolean isSafeModeEnabled() {
    return runOptions.safeModeEnabled;
  }

  public void setSafeModeEnabled( boolean safeModeEnabled ) {
    this.runOptions.safeModeEnabled = safeModeEnabled;
  }
}
