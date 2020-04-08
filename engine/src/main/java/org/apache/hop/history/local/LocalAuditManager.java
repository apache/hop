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

package org.apache.hop.history.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.IAuditManager;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The local audit manager stores its history in the hop home directory (~/.hop) under the history folder
 * So : $HOME/.hop/history
 * <p>
 * It will be done using a new metastore.
 * - Event groups are mapped to namespaces
 * -
 */
public class LocalAuditManager implements IAuditManager {

  private String rootFolder;

  public LocalAuditManager() {
    this( Const.getHopDirectory() + File.separator + "history" );
  }

  public LocalAuditManager( String rootFolder ) {
    this.rootFolder = rootFolder;
  }

  @Override public void storeEvent( AuditEvent event ) throws HopException {
    validateEvent( event );
    writeEvent( event );
  }


  private void writeEvent( AuditEvent event ) throws HopException {
    String filename = calculateEventFilename( event );
    try {
      File file = new File( filename );
      File parentFolder = file.getParentFile();
      if ( !parentFolder.exists() ) {
        parentFolder.mkdirs();
      }

      // write the event to JSON...
      //
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue( new File( filename ), event );

      // TODO: clean up old events
      //
    } catch ( IOException e ) {
      throw new HopException( "Unable to write event to filename '" + filename + "'", e );
    }
  }

  private String calculateEventFilename( AuditEvent event ) {
    String typePath = calculateTypePath( event.getGroup(), event.getType() );
    SimpleDateFormat format = new SimpleDateFormat( "yyyyMMdd-HHmmss.SSS" );
    String filename = format.format( event.getDate() ) + "-" + event.getOperation() + ".event";
    return typePath + File.separator + filename;
  }

  private String calculateGroupPath( String group ) {
    return rootFolder + File.separator + group ;
  }

  private String calculateTypePath( String group, String type ) {
    return calculateGroupPath( group ) + File.separator + type;
  }

  @Override public List<AuditEvent> findEvents( String group, String type ) throws HopException {
    if ( StringUtils.isEmpty( group ) ) {
      throw new HopException( "You need to specify a group to find events" );
    }
    if ( StringUtils.isEmpty( type ) ) {
      throw new HopException( "You need to specify a type to find events" );
    }

    ArrayList<AuditEvent> events = new ArrayList<>();

    String folderPath = calculateTypePath( group, type );
    File folder = new File( folderPath );
    if ( !folder.exists() ) {
      return events;
    }

    // JSON to event mapper...
    //
    ObjectMapper mapper = new ObjectMapper();

    File[] eventFiles = folder.listFiles( ( dir, name ) -> name.endsWith( "event" ) );
    for ( File eventFile : eventFiles ) {
      try {
        AuditEvent event = mapper.readValue( eventFile, AuditEvent.class );
        events.add(event);
      } catch ( IOException e ) {
        throw new HopException( "Error reading event file '" + eventFile.toString() + "'", e );
      }
    }

    // Sort the events by date descending (most recent first)
    //
    Collections.sort( events, Comparator.comparing( AuditEvent::getDate ).reversed() );

    return events;

  }

  @Override public void storeList( AuditList auditList ) throws HopException {
    validateList( auditList );
    String filename = calculateGroupPath( auditList.getGroup() ) + File.separator + auditList.getType() + ".list";
    File file = new File(filename);
    File folder = file.getParentFile();
    if (!folder.exists()) {
      folder.mkdirs();
    }
    try {
      new ObjectMapper().writeValue( file, auditList );
    } catch(IOException e) {
      throw new HopException( "It was not possible to write to audit list file '"+filename+"'", e );
    }
  }

  @Override public AuditList retrieveList( String group, String type ) throws HopException {
    if ( StringUtils.isEmpty( group ) ) {
      throw new HopException( "You need a group before you can retrieve an audit list" );
    }
    if ( StringUtils.isEmpty( type ) ) {
      throw new HopException( "To retrieve an audit list you need to specify the type" );
    }

    String filename = calculateGroupPath( group ) + File.separator + type + ".list";
    File file = new File(filename);
    if (!file.exists()) {
      return new AuditList( group, type, new ArrayList<>(  ) );
    }
    try {
      return new ObjectMapper().readValue( new File( filename ), AuditList.class );
    } catch(IOException e) {
      throw new HopException( "It was not possible to read audit list file '"+filename+"'", e );
    }
  }



  private void validateEvent( AuditEvent event ) throws HopException {
    if ( StringUtils.isEmpty( event.getGroup() ) ) {
      throw new HopException( "Audit events need to belong to a group" );
    }
    if ( StringUtils.isEmpty( event.getType() ) ) {
      throw new HopException( "Audit events need to have a type" );
    }
    if ( StringUtils.isEmpty( event.getName() ) ) {
      throw new HopException( "Audit events need to have a name" );
    }
    if ( StringUtils.isEmpty( event.getOperation() ) ) {
      throw new HopException( "Audit events need to have an operation" );
    }
    if ( event.getDate() == null ) {
      throw new HopException( "Audit events need to have a date" );
    }
  }

  private void validateList( AuditList auditList ) throws HopException {
    if (StringUtils.isEmpty(auditList.getGroup())) {
      throw new HopException( "An audit list needs to belong to a group" );
    }
    if (StringUtils.isEmpty(auditList.getType())) {
      throw new HopException( "An audit list needs to have a type" );
    }
    if (auditList.getNames()==null) {
      throw new HopException( "The audit list of names can't be null" );
    }
  }
}
