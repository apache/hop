/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.repository.kdr;

import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.repository.BaseRepositoryMeta;
import org.apache.hop.repository.RepositoriesMeta;
import org.apache.hop.repository.RepositoryCapabilities;
import org.apache.hop.repository.RepositoryMeta;
import org.w3c.dom.Node;

/*
 * Created on 31-mar-2004
 */
public class HopDatabaseRepositoryMeta extends BaseRepositoryMeta implements RepositoryMeta {

  public static final String ID = "id";
  public static final String DATABASE_CONNECTION = "databaseConnection";

  /** The id as specified in the repository plugin meta, used for backward compatibility only */
  public static String REPOSITORY_TYPE_ID = "HopDatabaseRepository";

  private DatabaseMeta databaseMeta;

  public HopDatabaseRepositoryMeta() {
    super( REPOSITORY_TYPE_ID );
  }

  public HopDatabaseRepositoryMeta( String id, String name, String description, DatabaseMeta connection ) {
    super( id, name, description );
    this.databaseMeta = connection;
  }

  public HopDatabaseRepositoryMeta( String id ) {
    super( id, "", "" );
    this.databaseMeta = null;
  }

  public RepositoryCapabilities getRepositoryCapabilities() {
    return new RepositoryCapabilities() {
      public boolean supportsUsers() {
        return true;
      }

      public boolean managesUsers() {
        return true;
      }

      public boolean isReadOnly() {
        return false;
      }

      public boolean supportsRevisions() {
        return false;
      }

      public boolean supportsMetadata() {
        return true;
      }

      public boolean supportsLocking() {
        return true;
      }

      public boolean hasVersionRegistry() {
        return false;
      }

      public boolean supportsAcls() {
        return false;
      }

      public boolean supportsReferences() {
        return true;
      }
    };
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public Boolean isDefault() {
    return isDefault;
  }

  public void setDefault( Boolean isDefault ) {
    this.isDefault = isDefault;
  }

  public void setConnection( DatabaseMeta connection ) {
    this.databaseMeta = connection;
  }

  public DatabaseMeta getConnection() {
    return databaseMeta;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( "  " ).append( XMLHandler.openTag( XML_TAG ) );
    retval.append( super.getXML() );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "connection", databaseMeta != null ? databaseMeta.getName() : null ) );
    retval.append( "  " ).append( XMLHandler.closeTag( XML_TAG ) );

    return retval.toString();
  }

  public void loadXML( Node repnode, List<DatabaseMeta> databases ) throws HopException {
    super.loadXML( repnode, databases );
    try {
      String conn = XMLHandler.getTagValue( repnode, "connection" );
      databaseMeta = DatabaseMeta.findDatabase( databases, conn );
    } catch ( Exception e ) {
      throw new HopException( "Unable to load Hop database repository meta object", e );
    }
  }

  public RepositoryMeta clone() {
    return new HopDatabaseRepositoryMeta( REPOSITORY_TYPE_ID, getName(), getDescription(), getConnection() );
  }

  @Override public void populate( Map<String, Object> properties, RepositoriesMeta repositoriesMeta ) {
    super.populate( properties, repositoriesMeta );
    String databaseConnection = (String) properties.get( DATABASE_CONNECTION );

    DatabaseMeta databaseMeta = repositoriesMeta.searchDatabase( databaseConnection );
    if ( databaseMeta != null ) {
      setConnection( databaseMeta );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override public JSONObject toJSONObject() {
    JSONObject object = super.toJSONObject();
    object.put( DATABASE_CONNECTION, databaseMeta != null ? databaseMeta.getName() : "" );
    return object;
  }

}
