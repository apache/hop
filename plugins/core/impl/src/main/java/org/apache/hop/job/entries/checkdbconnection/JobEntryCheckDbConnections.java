/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.checkdbconnection;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This check db connections
 *
 * @author Samatar
 * @since 10-12-2007
 */
@JobEntry( id = "CHECK_DB_CONNECTIONS",
  i18nPackageName = "org.apache.hop.job.entries.checkdbconnection",
  name = "JobEntryCheckDbConnections.Name",
  description = "JobEntryCheckDbConnections.TypeDesc",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Conditions" )
public class JobEntryCheckDbConnections extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryCheckDbConnections.class; // for i18n purposes, needed by Translator2!!

  private DatabaseMeta[] connections;

  public static final String[] unitTimeDesc = new String[] {
    BaseMessages.getString( PKG, "JobEntryCheckDbConnections.UnitTimeMilliSecond.Label" ),
    BaseMessages.getString( PKG, "JobEntryCheckDbConnections.UnitTimeSecond.Label" ),
    BaseMessages.getString( PKG, "JobEntryCheckDbConnections.UnitTimeMinute.Label" ),
    BaseMessages.getString( PKG, "JobEntryCheckDbConnections.UnitTimeHour.Label" ), };
  public static final String[] unitTimeCode = new String[] { "millisecond", "second", "minute", "hour" };

  public static final int UNIT_TIME_MILLI_SECOND = 0;
  public static final int UNIT_TIME_SECOND = 1;
  public static final int UNIT_TIME_MINUTE = 2;
  public static final int UNIT_TIME_HOUR = 3;

  private String[] waitfors;
  private int[] waittimes;

  private long timeStart;
  private long now;

  public JobEntryCheckDbConnections( String n ) {
    super( n, "" );
    connections = null;
    waitfors = null;
    waittimes = null;
  }

  public JobEntryCheckDbConnections() {
    this( "" );
  }

  public Object clone() {
    JobEntryCheckDbConnections je = (JobEntryCheckDbConnections) super.clone();
    return je;
  }

  public DatabaseMeta[] getConnections() {
    return connections;
  }

  public void setConnections( DatabaseMeta[] connections ) {
    this.connections = connections;
  }

  public String[] getWaitfors() {
    return waitfors;
  }

  public void setWaitfors( String[] waitfors ) {
    this.waitfors = waitfors;
  }

  public int[] getWaittimes() {
    return waittimes;
  }

  public void setWaittimes( int[] waittimes ) {
    this.waittimes = waittimes;
  }

  public long getTimeStart() {
    return timeStart;
  }

  public long getNow() {
    return now;
  }

  private static String getWaitTimeCode( int i ) {
    if ( i < 0 || i >= unitTimeCode.length ) {
      return unitTimeCode[ 0 ];
    }
    return unitTimeCode[ i ];
  }

  public static String getWaitTimeDesc( int i ) {
    if ( i < 0 || i >= unitTimeDesc.length ) {
      return unitTimeDesc[ 0 ];
    }
    return unitTimeDesc[ i ];
  }

  public static int getWaitTimeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < unitTimeDesc.length; i++ ) {
      if ( unitTimeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getWaitTimeByCode( tt );
  }

  private static int getWaitTimeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < unitTimeCode.length; i++ ) {
      if ( unitTimeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 120 );
    retval.append( super.getXML() );
    retval.append( "      <connections>" ).append( Const.CR );
    if ( connections != null ) {
      for ( int i = 0; i < connections.length; i++ ) {
        retval.append( "        <connection>" ).append( Const.CR );
        retval.append( "          " ).append(
          XMLHandler.addTagValue( "name", connections[ i ] == null ? null : connections[ i ].getName() ) );
        retval.append( "          " ).append( XMLHandler.addTagValue( "waitfor", waitfors[ i ] ) );
        retval
          .append( "          " ).append( XMLHandler.addTagValue( "waittime", getWaitTimeCode( waittimes[ i ] ) ) );
        retval.append( "        </connection>" ).append( Const.CR );
      }
    }
    retval.append( "      </connections>" ).append( Const.CR );

    return retval.toString();
  }

  private static int getWaitByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < unitTimeCode.length; i++ ) {
      if ( unitTimeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public void loadXML( Node entrynode, IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      Node fields = XMLHandler.getSubNode( entrynode, "connections" );

      // How many hosts?
      int nrFields = XMLHandler.countNodes( fields, "connection" );
      connections = new DatabaseMeta[ nrFields ];
      waitfors = new String[ nrFields ];
      waittimes = new int[ nrFields ];
      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "connection", i );
        String dbname = XMLHandler.getTagValue( fnode, "name" );
        connections[ i ] = DatabaseMeta.loadDatabase( metaStore, dbname );
        waitfors[ i ] = XMLHandler.getTagValue( fnode, "waitfor" );
        waittimes[ i ] = getWaitByCode( Const.NVL( XMLHandler.getTagValue( fnode, "waittime" ), "" ) );
      }
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryCheckDbConnections.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node", xe.getMessage() ) );
    }
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( true );
    int nrerrors = 0;
    int nrsuccess = 0;

    if ( connections != null ) {
      for ( int i = 0; i < connections.length && !parentJob.isStopped(); i++ ) {
        Database db = new Database( this, connections[ i ] );
        db.shareVariablesWith( this );
        try {
          db.connect( parentJob.getTransactionId(), null );

          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobEntryCheckDbConnections.Connected", connections[ i ]
              .getDatabaseName(), connections[ i ].getName() ) );
          }

          int iMaximumTimeout = Const.toInt( environmentSubstitute( waitfors[ i ] ), 0 );
          if ( iMaximumTimeout > 0 ) {

            int Multiple = 1;
            String waitTimeMessage = unitTimeDesc[ 0 ];
            switch ( waittimes[ i ] ) {
              case JobEntryCheckDbConnections.UNIT_TIME_MILLI_SECOND:
                Multiple = 1;
                waitTimeMessage = unitTimeDesc[ 0 ];
                break;
              case JobEntryCheckDbConnections.UNIT_TIME_SECOND:
                Multiple = 1000; // Second
                waitTimeMessage = unitTimeDesc[ 1 ];
                break;
              case JobEntryCheckDbConnections.UNIT_TIME_MINUTE:
                Multiple = 60000; // Minute
                waitTimeMessage = unitTimeDesc[ 2 ];
                break;
              case JobEntryCheckDbConnections.UNIT_TIME_HOUR:
                Multiple = 3600000; // Hour
                waitTimeMessage = unitTimeDesc[ 3 ];
                break;
              default:
                Multiple = 1000; // Second
                waitTimeMessage = unitTimeDesc[ 1 ];
                break;
            }
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEntryCheckDbConnections.Wait", "" + iMaximumTimeout, waitTimeMessage ) );
            }

            // starttime (in seconds ,Minutes or Hours)
            timeStart = System.currentTimeMillis();

            boolean continueLoop = true;
            while ( continueLoop && !parentJob.isStopped() ) {
              // Update Time value
              now = System.currentTimeMillis();
              // Let's check the limit time
              if ( ( now >= ( timeStart + iMaximumTimeout * Multiple ) ) ) {
                // We have reached the time limit
                if ( isDetailed() ) {
                  logDetailed( BaseMessages.getString(
                    PKG, "JobEntryCheckDbConnections.WaitTimeIsElapsed.Label", connections[ i ].getDatabaseName(),
                    connections[ i ].getName() ) );
                }

                continueLoop = false;
              } else {
                try {
                  Thread.sleep( 100 );
                } catch ( Exception e ) {
                  // Ignore sleep errors
                }
              }
            }
          }

          nrsuccess++;
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobEntryCheckDbConnections.ConnectionOK", connections[ i ]
              .getDatabaseName(), connections[ i ].getName() ) );
          }
        } catch ( HopDatabaseException e ) {
          nrerrors++;
          logError( BaseMessages.getString( PKG, "JobEntryCheckDbConnections.Exception", connections[ i ]
            .getDatabaseName(), connections[ i ].getName(), e.toString() ) );
        } finally {
          if ( db != null ) {
            try {
              db.disconnect();
              db = null;
            } catch ( Exception e ) { /* Ignore */
            }
          }
        }
      }
    }

    if ( nrerrors > 0 ) {
      result.setNrErrors( nrerrors );
      result.setResult( false );
    }

    if ( isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "JobEntryCheckDbConnections.Log.Info.ConnectionsInError", ""
        + nrerrors ) );
      logDetailed( BaseMessages.getString( PKG, "JobEntryCheckDbConnections.Log.Info.ConnectionsInSuccess", ""
        + nrsuccess ) );
      logDetailed( "=======================================" );
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return connections;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( connections != null ) {
      for ( int i = 0; i < connections.length; i++ ) {
        DatabaseMeta connection = connections[ i ];
        ResourceReference reference = new ResourceReference( this );
        reference.getEntries().add( new ResourceEntry( connection.getHostname(), ResourceType.SERVER ) );
        reference.getEntries().add( new ResourceEntry( connection.getDatabaseName(), ResourceType.DATABASENAME ) );
        references.add( reference );
      }
    }
    return references;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "tablename", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    JobEntryValidatorUtils.andValidator().validate( this, "columnname", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }

}
