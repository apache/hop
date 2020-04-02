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

package org.apache.hop.core.database;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.i18n.BaseMessages;

/**
 * @author matt
 */
public class DatabaseFactory implements IDatabaseFactory {

  private static Class<?> PKG = Database.class; // for i18n purposes, needed by Translator!!
  private boolean success;

  public static final ILoggingObject loggingObject = new SimpleLoggingObject(
    "Database factory", LoggingObjectType.GENERAL, null );

  public DatabaseFactory() {
  }

  @Override
  public String getConnectionTestReport( DatabaseMeta databaseMeta ) throws HopDatabaseException {
    success = true; // default

    StringBuilder report = new StringBuilder();
    Database db = new Database( loggingObject, databaseMeta );
    try {
      db.connect();
      report.append( BaseMessages.getString( PKG, "DatabaseMeta.report.ConnectionOk", databaseMeta.getName() )
        + Const.CR );
    } catch ( HopException e ) {
      report.append( BaseMessages.getString( PKG, "DatabaseMeta.report.ConnectionError", databaseMeta
        .getName() )
        + e.toString() + Const.CR );
      report.append( Const.getStackTracker( e ) + Const.CR );
      success = false;
    } finally {
      db.disconnect();
    }

    appendConnectionInfo( report, db, databaseMeta );
    report.append( Const.CR );

    return report.toString();
  }

  public DatabaseTestResults getConnectionTestResults( DatabaseMeta databaseMeta ) throws HopDatabaseException {
    DatabaseTestResults databaseTestResults = new DatabaseTestResults();
    String message = getConnectionTestReport( databaseMeta );
    databaseTestResults.setMessage( message );
    databaseTestResults.setSuccess( success );
    return databaseTestResults;
  }

  private StringBuilder appendConnectionInfo( StringBuilder report, Database db, DatabaseMeta databaseMeta ) {

    // Check to see if the interface is of a type GenericDatabaseMeta, since it does not have hostname and port fields
    if ( databaseMeta.getIDatabase() instanceof GenericDatabaseMeta ) {
      String customUrl = databaseMeta.getManualUrl();
      String customDriverClass = databaseMeta.getAttributes().getProperty( GenericDatabaseMeta.ATRRIBUTE_CUSTOM_DRIVER_CLASS );

      return report.append( BaseMessages.getString( PKG, "GenericDatabaseMeta.report.customUrl" ) ).append(
        db.environmentSubstitute( customUrl ) ).append( Const.CR ).append(
        BaseMessages.getString( PKG, "GenericDatabaseMeta.report.customDriverClass" ) ).append(
        db.environmentSubstitute( customDriverClass ) ).append( Const.CR );
    }

    return appendConnectionInfo( report, db.environmentSubstitute( databaseMeta.getHostname() ), db
      .environmentSubstitute( databaseMeta.getPort() ), db
      .environmentSubstitute( databaseMeta.getDatabaseName() ) );
  }

  //CHECKSTYLE:LineLength:OFF
  private StringBuilder appendConnectionInfo( StringBuilder report, String hostName, String portNumber, String dbName ) {
    report.append( BaseMessages.getString( PKG, "DatabaseMeta.report.Hostname" ) ).append( hostName ).append( Const.CR );
    report.append( BaseMessages.getString( PKG, "DatabaseMeta.report.Port" ) ).append( portNumber ).append( Const.CR );
    report.append( BaseMessages.getString( PKG, "DatabaseMeta.report.DatabaseName" ) ).append( dbName ).append( Const.CR );
    return report;
  }
}
