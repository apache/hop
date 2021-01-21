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

package org.apache.hop.core.database;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

/**
 * @author matt
 */
public class DatabaseFactory implements IDatabaseFactory {

  private static final Class<?> PKG = Database.class; // For Translator
  private boolean success;

  public static final ILoggingObject loggingObject = new SimpleLoggingObject(
    "Database factory", LoggingObjectType.GENERAL, null );

  public DatabaseFactory() {
  }

  @Override
  public String getConnectionTestReport( IVariables variables, DatabaseMeta databaseMeta ) throws HopDatabaseException {
    success = true; // default

    StringBuilder report = new StringBuilder();
    Database db = new Database( loggingObject, variables, databaseMeta );
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

    appendConnectionInfo( variables, report, db, databaseMeta );
    report.append( Const.CR );

    return report.toString();
  }

  public DatabaseTestResults getConnectionTestResults( IVariables variables, DatabaseMeta databaseMeta ) throws HopDatabaseException {
    DatabaseTestResults databaseTestResults = new DatabaseTestResults();
    String message = getConnectionTestReport( variables, databaseMeta );
    databaseTestResults.setMessage( message );
    databaseTestResults.setSuccess( success );
    return databaseTestResults;
  }

  private StringBuilder appendConnectionInfo( IVariables variables, StringBuilder report, Database db, DatabaseMeta databaseMeta ) {

    // Check to see if the interface is of a type GenericDatabaseMeta, since it does not have hostname and port fields
    if ( databaseMeta.getIDatabase() instanceof GenericDatabaseMeta ) {
      
    	String customDriverClass = databaseMeta.getAttributes().get( GenericDatabaseMeta.ATRRIBUTE_CUSTOM_DRIVER_CLASS );
  		append( report, "GenericDatabaseMeta.report.customUrl",  db.resolve(  databaseMeta.getManualUrl() ) );
  		append( report, "GenericDatabaseMeta.report.customDriverClass",  db.resolve(  customDriverClass ) );
      
  		return report;
    }
        
	append( report, "DatabaseMeta.report.Hostname",  db.resolve(  databaseMeta.getHostname() ) );
	append( report, "DatabaseMeta.report.Port",  db.resolve( databaseMeta.getPort() ) );
	append( report, "DatabaseMeta.report.DatabaseName",  db.resolve( databaseMeta.getDatabaseName() ) );
    
	String url =  "";
	try {
		url =  databaseMeta.getURL(variables);
	} catch (HopDatabaseException e) {
		url = e.toString();
	}	
	append( report, "DatabaseMeta.report.Url",  url );
        
    return report;
  }
  
  private void append( StringBuilder report, String label, String text) {
	  report.append( BaseMessages.getString( PKG, label ) ).append('\t').append( text ).append( Const.CR );
  }
}
