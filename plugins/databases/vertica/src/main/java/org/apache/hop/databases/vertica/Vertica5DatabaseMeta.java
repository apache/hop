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

package org.apache.hop.databases.vertica;

import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Vertica Analytic Database version 5 and later (changed driver class name)
 *
 * @author DEinspanjer
 * @author Matt
 * @author Jens
 * @since 2009-03-16
 * @since May-2008
 * @since Aug-2012
 */
@DatabaseMetaPlugin(
  type = "VERTICA5",
  typeDescription = "Vertica 5"
)
@GuiPlugin( id = "GUI-Vertica5DatabaseMeta" )
public class Vertica5DatabaseMeta extends VerticaDatabaseMeta {
  @Override
  public String getDriverClass() {
    return "com.vertica.jdbc.Driver";
  }

  /**
   * @return false as the database does not support timestamp to date conversion.
   */
  @Override
  public boolean supportsTimeStampToDateConversion() {
    return false;
  }

  /**
   * This method allows a database dialect to convert database specific data types to Hop data types.
   *
   * @param rs    The result set to use
   * @param val   The description of the value to retrieve
   * @param index the index on which we need to retrieve the value, 0-based.
   * @return The correctly converted Hop data type corresponding to the valueMeta description.
   * @throws HopDatabaseException
   */
  @Override
  public Object getValueFromResultSet( ResultSet rs, IValueMeta val, int index ) throws HopDatabaseException {
    Object data;

    try {
      switch ( val.getType() ) {
        case IValueMeta.TYPE_TIMESTAMP:
        case IValueMeta.TYPE_DATE:
          if ( val.getOriginalColumnType() == java.sql.Types.TIMESTAMP ) {
            data = rs.getTimestamp( index + 1 );
            break; // Timestamp extends java.util.Date
          } else if ( val.getOriginalColumnType() == java.sql.Types.TIME ) {
            data = rs.getTime( index + 1 );
            break;
          } else {
            data = rs.getDate( index + 1 );
            break;
          }
        default:
          return super.getValueFromResultSet( rs, val, index );
      }
      if ( rs.wasNull() ) {
        data = null;
      }
    } catch ( SQLException e ) {
      throw new HopDatabaseException( "Unable to get value '"
        + val.toStringMeta() + "' from database resultset, index " + index, e );
    }

    return data;
  }
}
