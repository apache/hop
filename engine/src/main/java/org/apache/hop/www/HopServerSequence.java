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

package org.apache.hop.www;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.util.List;

public class HopServerSequence {
  public static final String XML_TAG = "sequence";

  /**
   * The name of the server sequence
   */
  private String name;

  /**
   * The start value
   */
  private long startValue;

  /**
   * The database to use
   */
  private DatabaseMeta databaseMeta;

  /**
   * The schema to use
   */
  private String schemaName;

  /**
   * The table to use
   */
  private String tableName;

  /**
   * The sequence name field in the table
   */
  private String sequenceNameField;

  /**
   * The current value of the sequence
   */
  private String valueField;

  public HopServerSequence() {
    startValue = 1;
  }

  /**
   * @param name
   * @param startValue
   * @param databaseMeta
   * @param schemaName
   * @param tableName
   * @param sequenceNameField
   * @param valueField
   */
  public HopServerSequence( String name, long startValue, DatabaseMeta databaseMeta, String schemaName,
                            String tableName, String sequenceNameField, String valueField ) {
    this.name = name;
    this.startValue = startValue;
    this.databaseMeta = databaseMeta;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.sequenceNameField = sequenceNameField;
    this.valueField = valueField;
  }

  public synchronized long getNextValue( IVariables variables, ILoggingObject log, long incrementValue ) throws HopException {

    Database db = null;
    try {
      db = new Database( log, variables, databaseMeta );
      db.connect();

      String schemaTable = databaseMeta.getQuotedSchemaTableCombination( variables, schemaName, tableName );
      String seqField = databaseMeta.quoteField( sequenceNameField );
      String valField = databaseMeta.quoteField( valueField );

      boolean update = false;

      String sql = "SELECT " + valField + " FROM " + schemaTable + " WHERE " + seqField + " = ?";
      RowMetaAndData param = new RowMetaAndData();
      param.addValue( seqField, IValueMeta.TYPE_STRING, name );
      RowMetaAndData row = db.getOneRow( sql, param.getRowMeta(), param.getData() );
      long value;
      if ( row != null && row.getData() != null ) {
        update = true;
        Long longValue = row.getInteger( 0 );
        if ( longValue == null ) {
          value = startValue;
        } else {
          value = longValue.longValue();
        }
      } else {
        value = startValue;
      }

      long maximum = value + incrementValue;

      // Update the value in the table...
      //
      if ( update ) {
        sql = "UPDATE " + schemaTable + " SET " + valField + "= ? WHERE " + seqField + "= ? ";
        param = new RowMetaAndData();
        param.addValue( valField, IValueMeta.TYPE_INTEGER, Long.valueOf( maximum ) );
        param.addValue( seqField, IValueMeta.TYPE_STRING, name );

      } else {
        sql = "INSERT INTO " + schemaTable + "(" + seqField + ", " + valField + ") VALUES( ? , ? )";
        param = new RowMetaAndData();
        param.addValue( seqField, IValueMeta.TYPE_STRING, name );
        param.addValue( valField, IValueMeta.TYPE_INTEGER, Long.valueOf( maximum ) );
      }
      db.execStatement( sql, param.getRowMeta(), param.getData() );

      return value;

    } catch ( Exception e ) {
      throw new HopException( "Unable to get next value for server sequence '"
        + name + "' on database '" + databaseMeta.getName() + "'", e );
    } finally {
      db.disconnect();
    }
  }

  public HopServerSequence( Node node, List<DatabaseMeta> databases ) throws HopXmlException {
    name = XmlHandler.getTagValue( node, "name" );
    startValue = Const.toInt( XmlHandler.getTagValue( node, "start" ), 0 );
    databaseMeta = DatabaseMeta.findDatabase( databases, XmlHandler.getTagValue( node, "connection" ) );
    schemaName = XmlHandler.getTagValue( node, "schema" );
    tableName = XmlHandler.getTagValue( node, "table" );
    sequenceNameField = XmlHandler.getTagValue( node, "sequence_field" );
    valueField = XmlHandler.getTagValue( node, "value_field" );
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder( 100 );

    xml.append( XmlHandler.addTagValue( "name", name ) );
    xml.append( XmlHandler.addTagValue( "start", startValue ) );
    xml.append( XmlHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    xml.append( XmlHandler.addTagValue( "schema", schemaName ) );
    xml.append( XmlHandler.addTagValue( "table", tableName ) );
    xml.append( XmlHandler.addTagValue( "sequence_field", sequenceNameField ) );
    xml.append( XmlHandler.addTagValue( "value_field", valueField ) );

    return xml.toString();
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the startValue
   */
  public long getStartValue() {
    return startValue;
  }

  /**
   * @param startValue the startValue to set
   */
  public void setStartValue( long startValue ) {
    this.startValue = startValue;
  }

  /**
   * @return the databaseMeta
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param databaseMeta the databaseMeta to set
   */
  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  /**
   * @return the sequenceNameField
   */
  public String getSequenceNameField() {
    return sequenceNameField;
  }

  /**
   * @param sequenceNameField the sequenceNameField to set
   */
  public void setSequenceNameField( String sequenceNameField ) {
    this.sequenceNameField = sequenceNameField;
  }

  /**
   * @return the valueField
   */
  public String getValueField() {
    return valueField;
  }

  /**
   * @param valueField the valueField to set
   */
  public void setValueField( String valueField ) {
    this.valueField = valueField;
  }

  /**
   * Find a server sequence with a certain name
   *
   * @param name the name to look for
   * @return the server sequence with the specified name or null of the sequence couldn't be found.
   */
  public static HopServerSequence findServerSequence( String name, List<HopServerSequence> hopServerSequences ) {
    for ( HopServerSequence hopServerSequence : hopServerSequences ) {
      if ( hopServerSequence.getName().equalsIgnoreCase( name ) ) {
        return hopServerSequence;
      }
    }
    return null;
  }
}
