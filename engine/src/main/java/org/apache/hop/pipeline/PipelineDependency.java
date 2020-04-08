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

package org.apache.hop.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 5-apr-2004
 *
 */

public class PipelineDependency implements IXml, Cloneable {
  private static Class<?> PKG = Pipeline.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG = "dependency";

  private DatabaseMeta db;
  private String tablename;
  private String fieldname;

  public PipelineDependency( DatabaseMeta db, String tablename, String fieldname ) {
    this.db = db;
    this.tablename = tablename;
    this.fieldname = fieldname;
  }

  public PipelineDependency() {
    this( null, null, null );
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "      " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
    xml.append( "        " ).append( XmlHandler.addTagValue( "connection", db == null ? "" : db.getName() ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "table", tablename ) );
    xml.append( "        " ).append( XmlHandler.addTagValue( "field", fieldname ) );
    xml.append( "      " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return xml.toString();
  }

  public PipelineDependency( Node depnode, List<DatabaseMeta> databases ) throws HopXmlException {
    try {
      String depcon = XmlHandler.getTagValue( depnode, "connection" );
      db = DatabaseMeta.findDatabase( databases, depcon );
      tablename = XmlHandler.getTagValue( depnode, "table" );
      fieldname = XmlHandler.getTagValue( depnode, "field" );
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "PipelineDependency.Exception.UnableToLoadPipeline" ), e );
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public void setDatabase( DatabaseMeta db ) {
    this.db = db;
  }

  public DatabaseMeta getDatabase() {
    return db;
  }

  public void setTablename( String tablename ) {
    this.tablename = tablename;
  }

  public String getTablename() {
    return tablename;
  }

  public void setFieldname( String fieldname ) {
    this.fieldname = fieldname;
  }

  public String getFieldname() {
    return fieldname;
  }
}
