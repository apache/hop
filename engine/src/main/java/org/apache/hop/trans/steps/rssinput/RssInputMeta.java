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

package org.apache.hop.trans.steps.rssinput;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public class RssInputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = RssInput.class; // for i18n purposes, needed by Translator2!!

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  /** Flag indicating that url field should be included in the output */
  private boolean includeUrl;

  /** The name of the field in the output containing the url */
  private String urlField;

  /** The maximum number or lines to read */
  private long rowLimit;

  /** The fields to import... */
  private RssInputField[] inputFields;

  /** The url **/
  private String[] url;

  /** read rss from */
  private String readfrom;

  /** if URL defined in a field? */
  private boolean urlInField;

  /** URL field name */
  private String urlFieldname;

  public RssInputMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the input fields.
   */
  public RssInputField[] getInputFields() {
    return inputFields;
  }

  /**
   * @param inputFields
   *          The input fields to set.
   */
  public void setInputFields( RssInputField[] inputFields ) {
    this.inputFields = inputFields;
  }

  /**
   * @return Returns the urlInField.
   */
  public boolean urlInField() {
    return urlInField;
  }

  /**
   * @param urlInField
   *          The urlInField to set.
   */
  public void seturlInField( boolean urlInField ) {
    this.urlInField = urlInField;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  public void setReadFrom( String readfrom ) {
    this.readfrom = readfrom;
  }

  public String getReadFrom() {
    return readfrom;
  }

  public String getRealReadFrom() {
    return getReadFrom();
  }

  /**
   * @return Returns the includeUrl.
   */
  public boolean includeUrl() {
    return includeUrl;
  }

  /**
   * @param includeRowNumber
   *          The includeRowNumber to set.
   */
  public void setIncludeRowNumber( boolean includeRowNumber ) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * @param includeUrl
   *          The includeUrl to set.
   */
  public void setIncludeUrl( boolean includeUrl ) {
    this.includeUrl = includeUrl;
  }

  /**
   * @return Returns the rowLimit.
   */
  public long getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit
   *          The rowLimit to set.
   */
  public void setRowLimit( long rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the rowNumberField.
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /**
   * @return Returns the urlField.
   */
  public String geturlField() {
    return urlField;
  }

  /**
   * @param urlField
   *          The urlField to set.
   */
  public void seturlField( String urlField ) {
    this.urlField = urlField;
  }

  /**
   * @param urlFieldname
   *          The urlFieldname to set.
   */
  public void setUrlFieldname( String urlFieldname ) {
    this.urlFieldname = urlFieldname;
  }

  /**
   * @return Returns the urlFieldname.
   */
  public String getUrlFieldname() {
    return urlFieldname;
  }

  /**
   * @param url
   *          The url to set.
   */
  public void setUrl( String[] url ) {
    this.url = url;
  }

  public String[] getUrl() {
    return url;
  }

  /**
   * @param rowNumberField
   *          The rowNumberField to set.
   */
  public void setRowNumberField( String rowNumberField ) {
    this.rowNumberField = rowNumberField;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    RssInputMeta retval = (RssInputMeta) super.clone();

    int nrFields = inputFields.length;
    int nrUrl = url.length;

    retval.allocate( nrUrl, nrFields );
    System.arraycopy( url, 0, retval.url, 0, nrUrl );
    for ( int i = 0; i < nrFields; i++ ) {
      if ( inputFields[i] != null ) {
        retval.inputFields[i] = (RssInputField) inputFields[i].clone();
      }
    }

    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "url_in_field", urlInField ) );
    retval.append( "    " + XMLHandler.addTagValue( "url_field_name", urlFieldname ) );
    retval.append( "    " + XMLHandler.addTagValue( "rownum", includeRowNumber ) );
    retval.append( "    " + XMLHandler.addTagValue( "rownum_field", rowNumberField ) );
    retval.append( "    " + XMLHandler.addTagValue( "include_url", includeUrl ) );
    retval.append( "    " + XMLHandler.addTagValue( "url_Field", urlField ) );
    retval.append( "    " + XMLHandler.addTagValue( "read_from", readfrom ) );
    retval.append( "    <urls>" + Const.CR );
    for ( int i = 0; i < url.length; i++ ) {
      retval.append( "      " + XMLHandler.addTagValue( "url", url[i] ) );
    }
    retval.append( "    </urls>" + Const.CR );
    retval.append( "    <fields>" + Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      RssInputField field = inputFields[i];
      retval.append( field.getXML() );
    }
    retval.append( "      </fields>" + Const.CR );
    retval.append( "    " + XMLHandler.addTagValue( "limit", rowLimit ) );
    return retval.toString();
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {

      urlInField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "url_in_field" ) );
      urlFieldname = XMLHandler.getTagValue( stepnode, "url_field_name" );
      includeRowNumber = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "rownum" ) );
      rowNumberField = XMLHandler.getTagValue( stepnode, "rownum_field" );
      includeUrl = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "include_url" ) );
      urlField = XMLHandler.getTagValue( stepnode, "url_Field" );
      readfrom = XMLHandler.getTagValue( stepnode, "read_from" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );
      Node urlnode = XMLHandler.getSubNode( stepnode, "urls" );
      int nrUrls = XMLHandler.countNodes( urlnode, "url" );
      allocate( nrUrls, nrFields );
      for ( int i = 0; i < nrUrls; i++ ) {
        Node urlnamenode = XMLHandler.getSubNodeByNr( urlnode, "url", i );
        url[i] = XMLHandler.getNodeValue( urlnamenode );
      }

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        RssInputField field = new RssInputField( fnode );
        inputFields[i] = field;
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong( XMLHandler.getTagValue( stepnode, "limit" ), 0L );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  public void allocate( int nrUrl, int nrfields ) {
    inputFields = new RssInputField[nrfields];
    url = new String[nrUrl];
  }

  public void setDefault() {
    urlInField = false;
    urlFieldname = "";
    includeRowNumber = false;
    rowNumberField = "";
    includeUrl = false;
    urlField = "";
    readfrom = "";

    int nrFields = 0;
    int nrUrl = 0;

    allocate( nrUrl, nrFields );

    for ( int i = 0; i < nrUrl; i++ ) {
      url[i] = "";

    }

    for ( int i = 0; i < nrFields; i++ ) {
      inputFields[i] = new RssInputField( "field" + ( i + 1 ) );
    }

    rowLimit = 0;
  }

  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    int i;
    for ( i = 0; i < inputFields.length; i++ ) {
      RssInputField field = inputFields[i];

      int type = field.getType();
      if ( type == ValueMetaInterface.TYPE_NONE ) {
        type = ValueMetaInterface.TYPE_STRING;
      }
      try {
        ValueMetaInterface v =
          ValueMetaFactory.createValueMeta( space.environmentSubstitute( field.getName() ), type );
        v.setLength( field.getLength(), field.getPrecision() );
        v.setOrigin( name );
        r.addValueMeta( v );
      } catch ( Exception e ) {
        throw new HopStepException( e );
      }

    }

    if ( includeUrl ) {
      ValueMetaInterface v = new ValueMetaString( space.environmentSubstitute( urlField ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

    if ( includeRowNumber ) {
      ValueMetaInterface v = new ValueMetaInteger( space.environmentSubstitute( rowNumberField ) );
      v.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
  }

  public StepDataInterface getStepData() {
    return new RssInputData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;

    if ( urlInField ) {
      if ( Utils.isEmpty( getUrlFieldname() ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "RssInputMeta.CheckResult.NoField" ), stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "RssInputMeta.CheckResult.FieldOk" ), stepMeta );
        remarks.add( cr );
      }
    } else {
      if ( getUrl() == null || getUrl().length == 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "RssInputMeta.CheckResult.NoUrl" ), stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "RssInputMeta.CheckResult.UrlOk", "" + getUrl().length ), stepMeta );
        remarks.add( cr );
      }
    }

  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new RssInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

}
