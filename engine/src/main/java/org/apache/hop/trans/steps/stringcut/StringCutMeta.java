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

package org.apache.hop.trans.steps.stringcut;

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

/**
 * @author Samatar Hassan
 * @since 30 September 2008
 */
public class StringCutMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = StringCutMeta.class; // for i18n purposes, needed by Translator2!!

  private String[] fieldInStream;

  private String[] fieldOutStream;

  private String[] cutFrom;

  private String[] cutTo;

  public StringCutMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the fieldInStream.
   */
  public String[] getFieldInStream() {
    return fieldInStream;
  }

  /**
   * @param keyStream
   *          The fieldInStream to set.
   */
  public void setFieldInStream( String[] keyStream ) {
    this.fieldInStream = keyStream;
  }

  /**
   * @return Returns the fieldOutStream.
   */
  public String[] getFieldOutStream() {
    return fieldOutStream;
  }

  /**
   * @param keyStream
   *          The fieldOutStream to set.
   */
  public void setFieldOutStream( String[] keyStream ) {
    this.fieldOutStream = keyStream;
  }

  public String[] getCutFrom() {
    return cutFrom;
  }

  public void setCutFrom( String[] cutFrom ) {
    this.cutFrom = cutFrom;
  }

  public String[] getCutTo() {
    return cutTo;
  }

  public void setCutTo( String[] cutTo ) {
    this.cutTo = cutTo;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int nrkeys ) {
    fieldInStream = new String[nrkeys];
    fieldOutStream = new String[nrkeys];
    cutTo = new String[nrkeys];
    cutFrom = new String[nrkeys];
  }

  public Object clone() {
    StringCutMeta retval = (StringCutMeta) super.clone();
    int nrkeys = fieldInStream.length;

    retval.allocate( nrkeys );
    System.arraycopy( fieldInStream, 0, retval.fieldInStream, 0, nrkeys );
    System.arraycopy( fieldOutStream, 0, retval.fieldOutStream, 0, nrkeys );
    System.arraycopy( cutTo, 0, retval.cutTo, 0, nrkeys );
    System.arraycopy( cutFrom, 0, retval.cutFrom, 0, nrkeys );

    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      int nrkeys;

      Node lookup = XMLHandler.getSubNode( stepnode, "fields" );
      nrkeys = XMLHandler.countNodes( lookup, "field" );

      allocate( nrkeys );

      for ( int i = 0; i < nrkeys; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( lookup, "field", i );
        fieldInStream[i] = Const.NVL( XMLHandler.getTagValue( fnode, "in_stream_name" ), "" );
        fieldOutStream[i] = Const.NVL( XMLHandler.getTagValue( fnode, "out_stream_name" ), "" );
        cutFrom[i] = Const.NVL( XMLHandler.getTagValue( fnode, "cut_from" ), "" );
        cutTo[i] = Const.NVL( XMLHandler.getTagValue( fnode, "cut_to" ), "" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "StringCutMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  public void setDefault() {
    fieldInStream = null;
    fieldOutStream = null;

    int nrkeys = 0;

    allocate( nrkeys );
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < fieldInStream.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "in_stream_name", fieldInStream[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "out_stream_name", fieldOutStream[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "cut_from", cutFrom[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "cut_to", cutTo[i] ) );
      retval.append( "      </field>" ).append( Const.CR );
    }

    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    for ( int i = 0; i < fieldOutStream.length; i++ ) {
      ValueMetaInterface v;
      if ( !Utils.isEmpty( fieldOutStream[i] ) ) {
        v = new ValueMetaString( space.environmentSubstitute( fieldOutStream[i] ) );
        v.setLength( 100, -1 );
        v.setOrigin( name );
        inputRowMeta.addValueMeta( v );
      } else {
        v = inputRowMeta.searchValueMeta( fieldInStream[i] );
        if ( v == null ) {
          continue;
        }
        v.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepinfo,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {

    CheckResult cr;
    String error_message = "";
    boolean first = true;
    boolean error_found = false;

    if ( prev == null ) {
      error_message += BaseMessages.getString( PKG, "StringCutMeta.CheckResult.NoInputReceived" ) + Const.CR;
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
      remarks.add( cr );
    } else {

      for ( int i = 0; i < fieldInStream.length; i++ ) {
        String field = fieldInStream[i];

        ValueMetaInterface v = prev.searchValueMeta( field );
        if ( v == null ) {
          if ( first ) {
            first = false;
            error_message +=
              BaseMessages.getString( PKG, "StringCutMeta.CheckResult.MissingInStreamFields" ) + Const.CR;
          }
          error_found = true;
          error_message += "\t\t" + field + Const.CR;
        }
      }
      if ( error_found ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "StringCutMeta.CheckResult.FoundInStreamFields" ), stepinfo );
      }
      remarks.add( cr );

      // Check whether all are strings
      first = true;
      error_found = false;
      for ( int i = 0; i < fieldInStream.length; i++ ) {
        String field = fieldInStream[i];

        ValueMetaInterface v = prev.searchValueMeta( field );
        if ( v != null ) {
          if ( v.getType() != ValueMetaInterface.TYPE_STRING ) {
            if ( first ) {
              first = false;
              error_message +=
                BaseMessages.getString( PKG, "StringCutMeta.CheckResult.OperationOnNonStringFields" ) + Const.CR;
            }
            error_found = true;
            error_message += "\t\t" + field + Const.CR;
          }
        }
      }
      if ( error_found ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "StringCutMeta.CheckResult.AllOperationsOnStringFields" ), stepinfo );
      }
      remarks.add( cr );

      if ( fieldInStream.length > 0 ) {
        for ( int idx = 0; idx < fieldInStream.length; idx++ ) {
          if ( Utils.isEmpty( fieldInStream[idx] ) ) {
            cr =
              new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString( PKG, "StringCutMeta.CheckResult.InStreamFieldMissing", new Integer(
                  idx + 1 ).toString() ), stepinfo );
            remarks.add( cr );

          }
        }
      }

    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new StringCut( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new StringCutData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
