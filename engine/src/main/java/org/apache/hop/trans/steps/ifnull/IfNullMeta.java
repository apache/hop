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

package org.apache.hop.trans.steps.ifnull;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.shared.SharedObjectInterface;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@InjectionSupported( localizationPrefix = "IfNull.Injection.", groups = { "FIELDS", "VALUE_TYPES" } )
public class IfNullMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = IfNullMeta.class; // for i18n purposes, needed by Translator2!!

  public static class Fields implements Cloneable {

    /** which fields to display? */
    @Injection( name = "FIELD_NAME", group = "FIELDS" )
    private String fieldName;

    /** by which value we replace */
    @Injection( name = "REPLACE_VALUE", group = "FIELDS" )
    private String replaceValue;

    @Injection( name = "REPLACE_MASK", group = "FIELDS" )
    private String replaceMask;

    /** Flag : set empty string **/
    @Injection( name = "SET_EMPTY_STRING", group = "FIELDS" )
    private boolean setEmptyString;

    public String getFieldName() {
      return fieldName;
    }

    public void setFieldName( String fieldName ) {
      this.fieldName = fieldName;
    }

    public String getReplaceValue() {
      return replaceValue;
    }

    public void setReplaceValue( String replaceValue ) {
      this.replaceValue = replaceValue;
    }

    public String getReplaceMask() {
      return replaceMask;
    }

    public void setReplaceMask( String replaceMask ) {
      this.replaceMask = replaceMask;
    }

    public boolean isSetEmptyString() {
      return setEmptyString;
    }

    public void setEmptyString( boolean setEmptyString ) {
      this.setEmptyString = setEmptyString;
    }

    public Fields clone() {
      try {
        return (Fields) super.clone();
      } catch ( CloneNotSupportedException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  public static class ValueTypes implements Cloneable {

    /** which types to display? */
    @Injection( name = "TYPE_NAME", group = "VALUE_TYPES" )
    private String typeName;

    /** by which value we replace */
    @Injection( name = "TYPE_REPLACE_VALUE", group = "VALUE_TYPES" )
    private String typereplaceValue;

    @Injection( name = "TYPE_REPLACE_MASK", group = "VALUE_TYPES" )
    private String typereplaceMask;

    /** Flag : set empty string for type **/
    @Injection( name = "SET_TYPE_EMPTY_STRING", group = "VALUE_TYPES" )
    private boolean setTypeEmptyString;

    public String getTypeName() {
      return typeName;
    }

    public void setTypeName( String typeName ) {
      this.typeName = typeName;
    }

    public String getTypereplaceValue() {
      return typereplaceValue;
    }

    public void setTypereplaceValue( String typereplaceValue ) {
      this.typereplaceValue = typereplaceValue;
    }

    public String getTypereplaceMask() {
      return typereplaceMask;
    }

    public void setTypereplaceMask( String typereplaceMask ) {
      this.typereplaceMask = typereplaceMask;
    }

    public boolean isSetTypeEmptyString() {
      return setTypeEmptyString;
    }

    public void setTypeEmptyString( boolean setTypeEmptyString ) {
      this.setTypeEmptyString = setTypeEmptyString;
    }

    public ValueTypes clone() {
      try {
        return (ValueTypes) super.clone();
      } catch ( CloneNotSupportedException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  @InjectionDeep
  private Fields[] fields;

  @InjectionDeep
  private ValueTypes[] valueTypes;

  @Injection( name = "SELECT_FIELDS" )
  private boolean selectFields;

  @Injection( name = "SELECT_VALUES_TYPE" )
  private boolean selectValuesType;

  @Injection( name = "REPLACE_ALL_BY_VALUE" )
  private String replaceAllByValue;

  @Injection( name = "REPLACE_ALL_MASK" )
  private String replaceAllMask;

  /** The flag to set auto commit on or off on the connection */
  @Injection( name = "SET_EMPTY_STRING_ALL" )
  private boolean setEmptyStringAll;

  public IfNullMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the setEmptyStringAll.
   */
  public boolean isSetEmptyStringAll() {
    return setEmptyStringAll;
  }

  /**
   * @param setEmptyStringAll
   *          The setEmptyStringAll to set.
   */
  public void setEmptyStringAll( boolean setEmptyStringAll ) {
    this.setEmptyStringAll = setEmptyStringAll;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public Object clone() {
    IfNullMeta retval = (IfNullMeta) super.clone();

    int nrTypes = valueTypes.length;
    int nrfields = fields.length;
    retval.allocate( nrTypes, nrfields );

    for ( int i = 0; i < nrTypes; i++ ) {
      retval.getValueTypes()[i] = valueTypes[i].clone();
    }

    for ( int i = 0; i < nrfields; i++ ) {
      retval.getFields()[i] = fields[i].clone();
    }

    return retval;
  }

  public void allocate( int nrtypes, int nrfields ) {
    valueTypes = new ValueTypes[nrtypes];
    for ( int i = 0; i < nrtypes; i++ ) {
      valueTypes[i] = new ValueTypes();
    }
    fields = new Fields[nrfields];
    for ( int i = 0; i < nrfields; i++ ) {
      fields[i] = new Fields();
    }
  }

  public boolean isSelectFields() {
    return selectFields;
  }

  public void setSelectFields( boolean selectFields ) {
    this.selectFields = selectFields;
  }

  public void setSelectValuesType( boolean selectValuesType ) {
    this.selectValuesType = selectValuesType;
  }

  public boolean isSelectValuesType() {
    return selectValuesType;
  }

  public void setReplaceAllByValue( String replaceValue ) {
    this.replaceAllByValue = replaceValue;
  }

  public String getReplaceAllByValue() {
    return replaceAllByValue;
  }

  public void setReplaceAllMask( String replaceAllMask ) {
    this.replaceAllMask = replaceAllMask;
  }

  public String getReplaceAllMask() {
    return replaceAllMask;
  }

  public Fields[] getFields() {
    return fields;
  }

  public void setFields( Fields[] fields ) {
    this.fields = fields;
  }

  public ValueTypes[] getValueTypes() {
    return valueTypes;
  }

  public void setValueTypes( ValueTypes[] valueTypes ) {
    this.valueTypes = valueTypes;
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      selectFields = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "selectFields" ) );
      selectValuesType = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "selectValuesType" ) );
      replaceAllByValue = XMLHandler.getTagValue( stepnode, "replaceAllByValue" );
      replaceAllMask = XMLHandler.getTagValue( stepnode, "replaceAllMask" );
      String setEmptyStringAllString = XMLHandler.getTagValue( stepnode, "setEmptyStringAll" );
      setEmptyStringAll = !Utils.isEmpty( setEmptyStringAllString ) && "Y".equalsIgnoreCase( setEmptyStringAllString );

      Node types = XMLHandler.getSubNode( stepnode, "valuetypes" );
      int nrtypes = XMLHandler.countNodes( types, "valuetype" );
      Node fieldNodes = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fieldNodes, "field" );

      allocate( nrtypes, nrfields );

      for ( int i = 0; i < nrtypes; i++ ) {
        Node tnode = XMLHandler.getSubNodeByNr( types, "valuetype", i );
        valueTypes[i].setTypeName( XMLHandler.getTagValue( tnode, "name" ) );
        valueTypes[i].setTypereplaceValue( XMLHandler.getTagValue( tnode, "value" ) );
        valueTypes[i].setTypereplaceMask( XMLHandler.getTagValue( tnode, "mask" ) );
        String typeemptyString = XMLHandler.getTagValue( tnode, "set_type_empty_string" );
        valueTypes[i].setTypeEmptyString( !Utils.isEmpty( typeemptyString ) && "Y".equalsIgnoreCase(
            typeemptyString ) );
      }
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fieldNodes, "field", i );
        fields[i].setFieldName( XMLHandler.getTagValue( fnode, "name" ) );
        fields[i].setReplaceValue( XMLHandler.getTagValue( fnode, "value" ) );
        fields[i].setReplaceMask( XMLHandler.getTagValue( fnode, "mask" ) );
        String emptyString = XMLHandler.getTagValue( fnode, "set_empty_string" );
        fields[i].setEmptyString( !Utils.isEmpty( emptyString ) && "Y".equalsIgnoreCase( emptyString ) );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "It was not possibke to load the IfNull metadata from XML", e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " + XMLHandler.addTagValue( "replaceAllByValue", replaceAllByValue ) );
    retval.append( "      " + XMLHandler.addTagValue( "replaceAllMask", replaceAllMask ) );
    retval.append( "      " + XMLHandler.addTagValue( "selectFields", selectFields ) );
    retval.append( "      " + XMLHandler.addTagValue( "selectValuesType", selectValuesType ) );
    retval.append( "      " + XMLHandler.addTagValue( "setEmptyStringAll", setEmptyStringAll ) );

    retval.append( "    <valuetypes>" + Const.CR );
    for ( int i = 0; i < valueTypes.length; i++ ) {
      retval.append( "      <valuetype>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", valueTypes[i].getTypeName() ) );
      retval.append( "        " + XMLHandler.addTagValue( "value", valueTypes[i].getTypereplaceValue() ) );
      retval.append( "        " + XMLHandler.addTagValue( "mask", valueTypes[i].getTypereplaceMask() ) );
      retval.append( "        " + XMLHandler.addTagValue( "set_type_empty_string", valueTypes[i]
          .isSetTypeEmptyString() ) );
      retval.append( "        </valuetype>" + Const.CR );
    }
    retval.append( "      </valuetypes>" + Const.CR );

    retval.append( "    <fields>" + Const.CR );
    for ( int i = 0; i < fields.length; i++ ) {
      retval.append( "      <field>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", fields[i].getFieldName() ) );
      retval.append( "        " + XMLHandler.addTagValue( "value", fields[i].getReplaceValue() ) );
      retval.append( "        " + XMLHandler.addTagValue( "mask", fields[i].getReplaceMask() ) );
      retval.append( "        " + XMLHandler.addTagValue( "set_empty_string", fields[i].isSetEmptyString() ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  public void setDefault() {
    replaceAllByValue = null;
    replaceAllMask = null;
    selectFields = false;
    selectValuesType = false;
    setEmptyStringAll = false;

    int nrfields = 0;
    int nrtypes = 0;
    allocate( nrtypes, nrfields );
    /*
     * Code will never execute. nrfields and nrtypes
     * are both zero above. so for-next is skipped on both.
     * 
     * MB - 5/2016
     * 
    for ( int i = 0; i < nrtypes; i++ ) {
      typeName[i] = "typename" + i;
      typereplaceValue[i] = "typevalue" + i;
      typereplaceMask[i] = "typemask" + i;
      setTypeEmptyString[i] = false;
    }
    for ( int i = 0; i < nrfields; i++ ) {
      fieldName[i] = "field" + i;
      replaceValue[i] = "value" + i;
      replaceMask[i] = "mask" + i;
      setEmptyString[i] = false;
    }
    */
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "IfNullMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "IfNullMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < fields.length; i++ ) {
        int idx = prev.indexOfValue( fields[i].getFieldName() );
        if ( idx < 0 ) {
          error_message += "\t\t" + fields[i].getFieldName() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message = BaseMessages.getString( PKG, "IfNullMeta.CheckResult.FieldsFound", error_message );

        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        if ( fields.length > 0 ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "IfNullMeta.CheckResult.AllFieldsFound" ), stepMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
              PKG, "IfNullMeta.CheckResult.NoFieldsEntered" ), stepMeta );
          remarks.add( cr );
        }
      }

    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "IfNullMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "IfNullMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new IfNull( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new IfNullData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
