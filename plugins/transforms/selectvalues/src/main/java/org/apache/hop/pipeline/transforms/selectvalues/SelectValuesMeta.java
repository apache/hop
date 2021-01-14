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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.FieldnameLineage;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Transform(
        id = "SelectValues",
        image = "selectvalues.svg",
        name = "i18n:org.apache.hop.pipeline.transforms.selectvalues:BaseTransform.TypeLongDesc.SelectValues",
        description = "i18n:org.apache.hop.pipeline.transforms.selectvalues:BaseTransform.TypeTooltipDesc.SelectValues",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/selectvalues.html"
)
@InjectionSupported( localizationPrefix = "SelectValues.Injection.", groups = { "FIELDS", "REMOVES", "METAS" } )
public class SelectValuesMeta extends BaseTransformMeta implements ITransformMeta<SelectValues, SelectValuesData> {
  private static final Class<?> PKG = SelectValuesMeta.class; // For Translator

  public static final int UNDEFINED = -2;

  // SELECT mode
  @InjectionDeep
  private SelectField[] selectFields = {};

  /**
   * Select: flag to indicate that the non-selected fields should also be taken along, ordered by fieldname
   */
  @Injection( name = "SELECT_UNSPECIFIED" )
  private boolean selectingAndSortingUnspecifiedFields;

  // DE-SELECT mode
  /**
   * Names of the fields to be removed!
   */
  @Injection( name = "REMOVE_NAME", group = "REMOVES" )
  private String[] deleteName = {};

  // META-DATA mode
  @InjectionDeep
  private SelectMetadataChange[] meta = {};

  public SelectValuesMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the deleteName.
   */
  public String[] getDeleteName() {
    return deleteName;
  }

  /**
   * @param deleteName The deleteName to set.
   */
  public void setDeleteName( String[] deleteName ) {
    this.deleteName = deleteName == null ? new String[ 0 ] : deleteName;
  }

  /**
   * @param selectName The selectName to set.
   */
  public void setSelectName( String[] selectName ) {
    resizeSelectFields( selectName.length );
    for ( int i = 0; i < selectFields.length; i++ ) {
      selectFields[ i ].setName( selectName[ i ] );
    }
  }

  public String[] getSelectName() {
    String[] selectName = new String[ selectFields.length ];
    for ( int i = 0; i < selectName.length; i++ ) {
      selectName[ i ] = selectFields[ i ].getName();
    }
    return selectName;
  }

  /**
   * @param selectRename The selectRename to set.
   */
  public void setSelectRename( String[] selectRename ) {
    if ( selectRename.length > selectFields.length ) {
      resizeSelectFields( selectRename.length );
    }
    for ( int i = 0; i < selectFields.length; i++ ) {
      if ( i < selectRename.length ) {
        selectFields[ i ].setRename( selectRename[ i ] );
      } else {
        selectFields[ i ].setRename( null );
      }
    }
  }

  public String[] getSelectRename() {
    String[] selectRename = new String[ selectFields.length ];
    for ( int i = 0; i < selectRename.length; i++ ) {
      selectRename[ i ] = selectFields[ i ].getRename();
    }
    return selectRename;
  }

  /**
   * @param selectLength The selectLength to set.
   */
  public void setSelectLength( int[] selectLength ) {
    if ( selectLength.length > selectFields.length ) {
      resizeSelectFields( selectLength.length );
    }
    for ( int i = 0; i < selectFields.length; i++ ) {
      if ( i < selectLength.length ) {
        selectFields[ i ].setLength( selectLength[ i ] );
      } else {
        selectFields[ i ].setLength( UNDEFINED );
      }
    }
  }

  public int[] getSelectLength() {
    int[] selectLength = new int[ selectFields.length ];
    for ( int i = 0; i < selectLength.length; i++ ) {
      selectLength[ i ] = selectFields[ i ].getLength();
    }
    return selectLength;
  }

  /**
   * @param selectPrecision The selectPrecision to set.
   */
  public void setSelectPrecision( int[] selectPrecision ) {
    if ( selectPrecision.length > selectFields.length ) {
      resizeSelectFields( selectPrecision.length );
    }
    for ( int i = 0; i < selectFields.length; i++ ) {
      if ( i < selectPrecision.length ) {
        selectFields[ i ].setPrecision( selectPrecision[ i ] );
      } else {
        selectFields[ i ].setPrecision( UNDEFINED );
      }
    }
  }

  public int[] getSelectPrecision() {
    int[] selectPrecision = new int[ selectFields.length ];
    for ( int i = 0; i < selectPrecision.length; i++ ) {
      selectPrecision[ i ] = selectFields[ i ].getPrecision();
    }
    return selectPrecision;
  }

  private void resizeSelectFields( int length ) {
    int fillStartIndex = selectFields.length;
    selectFields = Arrays.copyOf( selectFields, length );
    for ( int i = fillStartIndex; i < selectFields.length; i++ ) {
      selectFields[ i ] = new SelectField();
      selectFields[ i ].setLength( UNDEFINED );
      selectFields[ i ].setPrecision( UNDEFINED );
    }
  }

  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  public void allocate( int nrFields, int nrRemove, int nrMeta ) {
    allocateSelect( nrFields );
    allocateRemove( nrRemove );
    allocateMeta( nrMeta );
  }

  private void allocateSelect( int nrFields ) {
    selectFields = new SelectField[ nrFields ];
    for ( int i = 0; i < nrFields; i++ ) {
      selectFields[ i ] = new SelectField();
    }
  }

  private void allocateRemove( int nrRemove ) {
    deleteName = new String[ nrRemove ];
  }

  private void allocateMeta( int nrMeta ) {
    meta = new SelectMetadataChange[ nrMeta ];
    for ( int i = 0; i < nrMeta; i++ ) {
      meta[ i ] = new SelectMetadataChange();
    }
  }

  @Override
  public Object clone() {
    SelectValuesMeta retval = (SelectValuesMeta) super.clone();

    int nrFields = selectFields == null ? 0 : selectFields.length;
    int nrremove = deleteName == null ? 0 : deleteName.length;
    int nrmeta = meta == null ? 0 : meta.length;

    retval.allocate( nrFields, nrremove, nrmeta );
    for ( int i = 0; i < nrFields; i++ ) {
      retval.getSelectFields()[ i ] = selectFields[ i ].clone();
    }

    System.arraycopy( deleteName, 0, retval.deleteName, 0, nrremove );

    for ( int i = 0; i < nrmeta; i++ ) {
      // CHECKSTYLE:Indentation:OFF
      retval.getMeta()[ i ] = meta[ i ].clone();
    }

    return retval;
  }

  private void readData( Node transform ) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode( transform, "fields" );

      int nrFields = XmlHandler.countNodes( fields, "field" );
      int nrremove = XmlHandler.countNodes( fields, "remove" );
      int nrmeta = XmlHandler.countNodes( fields, SelectMetadataChange.XML_TAG );
      allocate( nrFields, nrremove, nrmeta );

      for ( int i = 0; i < nrFields; i++ ) {
        Node line = XmlHandler.getSubNodeByNr( fields, "field", i );
        selectFields[ i ] = new SelectField();
        selectFields[ i ].setName( XmlHandler.getTagValue( line, "name" ) );
        selectFields[ i ].setRename( XmlHandler.getTagValue( line, "rename" ) );
        selectFields[ i ].setLength( Const.toInt( XmlHandler.getTagValue( line, "length" ), UNDEFINED ) );
        selectFields[ i ].setPrecision( Const.toInt( XmlHandler.getTagValue( line, "precision" ), UNDEFINED ) );
      }
      selectingAndSortingUnspecifiedFields =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( fields, "select_unspecified" ) );

      for ( int i = 0; i < nrremove; i++ ) {
        Node line = XmlHandler.getSubNodeByNr( fields, "remove", i );
        deleteName[ i ] = XmlHandler.getTagValue( line, "name" );
      }

      for ( int i = 0; i < nrmeta; i++ ) {
        Node metaNode = XmlHandler.getSubNodeByNr( fields, SelectMetadataChange.XML_TAG, i );
        meta[ i ] = new SelectMetadataChange();
        meta[ i ].loadXml( metaNode );
      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG,
        "SelectValuesMeta.Exception.UnableToReadTransformMetaFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    allocate( 0, 0, 0 );
  }

  public void getSelectFields( IRowMeta inputRowMeta, String name ) throws HopTransformException {
    IRowMeta row;

    if ( selectFields != null && selectFields.length > 0 ) { // SELECT values

      // 0. Start with an empty row
      // 1. Keep only the selected values
      // 2. Rename the selected values
      // 3. Keep the order in which they are specified... (not the input order!)
      //

      row = new RowMeta();
      for ( int i = 0; i < selectFields.length; i++ ) {
        IValueMeta v = inputRowMeta.searchValueMeta( selectFields[ i ].getName() );

        if ( v != null ) { // We found the value

          v = v.clone();
          // Do we need to rename ?
          if ( !v.getName().equals( selectFields[ i ].getRename() ) && selectFields[ i ].getRename() != null
            && selectFields[ i ].getRename().length() > 0 ) {
            v.setName( selectFields[ i ].getRename() );
            v.setOrigin( name );
          }
          if ( selectFields[ i ].getLength() != UNDEFINED ) {
            v.setLength( selectFields[ i ].getLength() );
            v.setOrigin( name );
          }
          if ( selectFields[ i ].getPrecision() != UNDEFINED ) {
            v.setPrecision( selectFields[ i ].getPrecision() );
            v.setOrigin( name );
          }

          // Add to the resulting row!
          row.addValueMeta( v );
        }
      }

      if ( selectingAndSortingUnspecifiedFields ) {
        // Select the unspecified fields.
        // Sort the fields
        // Add them after the specified fields...
        //
        List<String> extra = new ArrayList<>();
        for ( int i = 0; i < inputRowMeta.size(); i++ ) {
          String fieldName = inputRowMeta.getValueMeta( i ).getName();
          if ( Const.indexOfString( fieldName, getSelectName() ) < 0 ) {
            extra.add( fieldName );
          }
        }
        Collections.sort( extra );
        for ( String fieldName : extra ) {
          IValueMeta extraValue = inputRowMeta.searchValueMeta( fieldName );
          row.addValueMeta( extraValue );
        }
      }

      // OK, now remove all from r and re-add row:
      inputRowMeta.clear();
      inputRowMeta.addRowMeta( row );
    }
  }

  public void getDeleteFields( IRowMeta inputRowMeta ) throws HopTransformException {
    if ( deleteName != null && deleteName.length > 0 ) { // DESELECT values from the stream...
      for ( int i = 0; i < deleteName.length; i++ ) {
        try {
          inputRowMeta.removeValueMeta( deleteName[ i ] );
        } catch ( HopValueException e ) {
          throw new HopTransformException( e );
        }
      }
    }
  }

  // Not called anywhere else in Hop. It's important to call the method below passing in the IVariables
  @Deprecated
  public void getMetadataFields( IRowMeta inputRowMeta, String name ) throws HopPluginException {
    getMetadataFields( inputRowMeta, name, null );
  }

  public void getMetadataFields( IRowMeta inputRowMeta, String name, IVariables variables ) throws HopPluginException {
    if ( meta != null && meta.length > 0 ) {
      // METADATA mode: change the meta-data of the values mentioned...

      for ( int i = 0; i < meta.length; i++ ) {
        SelectMetadataChange metaChange = meta[ i ];

        int idx = inputRowMeta.indexOfValue( metaChange.getName() );
        boolean metaTypeChangeUsesNewTypeDefaults = false; // Normal behavior as of 5.x or so
        if ( variables != null ) {
          metaTypeChangeUsesNewTypeDefaults = ValueMetaBase.convertStringToBoolean(
            variables.getVariable( Const.HOP_COMPATIBILITY_SELECT_VALUES_TYPE_CHANGE_USES_TYPE_DEFAULTS, "N" ) );
        }
        if ( idx >= 0 ) { // We found the value

          // This is the value we need to change:
          IValueMeta v = inputRowMeta.getValueMeta( idx );

          // Do we need to rename ?
          if ( !v.getName().equals( metaChange.getRename() ) && !Utils.isEmpty( metaChange.getRename() ) ) {
            v.setName( metaChange.getRename() );
            v.setOrigin( name );
          }
          // Change the type?
          if ( metaChange.getType() != IValueMeta.TYPE_NONE && v.getType() != metaChange.getType() ) {
            // Fix for PDI-16388 - clone copies over the conversion mask instead of using the default for the new type
            if ( !metaTypeChangeUsesNewTypeDefaults ) {
              v = ValueMetaFactory.cloneValueMeta( v, metaChange.getType() );
            } else {
              v = ValueMetaFactory.createValueMeta( v.getName(), metaChange.getType() );
            }

            // This is now a copy, replace it in the row!
            //
            inputRowMeta.setValueMeta( idx, v );

            // This also moves the data to normal storage type
            //
            v.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
          }
          if ( metaChange.getLength() != UNDEFINED ) {
            v.setLength( metaChange.getLength() );
            v.setOrigin( name );
          }
          if ( metaChange.getPrecision() != UNDEFINED ) {
            v.setPrecision( metaChange.getPrecision() );
            v.setOrigin( name );
          }
          if ( metaChange.getStorageType() >= 0 ) {
            v.setStorageType( metaChange.getStorageType() );
            v.setOrigin( name );
          }
          if ( !Utils.isEmpty( metaChange.getConversionMask() ) ) {
            v.setConversionMask( metaChange.getConversionMask() );
            v.setOrigin( name );
          }

          v.setDateFormatLenient( metaChange.isDateFormatLenient() );
          v.setDateFormatLocale( EnvUtil.createLocale( metaChange.getDateFormatLocale() ) );
          v.setDateFormatTimeZone( EnvUtil.createTimeZone( metaChange.getDateFormatTimeZone() ) );
          v.setLenientStringToNumber( metaChange.isLenientStringToNumber() );

          if ( !Utils.isEmpty( metaChange.getEncoding() ) ) {
            v.setStringEncoding( metaChange.getEncoding() );
            v.setOrigin( name );
          }
          if ( !Utils.isEmpty( metaChange.getDecimalSymbol() ) ) {
            v.setDecimalSymbol( metaChange.getDecimalSymbol() );
            v.setOrigin( name );
          }
          if ( !Utils.isEmpty( metaChange.getGroupingSymbol() ) ) {
            v.setGroupingSymbol( metaChange.getGroupingSymbol() );
            v.setOrigin( name );
          }
          if ( !Utils.isEmpty( metaChange.getCurrencySymbol() ) ) {
            v.setCurrencySymbol( metaChange.getCurrencySymbol() );
            v.setOrigin( name );
          }
        }
      }
    }
  }

  @Override
  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    try {
      IRowMeta rowMeta = inputRowMeta.clone();
      inputRowMeta.clear();
      inputRowMeta.addRowMeta( rowMeta );

      getSelectFields( inputRowMeta, name );
      getDeleteFields( inputRowMeta );
      getMetadataFields( inputRowMeta, name );
    } catch ( Exception e ) {
      throw new HopTransformException( e );
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    <fields>" );
    for ( int i = 0; i < selectFields.length; i++ ) {
      retval.append( "      <field>" );
      retval.append( "        " ).append( XmlHandler.addTagValue( "name", selectFields[ i ].getName() ) );
      if ( selectFields[ i ].getRename() != null ) {
        retval.append( "        " ).append( XmlHandler.addTagValue( "rename", selectFields[ i ].getRename() ) );
      }
      if ( selectFields[ i ].getPrecision() > 0 ) {
        retval.append( "        " ).append( XmlHandler.addTagValue( "length", selectFields[ i ].getLength() ) );
      }
      if ( selectFields[ i ].getPrecision() > 0 ) {
        retval.append( "        " ).append( XmlHandler.addTagValue( "precision", selectFields[ i ].getPrecision() ) );
      }
      retval.append( "      </field>" );
    }
    retval.append( "        " ).append( XmlHandler.addTagValue( "select_unspecified", selectingAndSortingUnspecifiedFields ) );
    for ( int i = 0; i < deleteName.length; i++ ) {
      retval.append( "      <remove>" );
      retval.append( "        " ).append( XmlHandler.addTagValue( "name", deleteName[ i ] ) );
      retval.append( "      </remove>" );
    }
    for ( int i = 0; i < meta.length; i++ ) {
      retval.append( meta[ i ].getXml() );
    }
    retval.append( "    </fields>" );

    return retval.toString();
  }

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                     String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;

    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "SelectValuesMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      /*
       * Take care of the normal SELECT fields...
       */
      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < this.selectFields.length; i++ ) {
        int idx = prev.indexOfValue( selectFields[ i ].getName() );
        if ( idx < 0 ) {
          errorMessage += "\t\t" + selectFields[ i ].getName() + Const.CR;
          errorFound = true;
        }
      }
      if ( errorFound ) {
        errorMessage =
          BaseMessages.getString( PKG, "SelectValuesMeta.CheckResult.SelectedFieldsNotFound" ) + Const.CR + Const.CR
            + errorMessage;

        cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "SelectValuesMeta.CheckResult.AllSelectedFieldsFound" ), transformMeta );
        remarks.add( cr );
      }

      if ( this.selectFields.length > 0 ) {
        // Starting from prev...
        for ( int i = 0; i < prev.size(); i++ ) {
          IValueMeta pv = prev.getValueMeta( i );
          int idx = Const.indexOfString( pv.getName(), getSelectName() );
          if ( idx < 0 ) {
            errorMessage += "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
            errorFound = true;
          }
        }
        if ( errorFound ) {
          errorMessage =
            BaseMessages.getString( PKG, "SelectValuesMeta.CheckResult.FieldsNotFound" ) + Const.CR + Const.CR
              + errorMessage;

          cr = new CheckResult( ICheckResult.TYPE_RESULT_COMMENT, errorMessage, transformMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "SelectValuesMeta.CheckResult.AllSelectedFieldsFound2" ), transformMeta );
          remarks.add( cr );
        }
      }

      /*
       * How about the DE-SELECT (remove) fields...
       */

      errorMessage = "";
      errorFound = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < this.deleteName.length; i++ ) {
        int idx = prev.indexOfValue( deleteName[ i ] );
        if ( idx < 0 ) {
          errorMessage += "\t\t" + deleteName[ i ] + Const.CR;
          errorFound = true;
        }
      }
      if ( errorFound ) {
        errorMessage =
          BaseMessages.getString( PKG, "SelectValuesMeta.CheckResult.DeSelectedFieldsNotFound" ) + Const.CR + Const.CR
            + errorMessage;

        cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "SelectValuesMeta.CheckResult.AllDeSelectedFieldsFound" ), transformMeta );
        remarks.add( cr );
      }

      /*
       * How about the Meta-fields...?
       */
      errorMessage = "";
      errorFound = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < this.meta.length; i++ ) {
        int idx = prev.indexOfValue( this.meta[ i ].getName() );
        if ( idx < 0 ) {
          errorMessage += "\t\t" + this.meta[ i ].getName() + Const.CR;
          errorFound = true;
        }
      }
      if ( errorFound ) {
        errorMessage =
          BaseMessages.getString( PKG, "SelectValuesMeta.CheckResult.MetadataFieldsNotFound" ) + Const.CR + Const.CR
            + errorMessage;

        cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "SelectValuesMeta.CheckResult.AllMetadataFieldsFound" ), transformMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
          "SelectValuesMeta.CheckResult.FieldsNotFound2" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "SelectValuesMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
          "SelectValuesMeta.CheckResult.NoInputReceivedError" ), transformMeta );
      remarks.add( cr );
    }

    // Check for doubles in the selected fields...
    int[] cnt = new int[ selectFields.length ];
    boolean errorFound = false;
    String errorMessage = "";

    for ( int i = 0; i < selectFields.length; i++ ) {
      cnt[ i ] = 0;
      for ( int j = 0; j < selectFields.length; j++ ) {
        if ( selectFields[ i ].getName().equals( selectFields[ j ].getName() ) ) {
          cnt[ i ]++;
        }
      }

      if ( cnt[ i ] > 1 ) {
        if ( !errorFound ) { // first time...
          errorMessage =
            BaseMessages.getString( PKG, "SelectValuesMeta.CheckResult.DuplicateFieldsSpecified" ) + Const.CR;
        } else {
          errorFound = true;
        }
        errorMessage +=
          BaseMessages.getString( PKG, "SelectValuesMeta.CheckResult.OccurentRow", i + " : " + selectFields[ i ]
            .getName() + "  (" + cnt[ i ] ) + Const.CR;
        errorFound = true;
      }
    }
    if ( errorFound ) {
      cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform createTransform( TransformMeta transformMeta, SelectValuesData data, int cnr, PipelineMeta pipelineMeta,
                                     Pipeline pipeline ) {
    return new SelectValues( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public SelectValuesData getTransformData() {
    return new SelectValuesData();
  }

  /**
   * @return the selectingAndSortingUnspecifiedFields
   */
  public boolean isSelectingAndSortingUnspecifiedFields() {
    return selectingAndSortingUnspecifiedFields;
  }

  /**
   * @param selectingAndSortingUnspecifiedFields the selectingAndSortingUnspecifiedFields to set
   */
  public void setSelectingAndSortingUnspecifiedFields( boolean selectingAndSortingUnspecifiedFields ) {
    this.selectingAndSortingUnspecifiedFields = selectingAndSortingUnspecifiedFields;
  }

  /**
   * @return the meta
   */
  public SelectMetadataChange[] getMeta() {
    return meta;
  }

  /**
   * @param meta the meta to set
   */
  public void setMeta( SelectMetadataChange[] meta ) {
    this.meta = meta == null ? new SelectMetadataChange[ 0 ] : meta;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public SelectField[] getSelectFields() {
    return selectFields;
  }

  public void setSelectFields( SelectField[] selectFields ) {
    this.selectFields = selectFields == null ? new SelectField[ 0 ] : selectFields;
  }

  /**
   * We will describe in which way the field names change between input and output in this transform.
   *
   * @return The list of field name lineage objects
   */
  public List<FieldnameLineage> getFieldnameLineage() {
    List<FieldnameLineage> lineages = new ArrayList<>();

    // Select values...
    //
    for ( int i = 0; i < selectFields.length; i++ ) {
      String input = selectFields[ i ].getName();
      String output = selectFields[ i ].getRename();

      // See if the select tab renames a column!
      //
      if ( !Utils.isEmpty( output ) && !input.equalsIgnoreCase( output ) ) {
        // Yes, add it to the list
        //
        lineages.add( new FieldnameLineage( input, output ) );
      }
    }

    // Metadata
    //
    for ( int i = 0; i < getMeta().length; i++ ) {
      String input = getMeta()[ i ].getName();
      String output = getMeta()[ i ].getRename();

      // See if the select tab renames a column!
      //
      if ( !Utils.isEmpty( output ) && !input.equalsIgnoreCase( output ) ) {
        // See if the input is not the output of a row in the Select tab
        //
        int idx = Const.indexOfString( input, getSelectRename() );

        if ( idx < 0 ) {
          // nothing special, add it to the list
          //
          lineages.add( new FieldnameLineage( input, output ) );
        } else {
          // Modify the existing field name lineage entry
          //
          FieldnameLineage lineage = FieldnameLineage.findFieldnameLineageWithInput( lineages, input );
          lineage.setOutputFieldname( output );
        }
      }
    }

    return lineages;
  }

  public static class SelectField implements Cloneable {

    /**
     * Select: Name of the selected field
     */
    @Injection( name = "FIELD_NAME", group = "FIELDS" )
    private String name;

    /**
     * Select: Rename to ...
     */
    @Injection( name = "FIELD_RENAME", group = "FIELDS" )
    private String rename;

    /**
     * Select: length of field
     */
    @Injection( name = "FIELD_LENGTH", group = "FIELDS" )
    private int length = UNDEFINED;

    /**
     * Select: Precision of field (for numbers)
     */
    @Injection( name = "FIELD_PRECISION", group = "FIELDS" )
    private int precision = UNDEFINED;

    public String getName() {
      return name;
    }

    public void setName( String name ) {
      this.name = name;
    }

    public String getRename() {
      return rename;
    }

    public void setRename( String rename ) {
      this.rename = rename;
    }

    public int getLength() {
      return length;
    }

    public void setLength( int length ) {
      this.length = length;
    }

    public int getPrecision() {
      return precision;
    }

    public void setPrecision( int precision ) {
      this.precision = precision;
    }

    @Override
    public SelectField clone() {
      try {
        return (SelectField) super.clone();
      } catch ( CloneNotSupportedException e ) {
        throw new RuntimeException( e );
      }
    }

  }
}
