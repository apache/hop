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

package org.apache.hop.pipeline.transforms.janino;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Contains the meta-data for the Formula transform: calculates ad-hoc formula's Powered by Pentaho's "libformula"
 * <p>
 * Created on 22-feb-2007
 */

public class JaninoMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = JaninoMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * The formula calculations to be performed
   */
  private JaninoMetaFunction[] formula;

  public JaninoMeta() {
    super(); // allocate BaseTransformMeta
  }

  public JaninoMetaFunction[] getFormula() {
    return formula;
  }

  public void setFormula( JaninoMetaFunction[] calcTypes ) {
    this.formula = calcTypes;
  }

  public void allocate( int nrCalcs ) {
    formula = new JaninoMetaFunction[ nrCalcs ];
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    int nrCalcs = XMLHandler.countNodes( transformNode, JaninoMetaFunction.XML_TAG );
    allocate( nrCalcs );
    for ( int i = 0; i < nrCalcs; i++ ) {
      Node calcnode = XMLHandler.getSubNodeByNr( transformNode, JaninoMetaFunction.XML_TAG, i );
      formula[ i ] = new JaninoMetaFunction( calcnode );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    if ( formula != null ) {
      for ( int i = 0; i < formula.length; i++ ) {
        retval.append( "       " + formula[ i ].getXML() + Const.CR );
      }
    }

    return retval.toString();
  }

  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      JaninoMeta m = (JaninoMeta) obj;
      return Objects.equals( getXML(), m.getXML() );
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode( formula );
  }

  public Object clone() {
    JaninoMeta retval = (JaninoMeta) super.clone();
    if ( formula != null ) {
      retval.allocate( formula.length );
      for ( int i = 0; i < formula.length; i++ ) {
        //CHECKSTYLE:Indentation:OFF
        retval.getFormula()[ i ] = (JaninoMetaFunction) formula[ i ].clone();
      }
    } else {
      retval.allocate( 0 );
    }
    return retval;
  }

  public void setDefault() {
    formula = new JaninoMetaFunction[ 0 ];
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    for ( int i = 0; i < formula.length; i++ ) {
      JaninoMetaFunction fn = formula[ i ];
      if ( Utils.isEmpty( fn.getReplaceField() ) ) {
        // Not replacing a field.
        if ( !Utils.isEmpty( fn.getFieldName() ) ) {
          // It's a new field!

          try {
            IValueMeta v = ValueMetaFactory.createValueMeta( fn.getFieldName(), fn.getValueType() );
            v.setLength( fn.getValueLength(), fn.getValuePrecision() );
            v.setOrigin( name );
            row.addValueMeta( v );
          } catch ( Exception e ) {
            throw new HopTransformException( e );
          }
        }
      } else {
        // Replacing a field
        int index = row.indexOfValue( fn.getReplaceField() );
        if ( index < 0 ) {
          throw new HopTransformException( "Unknown field specified to replace with a formula result: ["
            + fn.getReplaceField() + "]" );
        }
        // Change the data type etc.
        //
        IValueMeta v = row.getValueMeta( index ).clone();
        v.setLength( fn.getValueLength(), fn.getValuePrecision() );
        v.setOrigin( name );
        row.setValueMeta( index, v ); // replace it
      }
    }
  }

  /**
   * Checks the settings of this transform and puts the findings in a remarks List.
   *
   * @param remarks  The list to put the remarks in @see org.apache.hop.core.CheckResult
   * @param transformMeta The transformMeta to help checking
   * @param prev     The fields coming from the previous transform
   * @param input    The input transform names
   * @param output   The output transform names
   * @param info     The fields that are used as information by the transform
   */
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "JaninoMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JaninoMeta.CheckResult.FieldsReceived", "" + prev.size() ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JaninoMeta.CheckResult.ExpectedInputOk" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JaninoMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new Janino( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new JaninoData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public JaninoMetaInjection getTransformMetaInjectionInterface() {
    return new JaninoMetaInjection( this );
  }

  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return getTransformMetaInjectionInterface().extractTransformMetadataEntries();
  }

}
