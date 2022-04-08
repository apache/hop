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

package org.apache.hop.pipeline.transforms.formula;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
        id = "Formula",
        image = "FRM.svg",
        name = "i18n::Formula.name",
        description = "i18n::Formula.description",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
        keywords = "i18n::Formula.keywords",
        documentationUrl = "/pipeline/transforms/formula.html")
public class FormulaMeta extends BaseTransformMeta<Formula, FormulaData> {
    private static final Class<?> PKG = Formula.class; // For Translator

    /** The formula calculations to be performed */
    private FormulaMetaFunction[] formulas;

    public FormulaMeta() {
        super();
    }

    public void setFormulas(FormulaMetaFunction[] formulas){
        this.formulas = formulas;
    }
    public FormulaMetaFunction[] getFormulas() {
        return formulas;
    }

    public void allocate( int nrCalcs ) {
        formulas = new FormulaMetaFunction[nrCalcs];
    }

    @Override
    public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider) throws HopXmlException {
        int nrCalcs = XmlHandler.countNodes( transformNode, FormulaMetaFunction.XML_TAG );
        allocate( nrCalcs );
        for ( int i = 0; i < nrCalcs; i++ ) {
            Node calcnode = XmlHandler.getSubNodeByNr( transformNode, FormulaMetaFunction.XML_TAG, i );
            formulas[i] = new FormulaMetaFunction( calcnode );
        }
    }

    @Override
    public String getXml() throws HopException {
        StringBuilder retval = new StringBuilder(200);

        if ( formulas != null ) {
            for (int i = 0; i < formulas.length; i++ ) {
                retval.append( "       " + formulas[i].getXML() + Const.CR );
            }
        }

        return retval.toString();

    }

    @Override
    public Object clone(){
        FormulaMeta retval = (FormulaMeta) super.clone();
        if ( formulas != null ) {
            retval.allocate( formulas.length );
            for (int i = 0; i < formulas.length; i++ ) {
                //CHECKSTYLE:Indentation:OFF
                retval.getFormulas()[i] = (FormulaMetaFunction) formulas[i].clone();
            }
        } else {
            retval.allocate( 0 );
        }
        return retval;
    }

    @Override
    public void getFields(IRowMeta row, String name, IRowMeta[] info, TransformMeta nextStep,
                          IVariables space, IHopMetadataProvider metadataProvider ) throws HopTransformException {
        for ( int i = 0; i < formulas.length; i++ ) {
            FormulaMetaFunction fn = formulas[i];
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
}
