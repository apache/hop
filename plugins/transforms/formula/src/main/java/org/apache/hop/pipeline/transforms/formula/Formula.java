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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.formula.util.FormulaParser;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.util.Arrays;

public class Formula extends BaseTransform<FormulaMeta, FormulaData> {
    private static final Class<?> PKG = Formula.class; // For Translator

    private XSSFWorkbook workBook;
    private XSSFFormulaEvaluator evaluator;
    private XSSFSheet workSheet;
    private Row sheetRow;

    @Override
    public boolean init(){

        workBook = new XSSFWorkbook();
//        evaluator = workBook.getCreationHelper().createFormulaEvaluator();
        workSheet = workBook.createSheet();
        sheetRow = workSheet.createRow(0);

        data.returnType = new int[meta.getFormulas().length];
        for ( int i = 0; i < meta.getFormulas().length; i++ ) {
            data.returnType[i] = -1;
        }

        return true;
    }

    @Override
    public void dispose(){
        try {
            workBook.close();
        } catch (IOException e) {
            logError("Unable to close temporary workbook", e);
        }
        super.dispose();
    }

    @Override
    public boolean processRow() throws HopException {

        Object[] r = getRow();
        int tempIndex = getInputRowMeta().size();

        if(r == null){
            setOutputDone();
            return false;
        }

        if(first){
            first = false;

            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

            // Calculate replace indexes...
            //
            data.replaceIndex = new int[meta.getFormulas().length];
            for ( int i = 0; i < meta.getFormulas().length; i++ ) {
                FormulaMetaFunction fn = meta.getFormulas()[i];
                if ( !Utils.isEmpty( fn.getReplaceField() ) ) {
                    data.replaceIndex[i] = getInputRowMeta().indexOfValue( fn.getReplaceField() );
                    if ( data.replaceIndex[i] < 0 ) {
                        throw new HopException( "Unknown field specified to replace with a formula result: ["
                                + fn.getReplaceField() + "]" );
                    }
                } else {
                    data.replaceIndex[i] = -1;
                }
            }

        }

        if ( log.isRowLevel() ) {
            logRowlevel( "Read row #" + getLinesRead() + " : " + Arrays.toString( r ) );
        }

        if(sheetRow != null){
            workSheet.removeRow(sheetRow);
        }
        sheetRow = workSheet.createRow(0);

//        data.replaceIndex = new int[meta.getFormulas().length];
        Object outputValue = null;
        Object[] outputRowData = RowDataUtil.resizeArray(r, data.outputRowMeta.size());

        for(int i = 0; i < meta.getFormulas().length; i++){

            FormulaMetaFunction formula = meta.getFormulas()[i];
            FormulaParser parser = new FormulaParser(formula, getInputRowMeta(), r, sheetRow );
            CellValue cellValue = parser.getFormulaValue();

            CellType cellType = cellValue.getCellType();

            // Calculate the return type on the first row...
            // for most cases we can try to convert data on a fly.
            if ( data.returnType[i] < 0 ) {
                switch (cellType) {
                    case BLANK:
                        // should never happen.
                        break;
                    case NUMERIC:
                        outputValue = cellValue.getNumberValue();
                        int outputValueType = formula.getValueType();

                        switch (outputValueType) {
                            case IValueMeta.TYPE_NUMBER:
                                data.returnType[i] = FormulaData.RETURN_TYPE_NUMBER;
                                formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_NUMBER);
                                break;
                            case IValueMeta.TYPE_INTEGER:
                                data.returnType[i] = FormulaData.RETURN_TYPE_INTEGER;
                                formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_NUMBER);
                                break;
                            case IValueMeta.TYPE_BIGNUMBER:
                                data.returnType[i] = FormulaData.RETURN_TYPE_BIGDECIMAL;
                                formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_NUMBER);
                                break;
                            case IValueMeta.TYPE_DATE:
                                outputValue = DateUtil.getJavaDate(cellValue.getNumberValue());
                                data.returnType[i] = FormulaData.RETURN_TYPE_DATE;
                                formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_NUMBER);
                                break;
                            case IValueMeta.TYPE_TIMESTAMP:
                                data.returnType[i] = FormulaData.RETURN_TYPE_TIMESTAMP;
                                formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_NUMBER);
                                break;
                            default:
                                break;
                        }
                        // get cell value
                        break;
                    case BOOLEAN:
                        outputValue = cellValue.getBooleanValue();
                        data.returnType[i] = FormulaData.RETURN_TYPE_BOOLEAN;
                        formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_BOOLEAN);
                        break;
                    case STRING:
                        outputValue = cellValue.getStringValue();
                        data.returnType[i] = FormulaData.RETURN_TYPE_STRING;
                        formula.setNeedDataConversion(formula.getValueType() != IValueMeta.TYPE_STRING);
                        break;
                    default:
                        break;
                }
            }

            int realIndex = (data.replaceIndex[i] < 0) ? tempIndex++: data.replaceIndex[i];

            outputRowData[realIndex] = getReturnValue(outputValue, data.returnType[i], realIndex, formula);
       }

        putRow(data.outputRowMeta, outputRowData);
        if ( log.isRowLevel() ) {
            logRowlevel( "Wrote row #" + getLinesWritten() + " : " + Arrays.toString( r ) );
        }
        if ( checkFeedback( getLinesRead() ) ) {
            logBasic( "Linenr " + getLinesRead() );
        }

        return true;
    }

    /**
     * This is the base transform that forms that basis for all transforms. You can derive from this
     * class to implement your own transforms.
     *
     * @param transformMeta The TransformMeta object to run.
     * @param meta
     * @param data          the data object to store temporary data, database connections, caches, result sets,
     *                      hashtables etc.
     * @param copyNr        The copynumber for this transform.
     * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
     * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
     */
    public Formula(TransformMeta transformMeta, FormulaMeta meta, FormulaData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
        super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);//TODO
    }

    protected Object getReturnValue( Object formulaResult, int returnType, int realIndex, FormulaMetaFunction fn )
            throws HopException {
        if ( formulaResult == null ) {
            return null;
        }
        Object value = null;
        switch ( returnType ) {
            case FormulaData.RETURN_TYPE_STRING:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = formulaResult.toString();
                }
                break;
            case FormulaData.RETURN_TYPE_NUMBER:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = new Double( ( (Number) formulaResult ).doubleValue() );
                }
                break;
            case FormulaData.RETURN_TYPE_INTEGER:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = (Long)formulaResult;
                }
                break;
            case FormulaData.RETURN_TYPE_LONG:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = formulaResult;
                }
                break;
            case FormulaData.RETURN_TYPE_DATE:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = formulaResult;
                }
                break;
            case FormulaData.RETURN_TYPE_BIGDECIMAL:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = formulaResult;
                }
                break;
            case FormulaData.RETURN_TYPE_BYTE_ARRAY:
                value = formulaResult;
                break;
            case FormulaData.RETURN_TYPE_BOOLEAN:
                value = formulaResult;
                break;
            case FormulaData.RETURN_TYPE_TIMESTAMP:
                if ( fn.isNeedDataConversion() ) {
                    value = convertDataToTargetValueMeta( realIndex, formulaResult );
                } else {
                    value = formulaResult;
                }
                break;
        } //if none case is caught - null is returned.
        return value;
    }

    private Object convertDataToTargetValueMeta( int i, Object formulaResult ) throws HopException {
        if ( formulaResult == null ) {
            return formulaResult;
        }
        IValueMeta target = data.outputRowMeta.getValueMeta( i );
        IValueMeta actual = ValueMetaFactory.guessValueMetaInterface(formulaResult);
        Object value = target.convertData( actual, formulaResult );
        return value;
    }
}
