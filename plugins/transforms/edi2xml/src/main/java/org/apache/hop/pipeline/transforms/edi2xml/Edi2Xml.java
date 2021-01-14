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

package org.apache.hop.pipeline.transforms.edi2xml;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.edi2xml.grammar.FastSimpleGenericEdifactDirectXMLLexer;
import org.apache.hop.pipeline.transforms.edi2xml.grammar.FastSimpleGenericEdifactDirectXMLParser;

public class Edi2Xml extends BaseTransform<Edi2XmlMeta, Edi2XmlData> implements ITransform<Edi2XmlMeta, Edi2XmlData> {

  private static final Class<?> PKG = Edi2XmlMeta.class; // For Translator

  private FastSimpleGenericEdifactDirectXMLLexer lexer;
  private CommonTokenStream tokens;
  private FastSimpleGenericEdifactDirectXMLParser parser;

  public Edi2Xml( TransformMeta transformMeta, Edi2XmlMeta meta, Edi2XmlData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, blocks when needed!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    String inputValue = "";

    if ( first ) {
      first = false;

      data.inputRowMeta = getInputRowMeta().clone();
      data.outputRowMeta = getInputRowMeta().clone();

      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      String realInputField = resolve( meta.getInputField() );
      String realOutputField = resolve( meta.getOutputField() );

      data.inputFieldIndex = getInputRowMeta().indexOfValue( realInputField );

      int numErrors = 0;

      if ( data.inputFieldIndex < 0 ) {
        logError( BaseMessages.getString( PKG, "Edi2Xml.Log.CouldNotFindInputField", realInputField ) );
        numErrors++;
      }

      if ( !data.inputRowMeta.getValueMeta( data.inputFieldIndex ).isString() ) {
        logError( BaseMessages.getString( PKG, "Edi2Xml.Log.InputFieldIsNotAString", realInputField ) );
        numErrors++;
      }

      if ( numErrors > 0 ) {
        setErrors( numErrors );
        stopAll();
        return false;
      }

      data.inputMeta = data.inputRowMeta.getValueMeta( data.inputFieldIndex );

      if ( Utils.isEmpty( meta.getOutputField() ) ) {
        // same field
        data.outputMeta = data.outputRowMeta.getValueMeta( data.inputFieldIndex );
        data.outputFieldIndex = data.inputFieldIndex;
      } else {
        // new field
        data.outputMeta = data.outputRowMeta.searchValueMeta( realOutputField );
        data.outputFieldIndex = data.outputRowMeta.size() - 1;
      }

      // create instances of lexer/tokenstream/parser
      // treat null values as empty strings for parsing purposes
      inputValue = Const.NVL( data.inputMeta.getString( r[ data.inputFieldIndex ] ), "" );

      lexer = new FastSimpleGenericEdifactDirectXMLLexer( new ANTLRStringStream( inputValue ) );
      tokens = new CommonTokenStream( lexer );
      parser = new FastSimpleGenericEdifactDirectXMLParser( tokens );

    } else {

      // treat null values as empty strings for parsing purposes
      inputValue = Const.NVL( data.inputMeta.getString( r[ data.inputFieldIndex ] ), "" );

      lexer.setCharStream( new ANTLRStringStream( inputValue ) );
      tokens.setTokenSource( lexer );
      parser.setTokenStream( tokens );

    }

    try {
      parser.edifact();
      // make sure the row is big enough
      r = RowDataUtil.resizeArray( r, data.outputRowMeta.size() );
      // place parsing result into output field
      r[ data.outputFieldIndex ] = parser.buf.toString();
      putRow( data.outputRowMeta, r );

    } catch ( MismatchedTokenException e ) {

      StringBuilder errorMessage = new StringBuilder( 180 );
      errorMessage.append( "error parsing edi on line " + e.line + " position " + e.charPositionInLine );
      errorMessage.append( ": expecting "
        + ( ( e.expecting > -1 ) ? parser.getTokenNames()[ e.expecting ] : "<UNKNOWN>" ) + " but found " );
      errorMessage.append( ( e.token.getType() >= 0 ) ? parser.getTokenNames()[ e.token.getType() ] : "<EOF>" );

      if ( getTransformMeta().isDoingErrorHandling() ) {
        putError(
          getInputRowMeta(), r, 1L, errorMessage.toString(), resolve( meta.getInputField() ),
          "MALFORMED_EDI" );
      } else {
        logError( errorMessage.toString() );

        // try to determine the error line
        String errorline = "<UNKNOWN>";
        try {
          errorline = inputValue.split( "\\r?\\n" )[ e.line - 1 ];
        } catch ( Exception ee ) {
          // Ignore pattern syntax errors
        }

        logError( "Problem line: " + errorline );
        logError( StringUtils.leftPad( "^", e.charPositionInLine + "Problem line: ".length() + 1 ) );
        throw new HopException( e );
      }
    } catch ( RecognitionException e ) {
      StringBuilder errorMessage = new StringBuilder( 180 );
      errorMessage.append( "error parsing edi on line " ).append( e.line ).append( " position " ).append(
        e.charPositionInLine ).append( ". " ).append( e.toString() );

      if ( getTransformMeta().isDoingErrorHandling() ) {
        putError(
          getInputRowMeta(), r, 1L, errorMessage.toString(), resolve( meta.getInputField() ),
          "MALFORMED_EDI" );
      } else {
        logError( errorMessage.toString() );

        // try to determine the error line
        String errorline = "<UNKNOWN>";
        try {
          errorline = inputValue.split( "\\r?\\n" )[ e.line - 1 ];
        } catch ( Exception ee ) {
          // Ignore pattern syntax errors
        }

        logError( "Problem line: " + errorline );
        logError( StringUtils.leftPad( "^", e.charPositionInLine + "Problem line: ".length() + 1 ) );

        throw new HopException( e );
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Linenr " + getLinesRead() );
    }

    return true;
  }

  public void dispose(){

    data.inputMeta = null;
    data.inputRowMeta = null;
    data.outputMeta = null;
    data.outputRowMeta = null;

    super.dispose();
  }

}
