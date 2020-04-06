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

package org.apache.hop.pipeline.transforms.validator;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calculate new field values using pre-defined functions.
 *
 * @author Matt
 * @since 8-sep-2005
 */
public class Validator extends BaseTransform implements ITransform {
  private static Class<?> PKG = ValidatorMeta.class; // for i18n purposes, needed by Translator!!

  public class FieldIndexes {
    public int indexName;
    public int indexA;
    public int indexB;
    public int indexC;
  }

  private ValidatorMeta meta;
  private ValidatorData data;

  public Validator( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (ValidatorMeta) smi;
    data = (ValidatorData) sdi;

    Object[] r;

    if ( first ) {
      first = false;

      readSourceValuesFromInfoTransforms();

      // Read the row AFTER the info rows
      // That way the info-rowsets are out of the way
      //
      r = getRow(); // get row, set busy!
      if ( r == null ) { // no more input to be expected...

        setOutputDone();
        return false;
      }

      data.fieldIndexes = new int[ meta.getValidations().size() ];

      // Calculate the indexes of the values and arguments in the target data or temporary data
      // We do this in advance to save time later on.
      //
      for ( int i = 0; i < meta.getValidations().size(); i++ ) {
        Validation field = meta.getValidations().get( i );

        if ( !Utils.isEmpty( field.getFieldName() ) ) {
          data.fieldIndexes[ i ] = getInputRowMeta().indexOfValue( field.getFieldName() );
          if ( data.fieldIndexes[ i ] < 0 ) {
            // Nope: throw an exception
            throw new HopTransformException( "Unable to find the specified fieldname '"
              + field.getFieldName() + "' for validation#" + ( i + 1 ) );
          }
        } else {
          throw new HopTransformException( "There is no name specified for validator field #" + ( i + 1 ) );
        }
      }
    } else {
      // Read the row AFTER the info rows
      // That way the info-rowsets are out of the way
      //
      r = getRow(); // get row, set busy!
      if ( r == null ) { // no more input to be expected...

        setOutputDone();
        return false;
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( "Read row #" + getLinesRead() + " : " + getInputRowMeta().getString( r ) );
    }

    try {
      List<HopValidatorException> exceptions = validateFields( getInputRowMeta(), r );
      if ( exceptions.size() > 0 ) {
        if ( getTransformMeta().isDoingErrorHandling() ) {
          if ( meta.isConcatenatingErrors() ) {
            StringBuilder messages = new StringBuilder();
            StringBuilder fields = new StringBuilder();
            StringBuilder codes = new StringBuilder();
            boolean notFirst = false;
            for ( HopValidatorException e : exceptions ) {
              if ( notFirst ) {
                messages.append( meta.getConcatenationSeparator() );
                fields.append( meta.getConcatenationSeparator() );
                codes.append( meta.getConcatenationSeparator() );
              } else {
                notFirst = true;
              }
              messages.append( e.getMessage() );
              fields.append( e.getFieldname() );
              codes.append( e.getCodeDesc() );
            }
            putError( getInputRowMeta(), r, exceptions.size(), messages.toString(), fields.toString(), codes
              .toString() );
          } else {
            for ( HopValidatorException e : exceptions ) {
              putError( getInputRowMeta(), r, 1, e.getMessage(), e.getFieldname(), e.getCodeDesc() );
            }
          }
        } else {
          HopValidatorException e = exceptions.get( 0 );
          throw new HopException( e.getMessage(), e );
        }
      } else {
        putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).
      }
    } catch ( HopValidatorException e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        putError( getInputRowMeta(), r, 1, e.getMessage(), e.getFieldname(), e.getCodeDesc() );
      } else {
        throw new HopException( e.getMessage(), e );
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( "Wrote row #" + getLinesWritten() + " : " + getInputRowMeta().getString( r ) );
    }
    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Linenr " + getLinesRead() );
    }

    return true;
  }

  void readSourceValuesFromInfoTransforms() throws HopTransformException {
    Map<String, Integer> inputTransformWasProcessed = new HashMap<>();
    for ( int i = 0; i < meta.getValidations().size(); i++ ) {
      Validation field = meta.getValidations().get( i );
      List<StreamInterface> streams = meta.getTransformIOMeta().getInfoStreams();

      // If we need to source the allowed values data from a different transform, we do this here as well
      //
      if ( field.isSourcingValues() ) {
        if ( streams.get( i ).getTransformMeta() == null ) {
          throw new HopTransformException(
            "There is no valid source transform specified for the allowed values of validation ["
              + field.getName() + "]" );
        }
        if ( Utils.isEmpty( field.getSourcingField() ) ) {
          throw new HopTransformException(
            "There is no valid source field specified for the allowed values of validation ["
              + field.getName() + "]" );
        }

        // Still here : OK, read the data from the specified transform...
        // The data is stored in data.listValues[i] and data.constantsMeta
        //
        String transformName = streams.get( i ).getTransformName();
        if ( inputTransformWasProcessed.containsKey( transformName ) ) {
          // transform was processed for other StreamInterface
          data.listValues[ i ] = data.listValues[ inputTransformWasProcessed.get( transformName ) ];
          data.constantsMeta[ i ] = data.constantsMeta[ inputTransformWasProcessed.get( transformName ) ];
          continue;
        }
        RowSet allowedRowSet = findInputRowSet( transformName );
        int fieldIndex = -1;
        List<Object> allowedValues = new ArrayList<Object>();
        Object[] allowedRowData = getRowFrom( allowedRowSet );
        while ( allowedRowData != null ) {
          IRowMeta allowedRowMeta = allowedRowSet.getRowMeta();
          if ( fieldIndex < 0 ) {
            fieldIndex = allowedRowMeta.indexOfValue( field.getSourcingField() );
            if ( fieldIndex < 0 ) {
              throw new HopTransformException( "Source field ["
                + field.getSourcingField() + "] is not found in the source row data" );
            }
            data.constantsMeta[ i ] = allowedRowMeta.getValueMeta( fieldIndex );
          }
          Object allowedValue = allowedRowData[ fieldIndex ];
          if ( allowedValue != null ) {
            allowedValues.add( allowedValue );
          }

          // Grab another row too...
          //
          allowedRowData = getRowFrom( allowedRowSet );
        }
        // Set the list values in the data block...
        //
        data.listValues[ i ] = allowedValues.toArray( new Object[ allowedValues.size() ] );
        inputTransformWasProcessed.put( transformName, i );
      }
    }
  }

  /**
   * @param inputRowMeta the input row metadata
   * @param r            the input row (data)
   * @throws HopValidatorException in case there is a validation error, details are stored in the exception.
   */
  private List<HopValidatorException> validateFields( IRowMeta inputRowMeta, Object[] r )
    throws HopValueException {
    List<HopValidatorException> exceptions = new ArrayList<HopValidatorException>();

    for ( int i = 0; i < meta.getValidations().size(); i++ ) {
      Validation field = meta.getValidations().get( i );

      int valueIndex = data.fieldIndexes[ i ];
      IValueMeta validatorMeta = data.constantsMeta[ i ];

      IValueMeta valueMeta = inputRowMeta.getValueMeta( valueIndex );
      Object valueData = r[ valueIndex ];

      // Check for null
      //
      boolean isNull = valueMeta.isNull( valueData );
      if ( !field.isNullAllowed() && isNull ) {
        HopValidatorException exception =
          new HopValidatorException(
            this,
            field,
            HopValidatorException.ERROR_NULL_VALUE_NOT_ALLOWED,
            BaseMessages.getString(
              PKG, "Validator.Exception.NullNotAllowed", field.getFieldName(), inputRowMeta.getString( r ) ),
            field.getFieldName() );
        exceptions.add( exception );
        if ( !meta.isValidatingAll() ) {
          return exceptions;
        }
      }

      if ( field.isOnlyNullAllowed() && !isNull ) {
        HopValidatorException exception =
          new HopValidatorException(
            this,
            field,
            HopValidatorException.ERROR_ONLY_NULL_VALUE_ALLOWED,
            BaseMessages.getString(
              PKG, "Validator.Exception.OnlyNullAllowed", field.getFieldName(), inputRowMeta.getString( r ) ),
            field.getFieldName() );
        exceptions.add( exception );
        if ( !meta.isValidatingAll() ) {
          return exceptions;
        }
      }

      // Check the data type!
      //
      if ( field.isDataTypeVerified() && field.getDataType() != IValueMeta.TYPE_NONE ) {

        // Same data type?
        //
        if ( field.getDataType() != valueMeta.getType() ) {
          HopValidatorException exception =
            new HopValidatorException(
              this, field, HopValidatorException.ERROR_UNEXPECTED_DATA_TYPE, BaseMessages.getString(
              PKG, "Validator.Exception.UnexpectedDataType", field.getFieldName(), valueMeta
                .toStringMeta(), validatorMeta.toStringMeta() ), field.getFieldName() );
          exceptions.add( exception );
          if ( !meta.isValidatingAll() ) {
            return exceptions;
          }
        }
      }

      // Check various things if the value is not null..
      //
      if ( !isNull ) {

        if ( data.fieldsMinimumLengthAsInt[ i ] >= 0
          || data.fieldsMaximumLengthAsInt[ i ] >= 0 || data.minimumValue[ i ] != null
          || data.maximumValue[ i ] != null || data.listValues[ i ].length > 0 || field.isSourcingValues()
          || !Utils.isEmpty( data.startString[ i ] ) || !Utils.isEmpty( data.endString[ i ] )
          || !Utils.isEmpty( data.startStringNotAllowed[ i ] ) || !Utils.isEmpty( data.endStringNotAllowed[ i ] )
          || field.isOnlyNumericAllowed() || data.patternExpected[ i ] != null
          || data.patternDisallowed[ i ] != null ) {

          String stringValue = valueMeta.getString( valueData );
          int stringLength = stringValue.length();

          // Minimum length
          //
          // if (field.getMinimumLength()>=0 && stringValue.length()<field.getMinimumLength() ) {
          if ( data.fieldsMinimumLengthAsInt[ i ] >= 0 && stringLength < data.fieldsMinimumLengthAsInt[ i ] ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_SHORTER_THAN_MINIMUM_LENGTH, BaseMessages
                .getString(
                  PKG, "Validator.Exception.ShorterThanMininumLength", field.getFieldName(), valueMeta
                    .getString( valueData ), Integer.toString( stringValue.length() ), field
                    .getMinimumLength() ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Maximum length
          //
          // if (field.getMaximumLength()>=0 && stringValue.length()>field.getMaximumLength() ) {
          if ( data.fieldsMaximumLengthAsInt[ i ] >= 0 && stringLength > data.fieldsMaximumLengthAsInt[ i ] ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_LONGER_THAN_MAXIMUM_LENGTH, BaseMessages
                .getString(
                  PKG, "Validator.Exception.LongerThanMaximumLength", field.getFieldName(), valueMeta
                    .getString( valueData ), Integer.toString( stringValue.length() ), field
                    .getMaximumLength() ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Minimal value
          //
          if ( data.minimumValue[ i ] != null
            && valueMeta.compare( valueData, validatorMeta, data.minimumValue[ i ] ) < 0 ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_LOWER_THAN_ALLOWED_MINIMUM,
                BaseMessages.getString(
                  PKG, "Validator.Exception.LowerThanMinimumValue", field.getFieldName(), valueMeta
                    .getString( valueData ), data.constantsMeta[ i ].getString( data.minimumValue[ i ] ) ),
                field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Maximum value
          //
          if ( data.maximumValue[ i ] != null
            && valueMeta.compare( valueData, validatorMeta, data.maximumValue[ i ] ) > 0 ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_HIGHER_THAN_ALLOWED_MAXIMUM,
                BaseMessages.getString( PKG, "Validator.Exception.HigherThanMaximumValue", field
                  .getFieldName(), valueMeta.getString( valueData ), data.constantsMeta[ i ]
                  .getString( data.maximumValue[ i ] ) ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // In list?
          //
          if ( field.isSourcingValues() || data.listValues[ i ].length > 0 ) {
            boolean found = false;
            for ( Object object : data.listValues[ i ] ) {
              if ( object != null
                && data.listValues[ i ] != null && valueMeta.compare( valueData, validatorMeta, object ) == 0 ) {
                found = true;
              }
            }
            if ( !found ) {
              HopValidatorException exception =
                new HopValidatorException(
                  this, field, HopValidatorException.ERROR_VALUE_NOT_IN_LIST, BaseMessages.getString(
                  PKG, "Validator.Exception.NotInList", field.getFieldName(), valueMeta
                    .getString( valueData ) ), field.getFieldName() );
              exceptions.add( exception );
              if ( !meta.isValidatingAll() ) {
                return exceptions;
              }
            }
          }

          // Numeric data or strings with only
          if ( field.isOnlyNumericAllowed() ) {
            HopValidatorException exception = assertNumeric( valueMeta, valueData, field );
            if ( exception != null ) {
              exceptions.add( exception );
              if ( !meta.isValidatingAll() ) {
                return exceptions;
              }
            }
          }

          // Does not start with string value
          //
          if ( !Utils.isEmpty( data.startString[ i ] ) && !stringValue.startsWith( data.startString[ i ] ) ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_DOES_NOT_START_WITH_STRING, BaseMessages
                .getString(
                  PKG, "Validator.Exception.DoesNotStartWithString", field.getFieldName(), valueMeta
                    .getString( valueData ), field.getStartString() ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Ends with string value
          //
          if ( !Utils.isEmpty( data.endString[ i ] ) && !stringValue.endsWith( data.endString[ i ] ) ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_DOES_NOT_END_WITH_STRING, BaseMessages.getString(
                PKG, "Validator.Exception.DoesNotEndWithString", field.getFieldName(), valueMeta
                  .getString( valueData ), field.getEndString() ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Starts with string value
          //
          if ( !Utils.isEmpty( data.startStringNotAllowed[ i ] )
            && stringValue.startsWith( data.startStringNotAllowed[ i ] ) ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_STARTS_WITH_STRING, BaseMessages.getString(
                PKG, "Validator.Exception.StartsWithString", field.getFieldName(), valueMeta
                  .getString( valueData ), field.getStartStringNotAllowed() ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Ends with string value
          //
          if ( !Utils.isEmpty( data.endStringNotAllowed[ i ] ) && stringValue
            .endsWith( data.endStringNotAllowed[ i ] ) ) {
            HopValidatorException exception =
              new HopValidatorException(
                this, field, HopValidatorException.ERROR_ENDS_WITH_STRING, BaseMessages.getString(
                PKG, "Validator.Exception.EndsWithString", field.getFieldName(), valueMeta
                  .getString( valueData ), field.getEndStringNotAllowed() ), field.getFieldName() );
            exceptions.add( exception );
            if ( !meta.isValidatingAll() ) {
              return exceptions;
            }
          }

          // Matching regular expression allowed?
          //
          if ( data.patternExpected[ i ] != null ) {
            Matcher matcher = data.patternExpected[ i ].matcher( stringValue );
            if ( !matcher.matches() ) {
              HopValidatorException exception =
                new HopValidatorException(
                  this, field, HopValidatorException.ERROR_MATCHING_REGULAR_EXPRESSION_EXPECTED,
                  BaseMessages.getString( PKG, "Validator.Exception.MatchingRegExpExpected", field
                    .getFieldName(), valueMeta.getString( valueData ), data.regularExpression[ i ] ), field
                  .getFieldName() );
              exceptions.add( exception );
              if ( !meta.isValidatingAll() ) {
                return exceptions;
              }
            }
          }

          // Matching regular expression NOT allowed?
          //
          if ( data.patternDisallowed[ i ] != null ) {
            Matcher matcher = data.patternDisallowed[ i ].matcher( stringValue );
            if ( matcher.matches() ) {
              HopValidatorException exception =
                new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_MATCHING_REGULAR_EXPRESSION_NOT_ALLOWED,
                  BaseMessages.getString( PKG, "Validator.Exception.MatchingRegExpNotAllowed", field
                    .getFieldName(), valueMeta.getString( valueData ), data.regularExpressionNotAllowed[ i ] ),
                  field.getFieldName() );
              exceptions.add( exception );
              if ( !meta.isValidatingAll() ) {
                return exceptions;
              }
            }
          }
        }
      }
    }

    return exceptions;
  }

  // package-local visibility for testing purposes
  HopValidatorException assertNumeric( IValueMeta valueMeta,
                                       Object valueData,
                                       Validation field ) throws HopValueException {
    if ( valueMeta.isNumeric() || containsOnlyDigits( valueMeta.getString( valueData ) ) ) {
      return null;
    }
    return new HopValidatorException( this, field, HopValidatorException.ERROR_NON_NUMERIC_DATA,
      BaseMessages.getString( PKG, "Validator.Exception.NonNumericDataNotAllowed", field.getFieldName(),
        valueMeta.toStringMeta() ), field.getFieldName() );
  }

  private boolean containsOnlyDigits( String string ) {
    for ( char c : string.toCharArray() ) {
      if ( c < '0' || c > '9' ) {
        return false;
      }
    }
    return true;
  }

  public boolean init() {
    meta = (ValidatorMeta) smi;
    data = (ValidatorData) sdi;

    if ( super.init() ) {
      // initialize transforms by names
      List<TransformMeta> transforms = new ArrayList<>();
      List<TransformMetaDataCombi> pipelineTransforms = getPipeline().getTransforms();
      if ( pipelineTransforms != null ) {
        for ( TransformMetaDataCombi s : pipelineTransforms ) {
          transforms.add( s.transformMeta );
        }
      }
      meta.searchInfoAndTargetTransforms( transforms );

      // initialize arrays of validation data
      data.constantsMeta = new IValueMeta[ meta.getValidations().size() ];
      data.minimumValueAsString = new String[ meta.getValidations().size() ];
      data.maximumValueAsString = new String[ meta.getValidations().size() ];
      data.fieldsMinimumLengthAsInt = new int[ meta.getValidations().size() ];
      data.fieldsMaximumLengthAsInt = new int[ meta.getValidations().size() ];
      data.minimumValue = new Object[ meta.getValidations().size() ];
      data.maximumValue = new Object[ meta.getValidations().size() ];
      data.listValues = new Object[ meta.getValidations().size() ][];
      data.errorCode = new String[ meta.getValidations().size() ];
      data.errorDescription = new String[ meta.getValidations().size() ];
      data.conversionMask = new String[ meta.getValidations().size() ];
      data.decimalSymbol = new String[ meta.getValidations().size() ];
      data.groupingSymbol = new String[ meta.getValidations().size() ];
      data.maximumLength = new String[ meta.getValidations().size() ];
      data.minimumLength = new String[ meta.getValidations().size() ];
      data.startString = new String[ meta.getValidations().size() ];
      data.endString = new String[ meta.getValidations().size() ];
      data.startStringNotAllowed = new String[ meta.getValidations().size() ];
      data.endStringNotAllowed = new String[ meta.getValidations().size() ];
      data.regularExpression = new String[ meta.getValidations().size() ];
      data.regularExpressionNotAllowed = new String[ meta.getValidations().size() ];
      data.patternExpected = new Pattern[ meta.getValidations().size() ];
      data.patternDisallowed = new Pattern[ meta.getValidations().size() ];

      for ( int i = 0; i < meta.getValidations().size(); i++ ) {

        Validation field = meta.getValidations().get( i );
        try {
          data.constantsMeta[ i ] = createValueMeta( field.getFieldName(), field.getDataType() );
          data.constantsMeta[ i ].setConversionMask( field.getConversionMask() );
          data.constantsMeta[ i ].setDecimalSymbol( field.getDecimalSymbol() );
          data.constantsMeta[ i ].setGroupingSymbol( field.getGroupingSymbol() );
          data.errorCode[ i ] = environmentSubstitute( Const.NVL( field.getErrorCode(), "" ) );
          data.errorDescription[ i ] = environmentSubstitute( Const.NVL( field.getErrorDescription(), "" ) );
          data.conversionMask[ i ] = environmentSubstitute( Const.NVL( field.getConversionMask(), "" ) );
          data.decimalSymbol[ i ] = environmentSubstitute( Const.NVL( field.getDecimalSymbol(), "" ) );
          data.groupingSymbol[ i ] = environmentSubstitute( Const.NVL( field.getGroupingSymbol(), "" ) );
          data.maximumLength[ i ] = environmentSubstitute( Const.NVL( field.getMaximumLength(), "" ) );
          data.minimumLength[ i ] = environmentSubstitute( Const.NVL( field.getMinimumLength(), "" ) );
          data.maximumValueAsString[ i ] = environmentSubstitute( Const.NVL( field.getMaximumValue(), "" ) );
          data.minimumValueAsString[ i ] = environmentSubstitute( Const.NVL( field.getMinimumValue(), "" ) );
          data.startString[ i ] = environmentSubstitute( Const.NVL( field.getStartString(), "" ) );
          data.endString[ i ] = environmentSubstitute( Const.NVL( field.getEndString(), "" ) );
          data.startStringNotAllowed[ i ] =
            environmentSubstitute( Const.NVL( field.getStartStringNotAllowed(), "" ) );
          data.endStringNotAllowed[ i ] = environmentSubstitute( Const.NVL( field.getEndStringNotAllowed(), "" ) );
          data.regularExpression[ i ] = environmentSubstitute( Const.NVL( field.getRegularExpression(), "" ) );
          data.regularExpressionNotAllowed[ i ] =
            environmentSubstitute( Const.NVL( field.getRegularExpressionNotAllowed(), "" ) );

          IValueMeta stringMeta = cloneValueMeta( data.constantsMeta[ i ], IValueMeta.TYPE_STRING );
          data.minimumValue[ i ] =
            Utils.isEmpty( data.minimumValueAsString[ i ] ) ? null : data.constantsMeta[ i ].convertData(
              stringMeta, data.minimumValueAsString[ i ] );
          data.maximumValue[ i ] =
            Utils.isEmpty( data.maximumValueAsString[ i ] ) ? null : data.constantsMeta[ i ].convertData(
              stringMeta, data.maximumValueAsString[ i ] );

          try {
            data.fieldsMinimumLengthAsInt[ i ] = Integer.valueOf( Const.NVL( data.minimumLength[ i ], "-1" ) );
          } catch ( NumberFormatException nfe ) {
            throw new HopValueException(
              "Caught a number format exception converting minimum length with value "
                + data.minimumLength[ i ] + " to an int.", nfe );
          }

          try {
            data.fieldsMaximumLengthAsInt[ i ] = Integer.valueOf( Const.NVL( data.maximumLength[ i ], "-1" ) );
          } catch ( NumberFormatException nfe ) {
            throw new HopValueException(
              "Caught a number format exception converting minimum length with value "
                + data.maximumLength[ i ] + " to an int.", nfe );
          }

          int listSize = field.getAllowedValues() != null ? field.getAllowedValues().length : 0;
          data.listValues[ i ] = new Object[ listSize ];
          for ( int s = 0; s < listSize; s++ ) {
            data.listValues[ i ][ s ] =
              Utils.isEmpty( field.getAllowedValues()[ s ] ) ? null : data.constantsMeta[ i ].convertData(
                stringMeta, environmentSubstitute( field.getAllowedValues()[ s ] ) );
          }
        } catch ( HopException e ) {
          if ( field.getDataType() == IValueMeta.TYPE_NONE ) {
            logError( BaseMessages.getString( PKG, "Validator.Exception.SpecifyDataType" ), e );
          } else {
            logError( BaseMessages.getString( PKG, "Validator.Exception.DataConversionErrorEncountered" ), e );
          }
          return false;
        }

        if ( !Utils.isEmpty( data.regularExpression[ i ] ) ) {
          data.patternExpected[ i ] = Pattern.compile( data.regularExpression[ i ] );
        }
        if ( !Utils.isEmpty( data.regularExpressionNotAllowed[ i ] ) ) {
          data.patternDisallowed[ i ] = Pattern.compile( data.regularExpressionNotAllowed[ i ] );
        }

      }

      return true;
    }
    return false;
  }

  protected IValueMeta createValueMeta( String name, int type ) throws HopPluginException {
    return ValueMetaFactory.createValueMeta( name, type );
  }

  protected IValueMeta cloneValueMeta( IValueMeta valueMeta, int type ) throws HopPluginException {
    return ValueMetaFactory.cloneValueMeta( valueMeta, type );
  }

}
