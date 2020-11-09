/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.script;

import org.apache.hop.compatibility.Value;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.JavaScriptUtils;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;

/**
 * Executes a JavaScript on the values in the input stream. Selected calculated values can then be put on the output
 * stream.
 *
 * @author Matt
 * @since 5-April-2003
 */
public class Script extends BaseTransform implements ITransform {
  private static final Class<?> PKG = ScriptMeta.class; // Needed by Translator

  private ScriptMeta meta;

  private ScriptData data;

  public static final int SKIP_PIPELINE = 1;

  public static final int ABORT_PIPELINE = -1;

  public static final int ERROR_PIPELINE = -2;

  public static final int CONTINUE_PIPELINE = 0;

  private boolean bWithPipelineStat = false;

  private boolean bRC = false;

  private int iPipelineStat = CONTINUE_PIPELINE;

  private boolean bFirstRun = false;

  private ScriptValuesScript[] jsScripts;

  private String strTransformScript = "";

  private String strStartScript = "";

  private String strEndScript = "";

  // public static Row insertRow;

  // public String script; //TODO AKRETION should be compiled script actually

  public Script( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private void determineUsedFields( IRowMeta row ) {
    int nr = 0;
    // Count the occurrences of the values.
    // Perhaps we find values in comments, but we take no risk!
    //
    for ( int i = 0; i < row.size(); i++ ) {
      String valname = row.getValueMeta( i ).getName().toUpperCase();
      if ( strTransformScript.toUpperCase().indexOf( valname ) >= 0 ) {
        nr++;
      }
    }

    // Allocate fields_used
    data.fieldsUsed = new int[ nr ];
    data.valuesUsed = new Value[ nr ];

    nr = 0;
    // Count the occurrences of the values.
    // Perhaps we find values in comments, but we take no risk!
    //
    for ( int i = 0; i < row.size(); i++ ) {
      // Values are case-insensitive in JavaScript.
      //
      String valname = row.getValueMeta( i ).getName();
      if ( strTransformScript.indexOf( valname ) >= 0 ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "Script.Log.UsedValueName", String.valueOf( i ), valname ) );
        }
        data.fieldsUsed[ nr ] = i;
        nr++;
      }
    }

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "Script.Log.UsingValuesFromInputStream", String
        .valueOf( data.fieldsUsed.length ) ) );
    }
  }

  private boolean addValues( IRowMeta rowMeta, Object[] row ) throws HopException {
    if ( first ) {
      first = false;

      // What is the output row looking like?
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Determine the indexes of the fields used!
      //
      determineUsedFields( rowMeta );

      // Get the indexes of the replaced fields...
      //
      data.replaceIndex = new int[ meta.getFieldname().length ];
      for ( int i = 0; i < meta.getFieldname().length; i++ ) {
        if ( meta.getReplace()[ i ] ) {
          data.replaceIndex[ i ] = rowMeta.indexOfValue( meta.getFieldname()[ i ] );
          if ( data.replaceIndex[ i ] < 0 ) {
            if ( Utils.isEmpty( meta.getFieldname()[ i ] ) ) {
              throw new HopTransformException( BaseMessages.getString(
                PKG, "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound", meta.getFieldname()[ i ] ) );
            }
            data.replaceIndex[ i ] = rowMeta.indexOfValue( meta.getRename()[ i ] );
            if ( data.replaceIndex[ i ] < 0 ) {
              throw new HopTransformException( BaseMessages.getString(
                PKG, "ScriptValuesMetaMod.Exception.FieldToReplaceNotFound", meta.getRename()[ i ] ) );
            }
          }
        } else {
          data.replaceIndex[ i ] = -1;
        }
      }

      data.cx = ScriptMeta.createNewScriptEngine( getTransformName() );
      data.scope = data.cx.getBindings( ScriptContext.ENGINE_SCOPE );

      bFirstRun = true;

      data.scope.put( "_transform_", this );

      // Adding the existing Scripts to the Context
      //
      for ( int i = 0; i < meta.getNumberOfJSScripts(); i++ ) {
        data.scope.put( jsScripts[ i ].getScriptName(), jsScripts[ i ].getScript() );
      }

      // Adding the Name of the Pipeline to the Context
      //
      data.scope.put( "_PipelineName_", this.getTransformName() );

      try {
        // add these now (they will be re-added later) to make
        // compilation succeed
        //

        // Add the old style row object for compatibility reasons...
        //
        data.scope.put( "row", row );

        // Add the used fields...
        //
        for ( int i = 0; i < data.fieldsUsed.length; i++ ) {
          IValueMeta valueMeta = rowMeta.getValueMeta( data.fieldsUsed[ i ] );
          Object valueData = row[ data.fieldsUsed[ i ] ];

          Object normalStorageValueData = valueMeta.convertToNormalStorageType( valueData );
          data.scope.put( valueMeta.getName(), normalStorageValueData );
        }

        // also add the meta information for the whole row
        //
        data.scope.put( "rowMeta", rowMeta );

        // Modification for Additional Script parsing
        //
        try {
          if ( meta.getAddClasses() != null ) {
            for ( int i = 0; i < meta.getAddClasses().length; i++ ) {
              // TODO AKRETION ensure it works
              data.scope.put( meta.getAddClasses()[ i ].getJSName(), meta.getAddClasses()[ i ].getAddObject() );
              // Object jsOut =
              // Context.javaToJS(meta.getAddClasses()[i].getAddObject(),
              // data.scope);
              // ScriptableObject.putProperty(data.scope,
              // meta.getAddClasses()[i].getJSName(), jsOut);
            }
          }
        } catch ( Exception e ) {
          throw new HopValueException( BaseMessages.getString(
            PKG, "Script.Log.CouldNotAttachAdditionalScripts" ), e );
        }

        // Adding some default JavaScriptFunctions to the System
        // TODO AKRETION not implemented yet
        // try {
        // Context.javaToJS(ScriptValuesAddedFunctions.class,
        // data.scope);
        // ((ScriptableObject)
        // data.scope).defineFunctionProperties(ScriptValuesAddedFunctions.jsFunctionList,
        // ScriptValuesAddedFunctions.class, ScriptableObject.DONTENUM);
        // } catch (Exception ex) {
        // // System.out.println(ex.toString());
        // throw new HopValueException(BaseMessages.getString(PKG, "Script.Log.CouldNotAddDefaultFunctions"), ex);
        // }
        // ;

        // Adding some Constants to the JavaScript
        try {

          data.scope.put( "SKIP_PIPELINE", Integer.valueOf( SKIP_PIPELINE ) );
          data.scope.put( "ABORT_PIPELINE", Integer.valueOf( ABORT_PIPELINE ) );
          data.scope.put( "ERROR_PIPELINE", Integer.valueOf( ERROR_PIPELINE ) );
          data.scope.put( "CONTINUE_PIPELINE", Integer.valueOf( CONTINUE_PIPELINE ) );

        } catch ( Exception ex ) {
          throw new HopValueException( BaseMessages.getString( PKG, "Script.Log.CouldNotAddDefaultConstants" ), ex );
        }

        try {
          // Checking for StartScript
          if ( strStartScript != null && strStartScript.length() > 0 ) {
            CompiledScript startScript = ( (Compilable) data.cx ).compile( strStartScript );
            startScript.eval( data.scope );
            if ( log.isDetailed() ) {
              logDetailed( ( "Start Script found!" ) );
            }
          } else {
            if ( log.isDetailed() ) {
              logDetailed( ( "No starting Script found!" ) );
            }
          }
        } catch ( Exception es ) {
          throw new HopValueException( BaseMessages.getString( PKG, "Script.Log.ErrorProcessingStartScript" ), es );

        }
        // Now Compile our Script
        // alternatively you could also support non compilable JSR223
        // languages, see how we were doing before:
        // http://github.com/rvalyi/jripple/blob/e6190fd89014a49b0faffae68c75762be124d899/
        // src/org.apache.hop/pipeline/transforms/scriptvalues_mod/ScriptValuesMod.java

        data.script = ( (Compilable) data.cx ).compile( strTransformScript );
      } catch ( Exception e ) {
        throw new HopValueException( BaseMessages.getString( PKG, "Script.Log.CouldNotCompileJavascript" ), e );
      }
    }

    // Filling the defined TranVars with the Values from the Row
    //
    Object[] outputRow = RowDataUtil.resizeArray( row, data.outputRowMeta.size() );

    // Keep an index...
    int outputIndex = rowMeta.size();

    try {
      try {
        data.scope.put( "row", row );

        for ( int i = 0; i < data.fieldsUsed.length; i++ ) {
          IValueMeta valueMeta = rowMeta.getValueMeta( data.fieldsUsed[ i ] );
          Object valueData = row[ data.fieldsUsed[ i ] ];

          Object normalStorageValueData = valueMeta.convertToNormalStorageType( valueData );
          data.scope.put( valueMeta.getName(), normalStorageValueData );
        }

        // also add the meta information for the hole row
        //
        data.scope.put( "rowMeta", rowMeta );
      } catch ( Exception e ) {
        throw new HopValueException( BaseMessages.getString( PKG, "Script.Log.UnexpectedeError" ), e );
      }

      data.script.eval( data.scope );

      if ( bFirstRun ) {
        bFirstRun = false;
        // Check if we had a Pipeline Status
        Object pipelineStatus = data.scope.get( "pipeline_Status" );
        if ( pipelineStatus != null ) { // TODO AKRETION not sure: !=
          // ScriptableObject.NOT_FOUND
          bWithPipelineStat = true;
          if ( log.isDetailed() ) {
            logDetailed( ( "tran_Status found. Checking pipeline status while script execution." ) );
          }
        } else {
          if ( log.isDetailed() ) {
            logDetailed( ( "No tran_Status found. Pipeline status checking not available." ) );
          }
          bWithPipelineStat = false;
        }
      }

      if ( bWithPipelineStat ) {
        iPipelineStat = (Integer) data.scope.get( "pipeline_Status" ); // TODO
        // ARETION
        // not
        // sure
        // the
        // casting
        // is
        // correct
      } else {
        iPipelineStat = CONTINUE_PIPELINE;
      }

      if ( iPipelineStat == CONTINUE_PIPELINE ) {
        bRC = true;
        for ( int i = 0; i < meta.getFieldname().length; i++ ) {
          Object result = data.scope.get( meta.getFieldname()[ i ] );
          Object valueData = getValueFromJScript( result, i );
          if ( data.replaceIndex[ i ] < 0 ) {
            outputRow[ outputIndex++ ] = valueData;
          } else {
            outputRow[ data.replaceIndex[ i ] ] = valueData;
          }
        }

        putRow( data.outputRowMeta, outputRow );
      } else {
        switch ( iPipelineStat ) {
          case SKIP_PIPELINE:
            // eat this row.
            bRC = true;
            break;
          case ABORT_PIPELINE:
            if ( data.cx != null ) {
              // Context.exit(); TODO AKRETION not sure
              stopAll();
            }
            setOutputDone();
            bRC = false;
            break;
          case ERROR_PIPELINE:
            if ( data.cx != null ) {
              // Context.exit(); TODO AKRETION not sure
              setErrors( 1 );
            }
            stopAll();
            bRC = false;
            break;
          default:
            break;
        }

        // TODO: kick this "ERROR handling" junk out now that we have
        // solid error handling in place.
        //
      }
    } catch ( ScriptException e ) {
      throw new HopValueException( BaseMessages.getString( PKG, "Script.Log.JavascriptError" ), e );
    }
    return bRC;
  }

  public Object getValueFromJScript( Object result, int i ) throws HopValueException {
    String fieldName = meta.getFieldname()[ i ];
    if ( !Utils.isEmpty( fieldName ) ) {
      // res.setName(meta.getRename()[i]);
      // res.setType(meta.getType()[i]);

      try {
        return ( result == null ) ? null
          : JavaScriptUtils.convertFromJs( result, meta.getType()[ i ], fieldName );
      } catch ( Exception e ) {
        throw new HopValueException( BaseMessages.getString( PKG, "Script.Log.JavascriptError" ), e );
      }
    } else {
      throw new HopValueException( "No name was specified for result value #" + ( i + 1 ) );
    }
  }

  public IRowMeta getOutputRowMeta() {
    return data.outputRowMeta;
  }

  public boolean processRow() throws HopException {

    meta = (ScriptMeta) smi;
    data = (ScriptData) sdi;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) {
      // Modification for Additional End Function
      try {
        if ( data.cx != null ) {
          // Checking for EndScript
          if ( strEndScript != null && strEndScript.length() > 0 ) {
            // Script endScript =
            // data.cx.compileString(strEndScript, "pipeline_End", 1,
            // null);
            // endScript.exec(data.cx, data.scope);
            data.cx.eval( strEndScript, data.scope );
            if ( log.isDetailed() ) {
              logDetailed( ( "End Script found!" ) );
            }
          } else {
            if ( log.isDetailed() ) {
              logDetailed( ( "No end Script found!" ) );
            }
          }
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "Script.Log.UnexpectedeError" ) + " : " + e.toString() );
        logError( BaseMessages.getString( PKG, "Script.Log.ErrorStackTrace" )
          + Const.CR + Const.getStackTracker( e ) );
        setErrors( 1 );
        stopAll();
      }

      setOutputDone();
      return false;
    }

    // Getting the Row, with the Pipeline Status
    try {
      addValues( getInputRowMeta(), r );
    } catch ( HopValueException e ) {
      String location = null;
      if ( e.getCause() instanceof ScriptException ) {
        ScriptException ee = (ScriptException) e.getCause();
        location = "--> " + ee.getLineNumber() + ":" + ee.getColumnNumber();
        //
      }

      if ( getTransformMeta().isDoingErrorHandling() ) {
        putError( getInputRowMeta(), r, 1, e.getMessage() + Const.CR + location, null, "SCR-001" );
        bRC = true; // continue by all means, even on the first row and
        // out of this ugly design
      } else {
        throw ( e );
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "Script.Log.LineNumber" ) + getLinesRead() );
    }
    return bRC;
  }

  public boolean init() {
    meta = (ScriptMeta) smi;
    data = (ScriptData) sdi;

    if ( super.init() ) {

      // Add init code here.
      // Get the actual Scripts from our MetaData
      jsScripts = meta.getJSScripts();
      for ( int j = 0; j < jsScripts.length; j++ ) {
        switch ( jsScripts[ j ].getScriptType() ) {
          case ScriptValuesScript.TRANSFORM_SCRIPT:
            strTransformScript = jsScripts[ j ].getScript();
            break;
          case ScriptValuesScript.START_SCRIPT:
            strStartScript = jsScripts[ j ].getScript();
            break;
          case ScriptValuesScript.END_SCRIPT:
            strEndScript = jsScripts[ j ].getScript();
            break;
          default:
            break;
        }
      }
      return true;
    }
    return false;
  }

  public void.dispose() {
    try {
      if ( data.cx != null ) {
        return;
        // Context.exit(); TODO AKRETION not sure
      }
    } catch ( Exception er ) {
      // Eat this error, it's typically :
      // "Calling Context.exit without previous Context.enter"
      // logError(BaseMessages.getString(PKG, "System.Log.UnexpectedError"), er);
    }

    super.dispose();
  }

}
