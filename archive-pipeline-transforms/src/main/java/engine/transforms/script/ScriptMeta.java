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

package org.apache.hop.pipeline.transforms.script;

import org.apache.hop.compatibility.Value;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.plugins.HopURLClassLoader;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/*
 * Created on 2-jun-2003
 *
 */
public class ScriptMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = ScriptMeta.class; // for i18n purposes, needed by Translator!!

  private static final String JSSCRIPT_TAG_TYPE = "jsScript_type";
  private static final String JSSCRIPT_TAG_NAME = "jsScript_name";
  private static final String JSSCRIPT_TAG_SCRIPT = "jsScript_script";

  private ScriptAddClasses[] additionalClasses;
  private ScriptValuesScript[] jsScripts;

  private String[] fieldname;
  private String[] rename;
  private int[] type;
  private int[] length;
  private int[] precision;
  private boolean[] replace; // Replace the specified field.

  public ScriptMeta() {
    super(); // allocate BaseTransformMeta
    try {
      parseXmlForAdditionalClasses();
    } catch ( Exception e ) { /* Ignore */
    }
  }

  /**
   * @return Returns the length.
   */
  public int[] getLength() {
    return length;
  }

  /**
   * @param length The length to set.
   */
  public void setLength( int[] length ) {
    this.length = length;
  }

  /**
   * @return Returns the name.
   */
  public String[] getFieldname() {
    return fieldname;
  }

  /**
   * @param fieldname The name to set.
   */
  public void setFieldname( String[] fieldname ) {
    this.fieldname = fieldname;
  }

  /**
   * @return Returns the precision.
   */
  public int[] getPrecision() {
    return precision;
  }

  /**
   * @param precision The precision to set.
   */
  public void setPrecision( int[] precision ) {
    this.precision = precision;
  }

  /**
   * @return Returns the rename.
   */
  public String[] getRename() {
    return rename;
  }

  /**
   * @param rename The rename to set.
   */
  public void setRename( String[] rename ) {
    this.rename = rename;
  }

  /**
   * @return Returns the type.
   */
  public int[] getType() {
    return type;
  }

  /**
   * @param type The type to set.
   */
  public void setType( int[] type ) {
    this.type = type;
  }

  public int getNumberOfJSScripts() {
    return jsScripts.length;
  }

  public String[] getJSScriptNames() {
    String[] strJSNames = new String[ jsScripts.length ];
    for ( int i = 0; i < jsScripts.length; i++ ) {
      strJSNames[ i ] = jsScripts[ i ].getScriptName();
    }
    return strJSNames;
  }

  public ScriptValuesScript[] getJSScripts() {
    return jsScripts;
  }

  public void setJSScripts( ScriptValuesScript[] jsScripts ) {
    this.jsScripts = jsScripts;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocate( int nrFields ) {
    fieldname = new String[ nrFields ];
    rename = new String[ nrFields ];
    type = new int[ nrFields ];
    length = new int[ nrFields ];
    precision = new int[ nrFields ];
    replace = new boolean[ nrFields ];
  }

  public Object clone() {
    ScriptMeta retval = (ScriptMeta) super.clone();

    int nrFields = fieldname.length;

    retval.allocate( nrFields );

    System.arraycopy( fieldname, 0, retval.fieldname, 0, nrFields );
    System.arraycopy( rename, 0, retval.rename, 0, nrFields );
    System.arraycopy( type, 0, retval.type, 0, nrFields );
    System.arraycopy( length, 0, retval.length, 0, nrFields );
    System.arraycopy( precision, 0, retval.precision, 0, nrFields );
    System.arraycopy( replace, 0, retval.replace, 0, nrFields );

    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      Node scripts = XMLHandler.getSubNode( transformNode, "jsScripts" );
      int nrscripts = XMLHandler.countNodes( scripts, "jsScript" );
      jsScripts = new ScriptValuesScript[ nrscripts ];
      for ( int i = 0; i < nrscripts; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( scripts, "jsScript", i );

        jsScripts[ i ] =
          new ScriptValuesScript(
            Integer.parseInt( XMLHandler.getTagValue( fnode, JSSCRIPT_TAG_TYPE ) ), XMLHandler.getTagValue(
            fnode, JSSCRIPT_TAG_NAME ), XMLHandler.getTagValue( fnode, JSSCRIPT_TAG_SCRIPT ) );
      }

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        fieldname[ i ] = XMLHandler.getTagValue( fnode, "name" );
        rename[ i ] = XMLHandler.getTagValue( fnode, "rename" );
        type[ i ] = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "type" ) );

        String slen = XMLHandler.getTagValue( fnode, "length" );
        String sprc = XMLHandler.getTagValue( fnode, "precision" );
        length[ i ] = Const.toInt( slen, -1 );
        precision[ i ] = Const.toInt( sprc, -1 );
        replace[ i ] = "Y".equalsIgnoreCase( XMLHandler.getTagValue( fnode, "replace" ) );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "ScriptMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    jsScripts = new ScriptValuesScript[ 1 ];
    jsScripts[ 0 ] =
      new ScriptValuesScript( ScriptValuesScript.TRANSFORM_SCRIPT, BaseMessages
        .getString( PKG, "Script.Script1" ), "//"
        + BaseMessages.getString( PKG, "Script.ScriptHere" ) + Const.CR + Const.CR );

    int nrFields = 0;
    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      fieldname[ i ] = "newvalue";
      rename[ i ] = "newvalue";
      type[ i ] = ValueMetaInterface.TYPE_NUMBER;
      length[ i ] = -1;
      precision[ i ] = -1;
      replace[ i ] = false;
    }
  }

  public void getFields( RowMetaInterface row, String originTransformName, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    for ( int i = 0; i < fieldname.length; i++ ) {
      if ( !Utils.isEmpty( fieldname[ i ] ) ) {
        String fieldName;
        int replaceIndex;
        int fieldType;

        if ( replace[ i ] ) {
          // Look up the field to replace...
          //
          if ( row.searchValueMeta( fieldname[ i ] ) == null && Utils.isEmpty( rename[ i ] ) ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "ScriptMeta.Exception.FieldToReplaceNotFound", fieldname[ i ] ) );
          }
          replaceIndex = row.indexOfValue( rename[ i ] );

          // Change the data type to match what's specified...
          //
          fieldType = type[ i ];
          fieldName = rename[ i ];
        } else {
          replaceIndex = -1;
          fieldType = type[ i ];
          if ( rename[ i ] != null && rename[ i ].length() != 0 ) {
            fieldName = rename[ i ];
          } else {
            fieldName = fieldname[ i ];
          }
        }
        try {
          ValueMetaInterface v = ValueMetaFactory.createValueMeta( fieldName, fieldType );
          v.setLength( length[ i ] );
          v.setPrecision( precision[ i ] );
          v.setOrigin( originTransformName );
          if ( replace[ i ] && replaceIndex >= 0 ) {
            row.setValueMeta( replaceIndex, v );
          } else {
            row.addValueMeta( v );
          }
        } catch ( HopPluginException e ) {
          // Ignore errors
        }
      }
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    <jsScripts>" );
    for ( int i = 0; i < jsScripts.length; i++ ) {
      retval.append( "      <jsScript>" );
      retval
        .append( "        " ).append( XMLHandler.addTagValue( JSSCRIPT_TAG_TYPE, jsScripts[ i ].getScriptType() ) );
      retval
        .append( "        " ).append( XMLHandler.addTagValue( JSSCRIPT_TAG_NAME, jsScripts[ i ].getScriptName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( JSSCRIPT_TAG_SCRIPT, jsScripts[ i ].getScript() ) );
      retval.append( "      </jsScript>" );
    }
    retval.append( "    </jsScripts>" );

    retval.append( "    <fields>" );
    for ( int i = 0; i < fieldname.length; i++ ) {
      retval.append( "      <field>" );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", fieldname[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "rename", rename[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type",
        ValueMetaFactory.getValueMetaName( type[ i ] ) ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "length", length[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "precision", precision[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "replace", replace[ i ] ) );
      retval.append( "      </field>" );
    }
    retval.append( "    </fields>" );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    boolean error_found = false;
    String error_message = "";
    CheckResult cr;

    ScriptEngine jscx;
    Bindings jsscope;
    CompiledScript jsscript;

    jscx = createNewScriptEngine( transformMeta.getName() );
    jsscope = jscx.getBindings( ScriptContext.ENGINE_SCOPE );

    // String strActiveScriptName="";
    String strActiveStartScriptName = "";
    String strActiveEndScriptName = "";

    String strActiveScript = "";
    String strActiveStartScript = "";
    String strActiveEndScript = "";

    // Building the Scripts
    if ( jsScripts.length > 0 ) {
      for ( int i = 0; i < jsScripts.length; i++ ) {
        if ( jsScripts[ i ].isTransformScript() ) {
          // strActiveScriptName =jsScripts[i].getScriptName();
          strActiveScript = jsScripts[ i ].getScript();
        } else if ( jsScripts[ i ].isStartScript() ) {
          strActiveStartScriptName = jsScripts[ i ].getScriptName();
          strActiveStartScript = jsScripts[ i ].getScript();
        } else if ( jsScripts[ i ].isEndScript() ) {
          strActiveEndScriptName = jsScripts[ i ].getScriptName();
          strActiveEndScript = jsScripts[ i ].getScript();
        }
      }
    }

    if ( prev != null && strActiveScript.length() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ScriptMeta.CheckResult.ConnectedTransformOK", String.valueOf( prev.size() ) ), transformMeta );
      remarks.add( cr );

      // Adding the existing Scripts to the Context
      for ( int i = 0; i < getNumberOfJSScripts(); i++ ) {
        jsscope.put( jsScripts[ i ].getScriptName(), jsScripts[ i ].getScript() );
      }

      // Modification for Additional Script parsing
      try {
        if ( getAddClasses() != null ) {
          for ( int i = 0; i < getAddClasses().length; i++ ) {
            // TODO AKRETION ensure it works
            jsscope.put( getAddClasses()[ i ].getJSName(), getAddClasses()[ i ].getAddObject() );
            // Object jsOut = Context.javaToJS(getAddClasses()[i].getAddObject(), jsscope);
            // ScriptableObject.putProperty(jsscope, getAddClasses()[i].getJSName(), jsOut);
            // ScriptableObject.putProperty(jsscope, getAddClasses()[i].getJSName(), jsOut);
          }
        }
      } catch ( Exception e ) {
        error_message = ( "Couldn't add JavaClasses to Context! Error:" );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      }

      // Adding some default JavaScriptFunctions to the System
      // TODO AKRETION not implemented yet
      // try {
      // Context.javaToJS(ScriptValuesAddedFunctions.class, jsscope);
      // ((ScriptableObject)jsscope).defineFunctionProperties(ScriptValuesAddedFunctions.jsFunctionList,
      // ScriptValuesAddedFunctions.class, ScriptableObject.DONTENUM);
      // } catch (Exception ex) {
      // error_message="Couldn't add Default Functions! Error:"+Const.CR+ex.toString();
      // cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, transforminfo);
      // remarks.add(cr);
      // };

      // Adding some Constants to the JavaScript
      try {
        jsscope.put( "SKIP_PIPELINE", Integer.valueOf( Script.SKIP_PIPELINE ) );
        jsscope.put( "ABORT_PIPELINE", Integer.valueOf( Script.ABORT_PIPELINE ) );
        jsscope.put( "ERROR_PIPELINE", Integer.valueOf( Script.ERROR_PIPELINE ) );
        jsscope.put( "CONTINUE_PIPELINE", Integer.valueOf( Script.CONTINUE_PIPELINE ) );
      } catch ( Exception ex ) {
        error_message = "Couldn't add Pipeline Constants! Error:" + Const.CR + ex.toString();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      }

      try {
        ScriptDummy dummyTransform = new ScriptDummy( prev, pipelineMeta.getTransformFields( transformMeta ) );
        jsscope.put( "_transform_", dummyTransform );

        Object[] row = new Object[ prev.size() ];
        jsscope.put( "rowMeta", prev );
        for ( int i = 0; i < prev.size(); i++ ) {
          ValueMetaInterface valueMeta = prev.getValueMeta( i );
          Object valueData = null;

          // Set date and string values to something to simulate real thing
          //
          if ( valueMeta.isDate() ) {
            valueData = new Date();
          }
          if ( valueMeta.isString() ) {
            valueData = "test value test value test value test value test value "
              + "test value test value test value test value test value";
          }
          if ( valueMeta.isInteger() ) {
            valueData = Long.valueOf( 0L );
          }
          if ( valueMeta.isNumber() ) {
            valueData = new Double( 0.0 );
          }
          if ( valueMeta.isBigNumber() ) {
            valueData = BigDecimal.ZERO;
          }
          if ( valueMeta.isBoolean() ) {
            valueData = Boolean.TRUE;
          }
          if ( valueMeta.isBinary() ) {
            valueData = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, };
          }

          row[ i ] = valueData;

          jsscope.put( valueMeta.getName(), valueData );
        }
        // Add support for Value class (new Value())
        jsscope.put( "Value", Value.class );

        // Add the old style row object for compatibility reasons...
        //
        jsscope.put( "row", row );
      } catch ( Exception ev ) {
        error_message = "Couldn't add Input fields to Script! Error:" + Const.CR + ev.toString();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      }

      try {
        // Checking for StartScript
        if ( strActiveStartScript != null && strActiveStartScript.length() > 0 ) {
          jscx.eval( strActiveStartScript, jsscope );
          error_message = "Found Start Script. " + strActiveStartScriptName + " Processing OK";
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, transformMeta );
          remarks.add( cr );
        }
      } catch ( Exception e ) {
        error_message = "Couldn't process Start Script! Error:" + Const.CR + e.toString();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      }

      try {
        jsscript = ( (Compilable) jscx ).compile( strActiveScript );

        // cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
        // "ScriptMeta.CheckResult.ScriptCompiledOK"), transforminfo);
        // remarks.add(cr);

        try {

          jsscript.eval( jsscope );

          cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "ScriptMeta.CheckResult.ScriptCompiledOK2" ), transformMeta );
          remarks.add( cr );

          if ( fieldname.length > 0 ) {
            StringBuilder message =
              new StringBuilder( BaseMessages.getString( PKG, "ScriptMeta.CheckResult.FailedToGetValues", String
                .valueOf( fieldname.length ) )
                + Const.CR + Const.CR );

            if ( error_found ) {
              cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, message.toString(), transformMeta );
            } else {
              cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, message.toString(), transformMeta );
            }
            remarks.add( cr );
          }
        } catch ( ScriptException jse ) {
          // Context.exit(); TODO AKRETION NOT SURE
          error_message =
            BaseMessages.getString( PKG, "ScriptMeta.CheckResult.CouldNotExecuteScript" )
              + Const.CR + jse.toString();
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
          remarks.add( cr );
        } catch ( Exception e ) {
          // Context.exit(); TODO AKRETION NOT SURE
          error_message =
            BaseMessages.getString( PKG, "ScriptMeta.CheckResult.CouldNotExecuteScript2" )
              + Const.CR + e.toString();
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
          remarks.add( cr );
        }

        // Checking End Script
        try {
          if ( strActiveEndScript != null && strActiveEndScript.length() > 0 ) {
            /* Object endScript = */
            jscx.eval( strActiveEndScript, jsscope );
            error_message = "Found End Script. " + strActiveEndScriptName + " Processing OK";
            cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, transformMeta );
            remarks.add( cr );
          }
        } catch ( Exception e ) {
          error_message = "Couldn't process End Script! Error:" + Const.CR + e.toString();
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
          remarks.add( cr );
        }
      } catch ( Exception e ) {
        // Context.exit(); TODO AKRETION NOT SURE
        error_message =
          BaseMessages.getString( PKG, "ScriptMeta.CheckResult.CouldNotCompileScript" )
            + Const.CR + e.toString();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      }
    } else {
      // Context.exit(); TODO AKRETION NOT SURE
      error_message = BaseMessages.getString( PKG, "ScriptMeta.CheckResult.CouldNotGetFieldsFromPreviousTransform" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ScriptMeta.CheckResult.ConnectedTransformOK2" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ScriptMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public String getFunctionFromScript( String strFunction, String strScript ) {
    String sRC = "";
    int iStartPos = strScript.indexOf( strFunction );
    if ( iStartPos > 0 ) {
      iStartPos = strScript.indexOf( '{', iStartPos );
      int iCounter = 1;
      while ( iCounter != 0 ) {
        if ( strScript.charAt( iStartPos++ ) == '{' ) {
          iCounter++;
        } else if ( strScript.charAt( iStartPos++ ) == '}' ) {
          iCounter--;
        }
        sRC = sRC + strScript.charAt( iStartPos );
      }
    }
    return sRC;
  }

  public boolean getValue( Bindings scope, int i, Value res, StringBuilder message ) {
    boolean error_found = false;

    if ( fieldname[ i ] != null && fieldname[ i ].length() > 0 ) {
      res.setName( rename[ i ] );
      res.setType( type[ i ] );

      try {

        Object result = scope.get( fieldname[ i ] );
        if ( result != null ) {

          String classname = result.getClass().getName();

          switch ( type[ i ] ) {
            case ValueMetaInterface.TYPE_NUMBER:
              if ( classname.equalsIgnoreCase( "org.mozilla.javascript.Undefined" ) ) {
                res.setNull();
              } else if ( classname.equalsIgnoreCase( "org.mozilla.javascript.NativeJavaObject" ) ) {
                // Is it a java Value class ?
                Value v = (Value) result;
                res.setValue( v.getNumber() );
              } else {
                res.setValue( ( (Double) result ).doubleValue() );
              }
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              if ( classname.equalsIgnoreCase( "java.lang.Byte" ) ) {
                res.setValue( ( (java.lang.Byte) result ).longValue() );
              } else if ( classname.equalsIgnoreCase( "java.lang.Short" ) ) {
                res.setValue( ( (Short) result ).longValue() );
              } else if ( classname.equalsIgnoreCase( "java.lang.Integer" ) ) {
                res.setValue( ( (Integer) result ).longValue() );
              } else if ( classname.equalsIgnoreCase( "java.lang.Long" ) ) {
                res.setValue( ( (Long) result ).longValue() );
              } else if ( classname.equalsIgnoreCase( "org.mozilla.javascript.Undefined" ) ) {
                res.setNull();
              } else if ( classname.equalsIgnoreCase( "org.mozilla.javascript.NativeJavaObject" ) ) {
                // Is it a java Value class ?
                Value v = (Value) result;
                res.setValue( v.getInteger() );
              } else {
                res.setValue( Math.round( ( (Double) result ).doubleValue() ) );
              }
              break;
            case ValueMetaInterface.TYPE_STRING:
              if ( classname.equalsIgnoreCase( "org.mozilla.javascript.NativeJavaObject" )
                || classname.equalsIgnoreCase( "org.mozilla.javascript.Undefined" ) ) {
                // Is it a java Value class ?
                try {
                  Value v = (Value) result;
                  res.setValue( v.getString() );
                } catch ( Exception ev ) {
                  // A String perhaps?
                  String s = (String) result;
                  res.setValue( s );
                }
              } else {
                res.setValue( ( (String) result ) );
              }
              break;
            case ValueMetaInterface.TYPE_DATE:
              double dbl = 0;
              if ( classname.equalsIgnoreCase( "org.mozilla.javascript.Undefined" ) ) {
                res.setNull();
              } else {
                if ( classname.equalsIgnoreCase( "org.mozilla.javascript.NativeDate" ) ) {
                  dbl = (Double) result; // TODO AKRETION not sure!
                } else if ( classname.equalsIgnoreCase( "org.mozilla.javascript.NativeJavaObject" ) ) {
                  // Is it a java Date() class ?
                  try {
                    Date dat = (Date) result;
                    dbl = dat.getTime();
                  } catch ( Exception e ) { // Nope, try a Value

                    Value v = (Value) result;
                    Date dat = v.getDate();
                    if ( dat != null ) {
                      dbl = dat.getTime();
                    } else {
                      res.setNull();
                    }
                  }
                } else { // Finally, try a number conversion to time

                  dbl = ( (Double) result ).doubleValue();
                }
                long lng = Math.round( dbl );
                Date dat = new Date( lng );
                res.setValue( dat );
              }
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              res.setValue( ( (Boolean) result ).booleanValue() );
              break;
            default:
              res.setNull();
          }
        } else {
          res.setNull();
        }
      } catch ( Exception e ) {
        message.append( BaseMessages.getString( PKG, "ScriptMeta.CheckResult.ErrorRetrievingValue", fieldname[ i ] )
          + " : " + e.toString() );
        error_found = true;
      }
      res.setLength( length[ i ], precision[ i ] );

      message.append( BaseMessages.getString( PKG, "ScriptMeta.CheckResult.RetrievedValue", fieldname[ i ], res
        .toStringMeta() ) );
    } else {
      message.append( BaseMessages.getString( PKG, "ScriptMeta.CheckResult.ValueIsEmpty", String.valueOf( i ) ) );
      error_found = true;
    }

    return error_found;
  }

  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Script( transformMeta, transformDataInterface, cnr, pipelineMeta, pipeline );
  }

  public TransformDataInterface getTransformData() {
    return new ScriptData();
  }

  // This is for Additional Classloading
  public void parseXmlForAdditionalClasses() throws HopException {
    try {
      Properties sysprops = System.getProperties();
      String strActPath = sysprops.getProperty( "user.dir" );
      Document dom = XMLHandler.loadXMLFile( strActPath + "/plugins/transforms/ScriptValues_mod/plugin.xml" );
      Node transformNode = dom.getDocumentElement();
      Node libraries = XMLHandler.getSubNode( transformNode, "js_libraries" );
      int nbOfLibs = XMLHandler.countNodes( libraries, "js_lib" );
      additionalClasses = new ScriptAddClasses[ nbOfLibs ];
      for ( int i = 0; i < nbOfLibs; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( libraries, "js_lib", i );
        String strJarName = XMLHandler.getTagAttribute( fnode, "name" );
        String strClassName = XMLHandler.getTagAttribute( fnode, "classname" );
        String strJSName = XMLHandler.getTagAttribute( fnode, "js_name" );

        Class<?> addClass =
          LoadAdditionalClass( strActPath + "/plugins/transforms/ScriptValues_mod/" + strJarName, strClassName );
        Object addObject = addClass.newInstance();
        additionalClasses[ i ] = new ScriptAddClasses( addClass, addObject, strJSName );
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "ScriptMeta.Exception.UnableToParseXMLforAdditionalClasses" ), e );
    }
  }

  private static Class<?> LoadAdditionalClass( String strJar, String strClassName ) throws HopException {
    try {
      Thread t = Thread.currentThread();
      ClassLoader cl = t.getContextClassLoader();
      URL u = new URL( "jar:file:" + strJar + "!/" );
      // We never know what else the script wants to load with the class loader, so lets not close it just like that.
      @SuppressWarnings( "resource" )
      HopURLClassLoader kl = new HopURLClassLoader( new URL[] { u }, cl );
      Class<?> toRun = kl.loadClass( strClassName );
      return toRun;
    } catch ( Exception e ) {
      throw new HopException(
        BaseMessages.getString( PKG, "ScriptMeta.Exception.UnableToLoadAdditionalClass" ), e );
    }
  }

  public ScriptAddClasses[] getAddClasses() {
    return additionalClasses;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * @return the replace
   */
  public boolean[] getReplace() {
    return replace;
  }

  /**
   * @param replace the replace to set
   */
  public void setReplace( boolean[] replace ) {
    this.replace = replace;
  }

  /**
   * Instanciates the right scripting language interpreter, falling back to Javascript for backward compat. Because
   * Hop GUI sucks for extensibility, we use the script name extension to determine the language rather than add a
   * Combo box. Complain to Pentaho please.
   *
   * @param transformName
   * @return
   */
  public static ScriptEngine createNewScriptEngine( String transformName ) {
    System.setProperty( "org.jruby.embed.localvariable.behavior", "persistent" ); // required for JRuby, transparent for
    // others
    if ( Thread.currentThread().getContextClassLoader() == null ) {
      Thread.currentThread().setContextClassLoader( ScriptMeta.class.getClassLoader() );
    }
    ScriptEngineManager manager = new ScriptEngineManager();
    String[] strings = transformName.split( "\\." );
    String extension = strings[ strings.length > 0 ? 1 : 0 ]; // skip the script number extension
    ScriptEngine scriptEngine = manager.getEngineByName( extension );
    if ( scriptEngine == null ) { // falls back to Javascript
      scriptEngine = manager.getEngineByName( "javascript" );
    }
    return scriptEngine;
  }

}
