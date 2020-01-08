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

package org.apache.hop.job.entries.eval;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Scriptable;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Job entry type to evaluate the result of a previous job entry. It uses a piece of javascript to do this.
 *
 * @author Matt
 * @since 5-11-2003
 */
public class JobEntryEval extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryEval.class; // for i18n purposes, needed by Translator2!!

  private String script;

  public JobEntryEval( String n, String scr ) {
    super( n, "" );
    script = scr;
  }

  public JobEntryEval() {
    this( "", "" );
  }

  public Object clone() {
    JobEntryEval je = (JobEntryEval) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "script", script ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      script = XMLHandler.getTagValue( entrynode, "script" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "JobEntryEval.UnableToLoadFromXml" ), e );
    }
  }

  public void setScript( String s ) {
    script = s;
  }

  public String getScript() {
    return script;
  }

  /**
   * Evaluate the result of the execution of previous job entry.
   *
   * @param result      The result to evaulate.
   * @param prev_result the previous result
   * @param parentJob   the parent job
   * @return The boolean result of the evaluation script.
   */
  public boolean evaluate( Result result, Job parentJob, Result prev_result ) {
    Context cx;
    Scriptable scope;

    cx = ContextFactory.getGlobal().enterContext();

    try {
      scope = cx.initStandardObjects( null );

      Long errors = new Long( result.getNrErrors() );
      Long lines_input = new Long( result.getNrLinesInput() );
      Long lines_output = new Long( result.getNrLinesOutput() );
      Long lines_updated = new Long( result.getNrLinesUpdated() );
      Long lines_rejected = new Long( result.getNrLinesRejected() );
      Long lines_read = new Long( result.getNrLinesRead() );
      Long lines_written = new Long( result.getNrLinesWritten() );
      Long exit_status = new Long( result.getExitStatus() );
      Long files_retrieved = new Long( result.getNrFilesRetrieved() );
      Long nr = new Long( result.getEntryNr() );

      scope.put( "errors", scope, errors );
      scope.put( "lines_input", scope, lines_input );
      scope.put( "lines_output", scope, lines_output );
      scope.put( "lines_updated", scope, lines_updated );
      scope.put( "lines_rejected", scope, lines_rejected );
      scope.put( "lines_read", scope, lines_read );
      scope.put( "lines_written", scope, lines_written );
      scope.put( "files_retrieved", scope, files_retrieved );
      scope.put( "exit_status", scope, exit_status );
      scope.put( "nr", scope, nr );
      scope.put( "is_windows", scope, Boolean.valueOf( Const.isWindows() ) );
      scope.put( "_entry_", scope, this );

      Object[] array = null;
      if ( result.getRows() != null ) {
        array = result.getRows().toArray();
      }

      scope.put( "rows", scope, array );
      scope.put( "parent_job", scope, parentJob );
      scope.put( "previous_result", scope, prev_result );

      try {
        Object res = cx.evaluateString( scope, this.script, "<cmd>", 1, null );
        boolean retval = Context.toBoolean( res );
        // System.out.println(result.toString()+" + ["+this.script+"] --> "+retval);
        result.setNrErrors( 0 );

        return retval;
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobEntryEval.CouldNotCompile", e.toString() ) );
        return false;
      }
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobEntryEval.ErrorEvaluating", e.toString() ) );
      return false;
    } finally {
      Context.exit();
    }
  }

  /**
   * Execute this job entry and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param prev_result The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute( Result prev_result, int nr ) {
    prev_result.setResult( evaluate( prev_result, parentJob, prev_result ) );
    return prev_result;
  }

  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous jobentry.
    return false;
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "script", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }

}
