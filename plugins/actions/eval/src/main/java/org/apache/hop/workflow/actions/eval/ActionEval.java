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

package org.apache.hop.workflow.actions.eval;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Scriptable;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Action type to evaluate the result of a previous action. It uses a piece of javascript to do this.
 *
 * @author Matt
 * @since 5-11-2003
 */

@Action(
  id = "EVAL",
  name = "i18n::ActionEval.Name",
  description = "i18n::ActionEval.Description",
  image = "eval.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/eval.html"
)
public class ActionEval extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionEval.class; // For Translator

  private String script;

  public ActionEval( String n, String scr ) {
    super( n, "" );
    script = scr;
  }

  public ActionEval() {
    this( "", "" );
  }

  public Object clone() {
    ActionEval je = (ActionEval) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "script", script ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      script = XmlHandler.getTagValue( entrynode, "script" );
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "ActionEval.UnableToLoadFromXml" ), e );
    }
  }

  public void setScript( String s ) {
    script = s;
  }

  public String getScript() {
    return script;
  }

  /**
   * Evaluate the result of the execution of previous action.
   *
   * @param result      The result to evaulate.
   * @param prevResult  the previous result
   * @param parentWorkflow   the parent workflow
   * @return The boolean result of the evaluation script.
   */
  public boolean evaluate( Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result prevResult ) {
    Context cx;
    Scriptable scope;

    cx = ContextFactory.getGlobal().enterContext();

    try {
      scope = cx.initStandardObjects( null );

      Long errors = new Long( result.getNrErrors() );
      Long linesInput = new Long( result.getNrLinesInput() );
      Long linesOutput = new Long( result.getNrLinesOutput() );
      Long linesUpdated = new Long( result.getNrLinesUpdated() );
      Long linesRejected = new Long( result.getNrLinesRejected() );
      Long linesRead = new Long( result.getNrLinesRead() );
      Long linesWritten = new Long( result.getNrLinesWritten() );
      Long exitStatus = new Long( result.getExitStatus() );
      Long filesRetrieved = new Long( result.getNrFilesRetrieved() );
      Long nr = new Long( result.getEntryNr() );

      scope.put( "errors", scope, errors );
      scope.put( "lines_input", scope, linesInput );
      scope.put( "lines_output", scope, linesOutput );
      scope.put( "lines_updated", scope, linesUpdated );
      scope.put( "lines_rejected", scope, linesRejected );
      scope.put( "lines_read", scope, linesRead );
      scope.put( "lines_written", scope, linesWritten );
      scope.put( "files_retrieved", scope, filesRetrieved );
      scope.put( "exit_status", scope, exitStatus );
      scope.put( "nr", scope, nr );
      scope.put( "is_windows", scope, Boolean.valueOf( Const.isWindows() ) );
      scope.put( "_entry_", scope, this ); // Compatible
      scope.put( "_action_", scope, this );
      scope.put( "action", scope, this ); // doc issue

      Object[] array = null;
      if ( result.getRows() != null ) {
        array = result.getRows().toArray();
      }

      scope.put( "rows", scope, array );
      scope.put( "parent_job", scope, parentWorkflow ); // migration
      scope.put( "parent_workflow", scope, parentWorkflow );
      scope.put( "previous_result", scope, prevResult );
      scope.put( "log", scope, getLogChannel() );

      try {
        Object res = cx.evaluateString( scope, this.script, "<cmd>", 1, null );
        boolean retval = Context.toBoolean( res );
        result.setNrErrors( 0 );

        return retval;
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "ActionEval.CouldNotCompile", e.toString() ) );
        return false;
      }
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "ActionEval.ErrorEvaluating", e.toString() ) );
      return false;
    } finally {
      Context.exit();
    }
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param prevResult The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute( Result prevResult, int nr ) {
    prevResult.setResult( evaluate( prevResult, parentWorkflow, prevResult ) );
    return prevResult;
  }

  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "script", remarks, AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
