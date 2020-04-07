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

package org.apache.hop.workflow.actions.eval;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
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
  i18nPackageName = "org.apache.hop.workflow.actions.eval",
  name = "ActionEval.Name",
  description = "ActionEval.Description",
  image = "Eval.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting"
)
public class ActionEval extends ActionBase implements Cloneable, IAction {
  private static Class<?> PKG = ActionEval.class; // for i18n purposes, needed by Translator!!

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
      throw new HopXMLException( BaseMessages.getString( PKG, "ActionEval.UnableToLoadFromXml" ), e );
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
   * @param prev_result the previous result
   * @param parentWorkflow   the parent workflow
   * @return The boolean result of the evaluation script.
   */
  public boolean evaluate( Result result, Workflow parentWorkflow, Result prev_result ) {
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
      scope.put( "parent_job", scope, parentWorkflow );
      scope.put( "previous_result", scope, prev_result );

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
   * @param prev_result The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute( Result prev_result, int nr ) {
    prev_result.setResult( evaluate( prev_result, parentWorkflow, prev_result ) );
    return prev_result;
  }

  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IMetaStore metaStore ) {
    ActionValidatorUtils.andValidator().validate( this, "script", remarks, AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
