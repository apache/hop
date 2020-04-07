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

package org.apache.hop.workflow.actions.waitforsql;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * This defines a Wait for SQL data action
 *
 * @author Samatar
 * @since 22-07-2008
 */

@Action(
  id = "WAIT_FOR_SQL",
  i18nPackageName = "org.apache.hop.workflow.actions.waitforsql",
  name = "ActionWaitForSQL.Name",
  description = "ActionWaitForSQL.Description",
  image = "WaitForSQL.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility"
)
public class ActionWaitForSQL extends ActionBase implements Cloneable, IAction {
  private static Class<?> PKG = ActionWaitForSQL.class; // for i18n purposes, needed by Translator!!

  public boolean isClearResultList;

  public boolean isAddRowsResult;

  public boolean isUseVars;

  public boolean iscustomSQL;

  public String customSQL;

  private DatabaseMeta connection;

  public String tablename;

  public String schemaname;

  private String maximumTimeout; // maximum timeout in seconds
  private String checkCycleTime; // cycle time in seconds
  private boolean successOnTimeout;

  private static final String selectCount = "SELECT count(*) FROM ";

  public static final String[] successConditionsDesc = new String[] {
    BaseMessages.getString( PKG, "ActionWaitForSQL.SuccessWhenRowCountEqual.Label" ),
    BaseMessages.getString( PKG, "ActionWaitForSQL.SuccessWhenRowCountDifferent.Label" ),
    BaseMessages.getString( PKG, "ActionWaitForSQL.SuccessWhenRowCountSmallerThan.Label" ),
    BaseMessages.getString( PKG, "ActionWaitForSQL.SuccessWhenRowCountSmallerOrEqualThan.Label" ),
    BaseMessages.getString( PKG, "ActionWaitForSQL.SuccessWhenRowCountGreaterThan.Label" ),
    BaseMessages.getString( PKG, "ActionWaitForSQL.SuccessWhenRowCountGreaterOrEqual.Label" )

  };
  public static final String[] successConditionsCode = new String[] {
    "rows_count_equal", "rows_count_different", "rows_count_smaller", "rows_count_smaller_equal",
    "rows_count_greater", "rows_count_greater_equal" };

  public static final int SUCCESS_CONDITION_ROWS_COUNT_EQUAL = 0;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_DIFFERENT = 1;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_SMALLER = 2;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_SMALLER_EQUAL = 3;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_GREATER = 4;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_GREATER_EQUAL = 5;

  public String rowsCountValue;
  public int successCondition;
  private static String DEFAULT_MAXIMUM_TIMEOUT = "0"; // infinite timeout
  private static String DEFAULT_CHECK_CYCLE_TIME = "60"; // 1 minute

  public ActionWaitForSQL( String n ) {
    super( n, "" );
    isClearResultList = true;
    rowsCountValue = "0";
    successCondition = SUCCESS_CONDITION_ROWS_COUNT_GREATER;
    iscustomSQL = false;
    isUseVars = false;
    isAddRowsResult = false;
    customSQL = null;
    schemaname = null;
    tablename = null;
    connection = null;
    maximumTimeout = DEFAULT_MAXIMUM_TIMEOUT;
    checkCycleTime = DEFAULT_CHECK_CYCLE_TIME;
    successOnTimeout = false;
  }

  public ActionWaitForSQL() {
    this( "" );
  }

  @Override
  public Object clone() {
    ActionWaitForSQL je = (ActionWaitForSQL) super.clone();
    return je;
  }

  public int getSuccessCondition() {
    return successCondition;
  }

  public static int getSuccessConditionByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < successConditionsDesc.length; i++ ) {
      if ( successConditionsDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getSuccessConditionByCode( tt );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXML() );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "tablename", tablename ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "success_condition", getSuccessConditionCode( successCondition ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "rows_count_value", rowsCountValue ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "is_custom_sql", iscustomSQL ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "is_usevars", isUseVars ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "custom_sql", customSQL ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_rows_result", isAddRowsResult ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "maximum_timeout", maximumTimeout ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "check_cycle_time", checkCycleTime ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "success_on_timeout", successOnTimeout ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "clear_result_rows", isClearResultList ) );
    return retval.toString();
  }

  private static String getSuccessConditionCode( int i ) {
    if ( i < 0 || i >= successConditionsCode.length ) {
      return successConditionsCode[ 0 ];
    }
    return successConditionsCode[ i ];
  }

  private static int getSucessConditionByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < successConditionsCode.length; i++ ) {
      if ( successConditionsCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static String getSuccessConditionDesc( int i ) {
    if ( i < 0 || i >= successConditionsDesc.length ) {
      return successConditionsDesc[ 0 ];
    }
    return successConditionsDesc[ i ];
  }

  public boolean isSuccessOnTimeout() {
    return successOnTimeout;
  }

  public void setSuccessOnTimeout( boolean successOnTimeout ) {
    this.successOnTimeout = successOnTimeout;
  }

  public String getCheckCycleTime() {
    return checkCycleTime;
  }

  public void setCheckCycleTime( String checkCycleTime ) {
    this.checkCycleTime = checkCycleTime;
  }

  public String getMaximumTimeout() {
    return maximumTimeout;
  }

  /**
   * Set how long the action may test the connection for a success result
   *
   * @param maximumTimeout Number of seconds to wait for success
   */
  public void setMaximumTimeout( String maximumTimeout ) {
    this.maximumTimeout = maximumTimeout;
  }

  @Override
  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      String dbname = XMLHandler.getTagValue( entrynode, "connection" );
      connection = DatabaseMeta.loadDatabase( metaStore, dbname );
      schemaname = XMLHandler.getTagValue( entrynode, "schemaname" );
      tablename = XMLHandler.getTagValue( entrynode, "tablename" );
      successCondition =
        getSucessConditionByCode( Const.NVL( XMLHandler.getTagValue( entrynode, "success_condition" ), "" ) );
      rowsCountValue = Const.NVL( XMLHandler.getTagValue( entrynode, "rows_count_value" ), "0" );
      iscustomSQL = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "is_custom_sql" ) );
      isUseVars = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "is_usevars" ) );
      customSQL = XMLHandler.getTagValue( entrynode, "custom_sql" );
      isAddRowsResult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_rows_result" ) );
      maximumTimeout = XMLHandler.getTagValue( entrynode, "maximum_timeout" );
      checkCycleTime = XMLHandler.getTagValue( entrynode, "check_cycle_time" );
      successOnTimeout = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "success_on_timeout" ) );
      isClearResultList = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "clear_result_rows" ) );

    } catch ( HopException e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "ActionWaitForSQL.UnableLoadXML" ), e );
    }
  }

  private static int getSuccessConditionByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < successConditionsCode.length; i++ ) {
      if ( successConditionsCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public void setDatabase( DatabaseMeta database ) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return connection;
  }

  @Override
  public boolean evaluates() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  // Visible for testing purposes
  protected void checkConnection() throws HopDatabaseException {
    // check connection
    // connect and disconnect
    Database dbchecked = null;
    try {
      dbchecked = new Database( this, connection );
      dbchecked.shareVariablesWith( this );
      dbchecked.connect( parentWorkflow.getTransactionId(), null );
    } finally {
      if ( dbchecked != null ) {
        dbchecked.disconnect();
      }
    }
  }

  @Override
  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 1 );
    String realCustomSQL = null;
    String realTablename = environmentSubstitute( tablename );
    String realSchemaname = environmentSubstitute( schemaname );

    if ( connection == null ) {
      logError( BaseMessages.getString( PKG, "ActionWaitForSQL.NoDbConnection" ) );
      return result;
    }

    if ( iscustomSQL ) {
      // clear result list rows
      if ( isClearResultList ) {
        result.getRows().clear();
      }

      realCustomSQL = customSQL;
      if ( isUseVars ) {
        realCustomSQL = environmentSubstitute( realCustomSQL );
      }
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "ActionWaitForSQL.Log.EnteredCustomSQL", realCustomSQL ) );
      }

      if ( Utils.isEmpty( realCustomSQL ) ) {
        logError( BaseMessages.getString( PKG, "ActionWaitForSQL.Error.NoCustomSQL" ) );
        return result;
      }

    } else {
      if ( Utils.isEmpty( realTablename ) ) {
        logError( BaseMessages.getString( PKG, "ActionWaitForSQL.Error.NoTableName" ) );
        return result;
      }
    }

    try {
      // check connection
      // connect and disconnect
      checkConnection();

      // starttime (in seconds)
      long timeStart = System.currentTimeMillis() / 1000;

      int nrRowsLimit = Const.toInt( environmentSubstitute( rowsCountValue ), 0 );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionWaitForSQL.Log.nrRowsLimit", "" + nrRowsLimit ) );
      }

      long iMaximumTimeout =
        Const.toInt( environmentSubstitute( maximumTimeout ), Const.toInt( DEFAULT_MAXIMUM_TIMEOUT, 0 ) );
      long iCycleTime =
        Const.toInt( environmentSubstitute( checkCycleTime ), Const.toInt( DEFAULT_CHECK_CYCLE_TIME, 0 ) );

      //
      // Sanity check on some values, and complain on insanity
      //
      if ( iMaximumTimeout < 0 ) {
        iMaximumTimeout = Const.toInt( DEFAULT_MAXIMUM_TIMEOUT, 0 );
        logBasic( "Maximum timeout invalid, reset to " + iMaximumTimeout );
      }

      if ( iCycleTime < 1 ) {
        // If lower than 1 set to the default
        iCycleTime = Const.toInt( DEFAULT_CHECK_CYCLE_TIME, 1 );
        logBasic( "Check cycle time invalid, reset to " + iCycleTime );
      }

      if ( iMaximumTimeout == 0 ) {
        logBasic( "Waiting indefinitely for SQL data" );
      } else {
        logBasic( "Waiting " + iMaximumTimeout + " seconds for SQL data" );
      }

      boolean continueLoop = true;
      while ( continueLoop && !parentWorkflow.isStopped() ) {
        if ( SQLDataOK( result, nrRowsLimit, realSchemaname, realTablename, realCustomSQL ) ) {
          // SQL data exists, we're happy to exit
          logBasic( "Detected SQL data within timeout" );
          result.setResult( true );
          continueLoop = false;
        } else {
          long now = System.currentTimeMillis() / 1000;

          if ( ( iMaximumTimeout > 0 ) && ( now > ( timeStart + iMaximumTimeout ) ) ) {
            continueLoop = false;

            // SQL data doesn't exist after timeout, either true or false
            if ( isSuccessOnTimeout() ) {
              logBasic( "Didn't detect SQL data before timeout, success" );
              result.setResult( true );
            } else {
              logBasic( "Didn't detect SQL data before timeout, failure" );
              result.setResult( false );
            }
          }
          // sleep algorithm
          long sleepTime = 0;

          if ( iMaximumTimeout == 0 ) {
            sleepTime = iCycleTime;
          } else {
            if ( ( now + iCycleTime ) < ( timeStart + iMaximumTimeout ) ) {
              sleepTime = iCycleTime;
            } else {
              sleepTime = iCycleTime - ( ( now + iCycleTime ) - ( timeStart + iMaximumTimeout ) );
            }
          }
          try {
            if ( sleepTime > 0 ) {
              if ( log.isDetailed() ) {
                logDetailed( "Sleeping " + sleepTime + " seconds before next check for SQL data" );
              }
              Thread.sleep( sleepTime * 1000 );
            }
          } catch ( InterruptedException e ) {
            // something strange happened
            result.setResult( false );
            continueLoop = false;
          }
        }

      }
    } catch ( Exception e ) {
      logBasic( "Exception while waiting for SQL data: " + e.getMessage() );
    }

    if ( result.getResult() ) {
      // Remove error count set at the beginning of the method
      // PDI-15437
      result.setNrErrors( 0 );
    }

    return result;
  }

  protected boolean SQLDataOK( Result result, long nrRowsLimit, String realSchemaName, String realTableName,
                               String customSQL ) throws HopException {
    String countStatement = null;
    long rowsCount = 0;
    boolean successOK = false;
    List<Object[]> ar = null;
    IRowMeta rowMeta = null;
    Database db = new Database( this, connection );
    db.shareVariablesWith( this );
    try {
      db.connect( parentWorkflow.getTransactionId(), null );
      if ( iscustomSQL ) {
        countStatement = customSQL;
      } else {
        if ( !Utils.isEmpty( realSchemaName ) ) {
          countStatement =
            selectCount + db.getDatabaseMeta().getQuotedSchemaTableCombination( realSchemaName, realTableName );
        } else {
          countStatement = selectCount + db.getDatabaseMeta().quoteField( realTableName );
        }
      }

      if ( countStatement != null ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionWaitForSQL.Log.RunSQLStatement", countStatement ) );
        }

        if ( iscustomSQL ) {
          ar = db.getRows( countStatement, 0 );
          if ( ar != null ) {
            rowsCount = ar.size();
          } else {
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString(
                PKG, "ActionWaitForSQL.Log.customSQLreturnedNothing", countStatement ) );
            }
          }

        } else {
          RowMetaAndData row = db.getOneRow( countStatement );
          if ( row != null ) {
            rowsCount = row.getInteger( 0 );
          }
        }
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionWaitForSQL.Log.NrRowsReturned", "" + rowsCount ) );
        }

        switch ( successCondition ) {
          case ActionWaitForSQL.SUCCESS_CONDITION_ROWS_COUNT_EQUAL:
            successOK = ( rowsCount == nrRowsLimit );
            break;
          case ActionWaitForSQL.SUCCESS_CONDITION_ROWS_COUNT_DIFFERENT:
            successOK = ( rowsCount != nrRowsLimit );
            break;
          case ActionWaitForSQL.SUCCESS_CONDITION_ROWS_COUNT_SMALLER:
            successOK = ( rowsCount < nrRowsLimit );
            break;
          case ActionWaitForSQL.SUCCESS_CONDITION_ROWS_COUNT_SMALLER_EQUAL:
            successOK = ( rowsCount <= nrRowsLimit );
            break;
          case ActionWaitForSQL.SUCCESS_CONDITION_ROWS_COUNT_GREATER:
            successOK = ( rowsCount > nrRowsLimit );
            break;
          case ActionWaitForSQL.SUCCESS_CONDITION_ROWS_COUNT_GREATER_EQUAL:
            successOK = ( rowsCount >= nrRowsLimit );
            break;
          default:
            break;
        }
      } // end if countStatement!=null
    } catch ( HopDatabaseException dbe ) {
      logError( BaseMessages.getString( PKG, "ActionWaitForSQL.Error.RunningEntry", dbe.getMessage() ) );
    } finally {
      if ( db != null ) {
        if ( isAddRowsResult && iscustomSQL && ar != null ) {
          rowMeta = db.getQueryFields( countStatement, false );
        }
        db.disconnect();
      }
    }

    if ( successOK ) {
      // ad rows to result
      if ( isAddRowsResult && iscustomSQL && ar != null ) {
        List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>();
        for ( int i = 0; i < ar.size(); i++ ) {
          rows.add( new RowMetaAndData( rowMeta, ar.get( i ) ) );
        }
        if ( rows != null ) {
          result.getRows().addAll( rows );
        }
      }
    }
    return successOK;

  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  @Override
  public List<ResourceReference> getResourceDependencies( WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( workflowMeta );
    if ( connection != null ) {
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( connection.getHostname(), ResourceType.SERVER ) );
      reference.getEntries().add( new ResourceEntry( connection.getDatabaseName(), ResourceType.DATABASENAME ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IMetaStore metaStore ) {
    ActionValidatorUtils.andValidator().validate( this, "WaitForSQL", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
