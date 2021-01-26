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

package org.apache.hop.workflow.actions.evaluatetablecontent;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * This defines a Table content evaluation action
 *
 * @author Samatar
 * @since 22-07-2008
 */

@Action(
		  id = "EVAL_TABLE_CONTENT",
		  name = "i18n::ActionEvalTableContent.Name",
		  description = "i18n::ActionEvalTableContent.Description",
		  image = "EvalTableContent.svg",
		  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
		  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/evaluatetablecontent.html"
)
public class ActionEvalTableContent extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionEvalTableContent.class; // For Translator

  private boolean addRowsResult;
  private boolean clearResultList;
  private boolean useVars;
  private boolean useCustomSql;
  private String customSql;
  private DatabaseMeta connection;
  private String tableName;
  private String schemaname;
  private String limit;
  private int successCondition;

  private static final String selectCount = "SELECT count(*) FROM ";

  public static final String[] successConditionsDesc = new String[] {
    BaseMessages.getString( PKG, "ActionEvalTableContent.SuccessWhenRowCountEqual.Label" ),
    BaseMessages.getString( PKG, "ActionEvalTableContent.SuccessWhenRowCountDifferent.Label" ),
    BaseMessages.getString( PKG, "ActionEvalTableContent.SuccessWhenRowCountSmallerThan.Label" ),
    BaseMessages.getString( PKG, "ActionEvalTableContent.SuccessWhenRowCountSmallerOrEqualThan.Label" ),
    BaseMessages.getString( PKG, "ActionEvalTableContent.SuccessWhenRowCountGreaterThan.Label" ),
    BaseMessages.getString( PKG, "ActionEvalTableContent.SuccessWhenRowCountGreaterOrEqual.Label" )

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

  public ActionEvalTableContent( String n ) {
    super( n, "" );
    limit = "0";
    successCondition = SUCCESS_CONDITION_ROWS_COUNT_GREATER;
    useCustomSql = false;
    useVars = false;
    addRowsResult = false;
    clearResultList = true;
    customSql = null;
    schemaname = null;
    tableName = null;
    connection = null;
  }

  public ActionEvalTableContent() {
    this( "" );
  }

  public Object clone() {
    ActionEvalTableContent je = (ActionEvalTableContent) super.clone();
    return je;
  }

  /**
   * @return the successCondition
   */
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

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "tablename", tableName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "success_condition", getSuccessConditionCode( successCondition ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "limit", limit ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "is_custom_sql", useCustomSql) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "is_usevars", useVars ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "custom_sql", customSql) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_rows_result", addRowsResult ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "clear_result_rows", clearResultList ) );

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

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      String dbname = XmlHandler.getTagValue( entrynode, "connection" );
      connection = DatabaseMeta.loadDatabase( metadataProvider, dbname );
      schemaname = XmlHandler.getTagValue( entrynode, "schemaname" );
      tableName = XmlHandler.getTagValue( entrynode, "tablename" );
      successCondition =
        getSucessConditionByCode( Const.NVL( XmlHandler.getTagValue( entrynode, "success_condition" ), "" ) );
      limit = Const.NVL( XmlHandler.getTagValue( entrynode, "limit" ), "0" );
      useCustomSql = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "is_custom_sql" ) );
      useVars = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "is_usevars" ) );
      customSql = XmlHandler.getTagValue( entrynode, "custom_sql" );
      addRowsResult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_rows_result" ) );
      clearResultList = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "clear_result_rows" ) );

    } catch ( HopException e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "ActionEvalTableContent.UnableLoadXML" ), e );
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

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    // see PDI-10270, PDI-10644 for details
    boolean oldBehavior = "Y".equalsIgnoreCase( getVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "N" ) );

    String countSqlStatement = null;
    long rowsCount = 0;
    long errCount = 0;

    boolean successOK = false;

    int nrRowsLimit = Const.toInt( resolve( limit ), 0 );
    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "ActionEvalTableContent.Log.nrRowsLimit", "" + nrRowsLimit ) );
    }

    if ( connection != null ) {
      Database db = new Database( this, this, connection );
      try {
        db.connect();

        if (useCustomSql) {
          String realCustomSql = customSql;
          if ( useVars ) {
            realCustomSql = resolve( realCustomSql );
          }
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "ActionEvalTableContent.Log.EnteredCustomSQL", realCustomSql ) );
          }

          if ( !Utils.isEmpty( realCustomSql ) ) {
            countSqlStatement = realCustomSql;
          } else {
            errCount++;
            logError( BaseMessages.getString( PKG, "ActionEvalTableContent.Error.NoCustomSQL" ) );
          }

        } else {
          String realTablename = resolve( tableName );
          String realSchemaname = resolve( schemaname );

          if ( !Utils.isEmpty( realTablename ) ) {
            if ( !Utils.isEmpty( realSchemaname ) ) {
              countSqlStatement =
                selectCount
                  + db.getDatabaseMeta().getQuotedSchemaTableCombination( this, realSchemaname, realTablename );
            } else {
              countSqlStatement = selectCount + db.getDatabaseMeta().quoteField( realTablename );
            }
          } else {
            errCount++;
            logError( BaseMessages.getString( PKG, "ActionEvalTableContent.Error.NoTableName" ) );
          }
        }

        if ( countSqlStatement != null ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "ActionEvalTableContent.Log.RunSQLStatement", countSqlStatement ) );
          }

          if (useCustomSql) {
            if ( clearResultList ) {
              result.getRows().clear();
            }

            List<Object[]> ar = db.getRows( countSqlStatement, 0 );
            if ( ar != null ) {
              rowsCount = ar.size();

              // ad rows to result
              IRowMeta rowMeta = db.getQueryFields( countSqlStatement, false );

              List<RowMetaAndData> rows = new ArrayList<>();
              for ( int i = 0; i < ar.size(); i++ ) {
                rows.add( new RowMetaAndData( rowMeta, ar.get( i ) ) );
              }
              if ( addRowsResult && useCustomSql) {
                if ( rows != null ) {
                  result.getRows().addAll( rows );
                }
              }
            } else {
              if ( log.isDebug() ) {
                logDebug( BaseMessages.getString(
                  PKG, "ActionEvalTableContent.Log.customSQLreturnedNothing", countSqlStatement ) );
              }
            }

          } else {
            RowMetaAndData row = db.getOneRow( countSqlStatement );
            if ( row != null ) {
              rowsCount = row.getInteger( 0 );
            }
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "ActionEvalTableContent.Log.NrRowsReturned", ""
              + rowsCount ) );
          }
          switch ( successCondition ) {
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL:
              successOK = ( rowsCount == nrRowsLimit );
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_DIFFERENT:
              successOK = ( rowsCount != nrRowsLimit );
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_SMALLER:
              successOK = ( rowsCount < nrRowsLimit );
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_SMALLER_EQUAL:
              successOK = ( rowsCount <= nrRowsLimit );
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_GREATER:
              successOK = ( rowsCount > nrRowsLimit );
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_GREATER_EQUAL:
              successOK = ( rowsCount >= nrRowsLimit );
              break;
            default:
              break;
          }

          if ( !successOK && oldBehavior ) {
            errCount++;
          }
        } // end if countSqlStatement!=null
      } catch ( HopException dbe ) {
        errCount++;
        logError( BaseMessages.getString( PKG, "ActionEvalTableContent.Error.RunningEntry", dbe.getMessage() ) );
      } finally {
        if ( db != null ) {
          db.disconnect();
        }
      }
    } else {
      errCount++;
      logError( BaseMessages.getString( PKG, "ActionEvalTableContent.NoDbConnection" ) );
    }

    result.setResult( successOK );
    result.setNrLinesRead( rowsCount );
    result.setNrErrors( errCount );

    return result;
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
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
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "WaitForSQL", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

  public boolean isAddRowsResult() {
    return addRowsResult;
  }

  public void setAddRowsResult( boolean addRowsResult ) {
    this.addRowsResult = addRowsResult;
  }

  public boolean isClearResultList() {
    return clearResultList;
  }

  public void setClearResultList( boolean clearResultList ) {
    this.clearResultList = clearResultList;
  }

  public boolean isUseVars() {
    return useVars;
  }

  public void setUseVars( boolean useVars ) {
    this.useVars = useVars;
  }

  public boolean isUseCustomSql() {
    return useCustomSql;
  }

  public void setUseCustomSql(boolean useCustomSql) {
    this.useCustomSql = useCustomSql;
  }

  public String getCustomSql() {
    return customSql;
  }

  public void setCustomSql(String customSql) {
    this.customSql = customSql;
  }

  public DatabaseMeta getConnection() {
    return connection;
  }

  public void setConnection( DatabaseMeta connection ) {
    this.connection = connection;
  }

  public String getTablename() {
    return tableName;
  }

  public void setTablename( String tableName ) {
    this.tableName = tableName;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname( String schemaname ) {
    this.schemaname = schemaname;
  }

  public String getLimit() {
    return limit;
  }

  public void setLimit( String limit ) {
    this.limit = limit;
  }

  public void setSuccessCondition( int successCondition ) {
    this.successCondition = successCondition;
  }

}
