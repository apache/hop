package org.apache.hop.core.database;

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.ValueMetaInterface;

public class NoneDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {
  @Override public String getFieldDefinition( ValueMetaInterface v, String tk, String pk, boolean use_autoinc, boolean add_fieldname, boolean add_cr ) {
    return null;
  }

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC };
  }

  @Override public String getDriverClass() {
    return "";
  }

  @Override public String getURL( String hostname, String port, String databaseName ) throws HopDatabaseException {
    return "jdbc://none";
  }

  @Override public String getAddColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon ) {
    return "";
  }

  @Override public String getModifyColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon ) {
    return "";
  }
}
