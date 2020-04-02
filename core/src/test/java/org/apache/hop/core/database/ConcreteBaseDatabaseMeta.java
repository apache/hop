package org.apache.hop.core.database;

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IValueMeta;

public class ConcreteBaseDatabaseMeta extends BaseDatabaseMeta {

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean use_autoinc,
                                    boolean add_fieldname, boolean add_cr ) {
    return null;
  }

  @Override
  public String getDriverClass() {
    return null;
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) throws HopDatabaseException {
    return null;
  }

  @Override
  public String getAddColumnStatement( String tablename, IValueMeta v, String tk, boolean use_autoinc,
                                       String pk, boolean semicolon ) {
    return null;
  }

  @Override
  public String getModifyColumnStatement( String tablename, IValueMeta v, String tk, boolean use_autoinc,
                                          String pk, boolean semicolon ) {
    return null;
  }

  @Override
  public int[] getAccessTypeList() {
    return null;
  }

}
