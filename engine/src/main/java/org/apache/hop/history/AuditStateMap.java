package org.apache.hop.history;

import java.util.HashMap;
import java.util.Map;

public class AuditStateMap {
  private Map<String, AuditState> nameStateMap;

  public AuditStateMap() {
    nameStateMap = new HashMap<>();
  }

  public AuditStateMap( Map<String, AuditState> nameStateMap ) {
    this.nameStateMap = nameStateMap;
  }

  /**
   * Gets nameStateMap
   *
   * @return value of nameStateMap
   */
  public Map<String, AuditState> getNameStateMap() {
    return nameStateMap;
  }

  /**
   * @param nameStateMap The nameStateMap to set
   */
  public void setNameStateMap( Map<String, AuditState> nameStateMap ) {
    this.nameStateMap = nameStateMap;
  }

  public void add( AuditState auditState ) {
    AuditState existing = get( auditState.getName() );
    if (existing!=null) {
      // Add the states to the existing states
      //
      existing.getStateMap().putAll(auditState.getStateMap());
    } else {
      // Store a new set of properties
      //
      nameStateMap.put( auditState.getName(), auditState );
    }
  }

  public AuditState get( String name ) {
    return nameStateMap.get( name );
  }


}
