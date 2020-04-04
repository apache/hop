package org.apache.hop.history;

import java.util.ArrayList;
import java.util.List;

/**
 * This is simply a list of things you want to store using the audit manager.
 * You can store a list of filenames for example.
 * You can store the list per group and you can specify the type of list.
 */
public class AuditList {

  private String group;

  private String type;

  private List<String> names;

  public AuditList() {
    names = new ArrayList<>();
  }

  public AuditList( String group, String type, List<String> names ) {
    this.group = group;
    this.type = type;
    this.names = names;
  }

  /**
   * Gets group
   *
   * @return value of group
   */
  public String getGroup() {
    return group;
  }

  /**
   * @param group The group to set
   */
  public void setGroup( String group ) {
    this.group = group;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( String type ) {
    this.type = type;
  }

  /**
   * Gets names
   *
   * @return value of names
   */
  public List<String> getNames() {
    return names;
  }

  /**
   * @param names The names to set
   */
  public void setNames( List<String> names ) {
    this.names = names;
  }
}
