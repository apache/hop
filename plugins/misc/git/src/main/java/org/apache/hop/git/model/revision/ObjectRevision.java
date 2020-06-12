package org.apache.hop.git.model.revision;

import java.util.Date;

/**
 * A revision is simply a name, a commit comment and a date
 *
 * @author matt
 *
 */
public interface ObjectRevision {

  /**
   * @return The internal name or number of the revision
   */
  public String getRevisionId();

  /**
   * @return The creation date of the revision
   */
  public Date getCreationDate();

  /**
   * @return The user that caused the revision
   */
  public String getLogin();

  /**
   * The commit message
   * @return the comment
   */
  String getComment();

}