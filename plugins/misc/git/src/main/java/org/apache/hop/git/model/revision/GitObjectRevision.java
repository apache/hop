/*!
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hop.git.model.revision;

import java.util.Date;

public class GitObjectRevision implements ObjectRevision {

  private String revisionId;
  private String login;
  private Date creationDate;
  private String comment;

  public GitObjectRevision() {}

  public GitObjectRevision(String revisionId, String login, Date creationDate, String comment) {
    this();
    this.revisionId = revisionId;
    this.login = login;
    this.creationDate = creationDate;
    this.comment = comment;
  }

  /**
   * Gets revisionId
   *
   * @return value of revisionId
   */
  @Override
  public String getRevisionId() {
    return revisionId;
  }

  /** @param revisionId The revisionId to set */
  public void setRevisionId(String revisionId) {
    this.revisionId = revisionId;
  }

  /**
   * Gets login
   *
   * @return value of login
   */
  @Override
  public String getLogin() {
    return login;
  }

  /** @param login The login to set */
  public void setLogin(String login) {
    this.login = login;
  }

  /**
   * Gets creationDate
   *
   * @return value of creationDate
   */
  @Override
  public Date getCreationDate() {
    return creationDate;
  }

  /** @param creationDate The creationDate to set */
  public void setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
  }

  /**
   * Gets comment
   *
   * @return value of comment
   */
  public String getComment() {
    return comment;
  }

  /** @param comment The comment to set */
  public void setComment(String comment) {
    this.comment = comment;
  }
}
