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

package org.apache.hop.history;

import org.apache.hop.core.exception.HopException;

import java.util.List;

/**
 * This interface describes all the things you can do with a history manager
 */
public interface IAuditManager {

  /**
   * Add an event
   *
   * @param event
   */
  void storeEvent( AuditEvent event) throws HopException;

  /**
   * Find all the events for a certain group and of a given type.
   *
   * @param group The event group
   * @param type The event type
   * @return The matching events reverse sorted by event date (last events first).
   */
  List<AuditEvent> findEvents( String group, String type) throws HopException;


  /**
   * Store a list
   * @param auditList The list to be stored
   * @throws HopException
   */
  void storeList( AuditList auditList) throws HopException;

  /**
   * Retrieve a list of items of a certain group and type
   * @param group The group to which the list belongs
   * @param type The type of list you want retrieved
   * @return The list
   * @throws HopException
   */
  AuditList retrieveList(String group, String type) throws HopException;


}
