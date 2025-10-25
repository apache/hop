/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.util;

import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.hop.core.util.Utils;
import org.apache.hop.resource.IResourceHolder;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.action.ActionBase;

/** ftp helper */
@UtilityClass
public class FtpHelper {

  /**
   * Adds a server {@link ResourceReference} to the given list if the provided server name is not
   * empty.
   *
   * @param references references the list of {@link ResourceReference} objects to add to
   * @param serverName serverName the server name to resolve and reference
   * @param action actionBase
   */
  public static void addServerResourceReferenceIfPresent(
      List<ResourceReference> references,
      String serverName,
      ActionBase action,
      IResourceHolder holder) {
    if (Utils.isEmpty(serverName)) {
      return;
    }

    String realServerName = action.resolve(serverName);
    ResourceReference reference = new ResourceReference(holder);
    reference
        .getEntries()
        .add(new ResourceEntry(realServerName, ResourceEntry.ResourceType.SERVER));
    references.add(reference);
  }
}
