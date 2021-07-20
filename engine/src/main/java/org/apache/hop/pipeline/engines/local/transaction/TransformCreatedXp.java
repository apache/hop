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

package org.apache.hop.pipeline.engines.local.transaction;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformInitThread;

@ExtensionPoint(
    id = "TransformCreatedXp",
    extensionPointId = "TransformBeforeInitialize",
    description =
        "If a transform is being created and before it's initialized we copy the connection group")
public class TransformCreatedXp implements IExtensionPoint<TransformInitThread> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, TransformInitThread initThread) throws HopException {
    ITransform transform = initThread.getCombi().transform;
    Pipeline pipeline = initThread.getPipeline();
    String connectionGroup = (String) pipeline.getExtensionDataMap().get(Const.CONNECTION_GROUP);
    if (connectionGroup != null) {
      // Pass the value down to the transform...
      // We do this before the transform initializes and perhaps asks for a new connection
      //
      transform.getExtensionDataMap().put(Const.CONNECTION_GROUP, connectionGroup);
    }
  }
}
