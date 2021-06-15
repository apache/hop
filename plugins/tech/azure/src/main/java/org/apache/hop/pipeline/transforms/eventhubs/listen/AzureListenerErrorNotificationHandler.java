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
 *
 */

package org.apache.hop.pipeline.transforms.eventhubs.listen;

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

import java.util.function.Consumer;

// The general notification handler is an object that derives from Consumer<> and takes an
// ExceptionReceivedEventArgs object
// as an argument. The argument provides the details of the error: the exception that occurred and
// the action (what EventProcessorHost
// was doing) during which the error occurred. The complete list of actions can be found in
// EventProcessorHostActionStrings.\
//
public class AzureListenerErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs> {

  private AzureListener azureTransform;

  public AzureListenerErrorNotificationHandler(AzureListener azureTransform) {
    this.azureTransform = azureTransform;
  }

  @Override
  public void accept(ExceptionReceivedEventArgs t) {

    azureTransform.logError(
        "Host "
            + t.getHostname()
            + " received general error notification during "
            + t.getAction()
            + ": "
            + t.getException().toString());
    azureTransform.setErrors(1);
    azureTransform.stopAll();
  }
}
