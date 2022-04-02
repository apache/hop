/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.laf;

import org.apache.hop.i18n.IMessageHandler;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class RemoveAltKeyMessageHandlerTest {

  @Test
  public void testChineseStyleAltKeyMessage() {
    IMessageHandler defMessageHandler = Mockito.mock(IMessageHandler.class);
    Mockito.when(defMessageHandler.getString("a")).thenReturn("a message");
    Mockito.when(defMessageHandler.getString("b")).thenReturn("Edit(&E)");
    Mockito.when(defMessageHandler.getString("c")).thenReturn("Open(&O)...");
    IMessageHandler messageHandler = new RemoveAltKeyMessageHandler(defMessageHandler);
    assertEquals("a message", messageHandler.getString("a"));
    assertEquals("Edit", messageHandler.getString("b"));
    assertEquals("Open...", messageHandler.getString("c"));
  }
}
