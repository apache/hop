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

package org.apache.hop.workflow.actions.abort;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.Assert;
import org.junit.Test;

public class ActionAbortTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    final String message = "This is a test message";
    ActionAbort action = new ActionAbort();
    action.setMessageAbort(message);
    String xml = action.getXml();

    String actionXml =
        XmlHandler.openTag(ActionMeta.XML_TAG) + xml + XmlHandler.closeTag(ActionMeta.XML_TAG);
    ActionAbort action2 = new ActionAbort();
    action2.loadXml(XmlHandler.loadXmlString(actionXml, ActionMeta.XML_TAG), null, new Variables());

    Assert.assertEquals(action.getMessageAbort(), action2.getMessageAbort());
  }
}
