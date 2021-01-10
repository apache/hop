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

package org.apache.hop.pipeline.transforms.mailinput;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.workflow.actions.getpop.MailConnection;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import javax.mail.Message;
import java.util.Iterator;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class MailInputData extends BaseTransformData implements ITransformData {
  public IRowMeta inputRowMeta;
  public int totalpreviousfields;

  public IRowMeta outputRowMeta;
  public MailConnection mailConn;
  public int messagesCount;
  public long rownr;
  public String folder;
  public String[] folders;
  public int folderenr;
  public boolean usePOP;
  public int indexOfFolderField;
  public Object[] readrow;
  public int rowlimit;
  public int nrFields;
  public Iterator<Message> folderIterator;

  public Integer start;
  public Integer end;

  public MailInputData() {
    super();
    mailConn = null;
    messagesCount = 0;
    folder = null;
    folderenr = 0;
    usePOP = true;
    indexOfFolderField = -1;
    readrow = null;
    totalpreviousfields = 0;
    rowlimit = 0;
  }

}
