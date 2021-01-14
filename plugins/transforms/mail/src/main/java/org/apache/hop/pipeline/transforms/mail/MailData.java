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

package org.apache.hop.pipeline.transforms.mail;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import java.util.HashSet;
import java.util.Properties;

/**
 * Send mail transform. based on Mail action
 *
 * @author Samatar
 * @since 28-07-2008
 */
public class MailData extends BaseTransformData implements ITransformData {
  public int indexOfDestination;
  public int indexOfDestinationCc;
  public int indexOfDestinationBCc;

  public int indexOfSenderName;
  public int indexOfSenderAddress;

  public int indexOfContactPerson;
  public int indexOfContactPhone;

  public int indexOfServer;
  public int indexOfPort;

  public int indexOfAuthenticationUser;
  public int indexOfAuthenticationPass;

  public int indexOfSubject;
  public int indexOfComment;

  public int indexOfSourceFilename;
  public int indexOfSourceWildcard;
  public long zipFileLimit;

  public int indexOfDynamicZipFilename;

  public String ZipFilename;

  Properties props;

  public MimeMultipart parts;

  public IRowMeta previousRowMeta;

  public int indexOfReplyToAddresses;

  public String realSourceFileFoldername;

  public String realSourceWildcard;

  public int nrEmbeddedImages;
  public int nrattachedFiles;

  public HashSet<MimeBodyPart> embeddedMimePart;

  public int indexOfAttachedContent;
  public int IndexOfAttachedFilename;

  public MailData() {
    super();
    indexOfDestination = -1;
    indexOfDestinationCc = -1;
    indexOfDestinationBCc = -1;
    indexOfSenderName = -1;
    indexOfSenderAddress = -1;
    indexOfContactPerson = -1;
    indexOfContactPhone = -1;
    indexOfServer = -1;
    indexOfPort = -1;
    indexOfAuthenticationUser = -1;
    indexOfAuthenticationPass = -1;
    indexOfSubject = -1;
    indexOfComment = -1;
    indexOfSourceFilename = -1;
    indexOfSourceWildcard = -1;
    zipFileLimit = 0;
    indexOfDynamicZipFilename = -1;
    props = new Properties();
    indexOfReplyToAddresses = -1;
    embeddedMimePart = null;
    nrEmbeddedImages = 0;
    nrattachedFiles = 0;
    indexOfAttachedContent = -1;
    IndexOfAttachedFilename = -1;
  }

}
