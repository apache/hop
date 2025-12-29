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

package org.apache.hop.mongo;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.i18n.BaseMessages;
import org.bson.Document;

class MongoPropToOption {
  private static final Class<?> PKG = MongoPropToOption.class;

  private MongoUtilLogger log;

  MongoPropToOption(MongoUtilLogger log) {
    this.log = log;
  }

  public int intValue(String value, int defaultVal) {
    if (!Util.isEmpty(value)) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException n) {
        logWarn(
            BaseMessages.getString(
                PKG,
                "MongoPropToOption.Warning.Message.NumberFormat",
                value,
                Integer.toString(defaultVal)));
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public long longValue(String value, long defaultVal) {
    if (!Util.isEmpty(value)) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException n) {
        logWarn(
            BaseMessages.getString(
                PKG,
                "MongoPropToOption.Warning.Message.NumberFormat",
                value,
                Long.toString(defaultVal)));
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public boolean boolValue(String value, boolean defaultVal) {
    if (!Util.isEmpty(value)) {
      return Boolean.parseBoolean(value);
    }
    return defaultVal;
  }

  public ReadPreference readPrefValue(MongoProperties props) throws MongoDbException {
    String readPreference = props.get(MongoProp.readPreference);
    if (Util.isEmpty(readPreference)) {
      // nothing to do
      return null;
    }
    Document[] tagSets = getTagSets(props);
    NamedReadPreference preference = NamedReadPreference.byName(readPreference);
    if (preference == null) {
      throw new MongoDbException(
          BaseMessages.getString(
              PKG,
              "MongoPropToOption.ErrorMessage.ReadPreferenceNotFound",
              readPreference,
              getPrettyListOfValidPreferences()));
    }
    logInfo(
        BaseMessages.getString(
            PKG, "MongoPropToOption.Message.UsingReadPreference", preference.getName()));

    if (preference == NamedReadPreference.PRIMARY && tagSets.length > 0) {
      // Invalid combination.  Tag sets are not used with PRIMARY
      logWarn(
          BaseMessages.getString(
              PKG, "MongoPropToOption.Message.Warning.PrimaryReadPrefWithTagSets"));
      return preference.getPreference();
    } else if (tagSets.length > 0) {
      logInfo(
          BaseMessages.getString(
              PKG,
              "MongoPropToOption.Message.UsingReadPreferenceTagSets",
              Arrays.toString(tagSets)));
      Document[] remainder =
          tagSets.length > 1 ? Arrays.copyOfRange(tagSets, 1, tagSets.length) : new Document[0];
      return preference.getTaggableReadPreference(tagSets[0], remainder);
    } else {
      logInfo(
          BaseMessages.getString(PKG, "MongoPropToOption.Message.NoReadPreferenceTagSetsDefined"));
      return preference.getPreference();
    }
  }

  private String getPrettyListOfValidPreferences() {
    // [primary, primaryPreferred, secondary, secondaryPreferred, nearest]
    return Arrays.toString(new ArrayList<>(NamedReadPreference.getPreferenceNames()).toArray());
  }

  Document[] getTagSets(MongoProperties props) throws MongoDbException {
    String tagSet = props.get(MongoProp.tagSet);
    if (tagSet != null) {
      List<Document> list;
      if (!tagSet.trim().startsWith("[")) {
        // wrap the set in an array
        tagSet = "[" + tagSet + "]";
      }
      try {
        // Parse as a JSON array of documents
        Document wrapper = Document.parse("{\"tagSets\":" + tagSet + "}");
        @SuppressWarnings("unchecked")
        List<Document> tagSetList = (List<Document>) wrapper.get("tagSets");
        return tagSetList.toArray(new Document[0]);
      } catch (Exception parseException) {
        throw new MongoDbException(
            BaseMessages.getString(
                PKG, "MongoPropToOption.ErrorMessage.UnableToParseTagSets", tagSet),
            parseException);
      }
    }
    return new Document[0];
  }

  public WriteConcern writeConcernValue(final MongoProperties props) throws MongoDbException {
    // write concern
    String writeConcern = props.get(MongoProp.writeConcern);
    String wTimeout = props.get(MongoProp.wTimeout);
    boolean journaled = Boolean.valueOf(props.get(MongoProp.JOURNALED));

    WriteConcern concern;

    if (!Util.isEmpty(writeConcern) && Util.isEmpty(wTimeout) && !journaled) {
      // all defaults - timeout 0, journal = false, w = 1
      concern = WriteConcern.W1;

      if (log != null) {
        log.info(
            BaseMessages.getString(
                PKG, "MongoPropToOption.Message.ConfiguringWithDefaultWriteConcern"));
      }
    } else {
      int wt = 0;
      if (!Util.isEmpty(wTimeout)) {
        try {
          wt = Integer.parseInt(wTimeout);
        } catch (NumberFormatException n) {
          throw new MongoDbException(n);
        }
      }

      if (!Util.isEmpty(writeConcern)) {
        // try parsing as a number first
        try {
          int wc = Integer.parseInt(writeConcern);
          concern = new WriteConcern(wc).withWTimeout(wt, TimeUnit.MILLISECONDS);
          if (journaled) {
            concern = concern.withJournal(true);
          }
        } catch (NumberFormatException n) {
          // assume its a valid string - e.g. "majority" or a custom
          // getLastError label associated with a tag set
          concern = new WriteConcern(writeConcern).withWTimeout(wt, TimeUnit.MILLISECONDS);
          if (journaled) {
            concern = concern.withJournal(true);
          }
        }
      } else {
        concern = WriteConcern.W1.withWTimeout(wt, TimeUnit.MILLISECONDS);
        if (journaled) {
          concern = concern.withJournal(true);
        }
      }

      if (log != null) {
        String lwc =
            "w = "
                + String.valueOf(concern.getWObject())
                + ", wTimeout = "
                + concern.getWTimeout(TimeUnit.MILLISECONDS)
                + ", journaled = "
                + concern.getJournal();
        log.info(
            BaseMessages.getString(
                PKG, "MongoPropToOption.Message.ConfiguringWithWriteConcern", lwc));
      }
    }
    return concern;
  }

  private void logInfo(String message) {
    if (log != null) {
      log.info(message);
    }
  }

  private void logWarn(String message) {
    if (log != null) {
      log.warn(message, null);
    }
  }
}
