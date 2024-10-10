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

package org.apache.hop.pipeline.transforms.stanford.nlp.simple;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.Bag;

public interface ParsedSentence {

  Object[] getInputRow();

  String getSentenceText();

  List<String> getSentenceWords();

  List<String> getSentencePosTags();

  Bag<String> getSentencePosBag();

  long getIndex();

  long getIndexBegin();

  long getIndexEnd();

  long getCharacterCount();

  long getWordCount();

  String getSentenceTextPosTagged();

  final class Builder {
    private boolean includePartOfSpeech;

    private Builder() {}

    public static Builder parsedSentenceBuilder() {
      return new Builder();
    }

    public Builder includePartOfSpeech(boolean includePartOfSpeech) {
      this.includePartOfSpeech = includePartOfSpeech;
      return this;
    }

    public ParsedSentence buildSentence(Object[] inputRow, Sentence sentence) {
      return new ParsedSentenceImpl(inputRow, sentence, includePartOfSpeech);
    }

    public List<ParsedSentence> buildSentences(Object[] inputRow, String corpus) {
      Document doc = new Document(corpus);
      List<Sentence> sentences = doc.sentences();
      List<ParsedSentence> parsedSentences = new ArrayList<>(sentences.size());
      for (Sentence sentence : sentences) {
        parsedSentences.add(buildSentence(inputRow, sentence));
      }

      return parsedSentences;
    }
  }
}
