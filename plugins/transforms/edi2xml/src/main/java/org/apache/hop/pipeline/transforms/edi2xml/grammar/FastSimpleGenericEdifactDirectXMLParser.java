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

// $ANTLR 3.4 C:\\workspace-sts\\Hop trunk -
// restruct\\engine\\src\\org\\project-hop\\di\\pipeline\\transforms\\edi2xml\\grammar\\
// FastSimpleGenericEdifactDirectXML.g 2012-12-06 11:16:38

package org.apache.hop.pipeline.transforms.edi2xml.grammar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.RuleReturnScope;
import org.antlr.runtime.TokenStream;
import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.language.AngleBracketTemplateLexer;
import org.apache.commons.lang.StringEscapeUtils;

@SuppressWarnings("java:S1104")
public class FastSimpleGenericEdifactDirectXMLParser extends Parser {
  public static final String[] tokenNames =
      new String[] {
        "<invalid>",
        "<EOR>",
        "<DOWN>",
        "<UP>",
        "COMPLEX_ELEMENT_ITEM_SEPARATOR",
        "ELEMENT_SEPARATOR",
        "RELEASE_CHARACTER",
        "SEGMENT_TERMINATOR",
        "TEXT_DATA",
        "' '",
        "'UNA:+,? \\''",
        "'UNA:+.? \\''",
        "'\\n'",
        "'\\r'",
        "'\\t'"
      };

  public static final int EOF = -1;
  public static final int T_9 = 9;
  public static final int T_10 = 10;
  public static final int T_11 = 11;
  public static final int T_12 = 12;
  public static final int T_13 = 13;
  public static final int T_14 = 14;
  public static final int COMPLEX_ELEMENT_ITEM_SEPARATOR = 4;
  public static final int ELEMENT_SEPARATOR = 5;
  public static final int RELEASE_CHARACTER = 6;
  public static final int SEGMENT_TERMINATOR = 7;
  public static final int TEXT_DATA = 8;

  // delegates
  public Parser[] getDelegates() {
    return new Parser[] {};
  }

  // delegators

  public FastSimpleGenericEdifactDirectXMLParser(TokenStream input) {
    this(input, new RecognizerSharedState());
  }

  public FastSimpleGenericEdifactDirectXMLParser(TokenStream input, RecognizerSharedState state) {
    super(input, state);
  }

  protected StringTemplateGroup templateLib =
      new StringTemplateGroup(
          "FastSimpleGenericEdifactDirectXMLParserTemplates", AngleBracketTemplateLexer.class);

  public void setTemplateLib(StringTemplateGroup templateLib) {
    this.templateLib = templateLib;
  }

  public StringTemplateGroup getTemplateLib() {
    return templateLib;
  }

  /** allows convenient multi-value initialization: "new STAttrMap().put(...).put(...)" */
  public static class STAttrMap extends HashMap<String, Object> {
    @Override
    public STAttrMap put(String attrName, Object value) {
      super.put(attrName, value);
      return this;
    }

    public STAttrMap put(String attrName, int value) {
      super.put(attrName, value);
      return this;
    }
  }

  @Override
  public String[] getTokenNames() {
    return FastSimpleGenericEdifactDirectXMLParser.tokenNames;
  }

  @Override
  public String getGrammarFileName() {
    return "C:\\workspace-sts\\Hop trunk - "
        + "restruct\\engine\\src\\org\\project-hop\\di\\pipeline\\transforms\\edi2xml\\grammar\\"
        + "FastSimpleGenericEdifactDirectXML.g";
  }

  public static final String XML_HEAD = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  public static final String TAG_EDIFACT = "<edifact>\n";
  public static final String TAG_EDIFACT_END = "</edifact>";
  public static final String TAG_ELEMENT = "\t\t<element>\n";
  public static final String TAG_ELEMENT_END = "\t\t</element>\n";
  public static final String TAG_VALUE = "\t\t\t<value>";
  public static final String TAG_VALUE_END = "</value>\n";

  public LinkedList<Object> tagIndexes = new LinkedList<>();

  // helper functions to sanitize incoming input
  public String sanitizeText(String txt) {

    // resolve all RELEASE characters
    if (txt.indexOf("?") >= 0) {
      txt = txt.replace("?+", "+");
      txt = txt.replace("?:", ":");
      txt = txt.replace("?'", "'");
      txt = txt.replace("??", "?");
    }

    // enocde XML entities
    return StringEscapeUtils.escapeXml(txt);
  }

  // assume about 8k for an edifact message
  public StringBuilder buf = new StringBuilder(8192);

  // helper method for writing tag indexes to the stream
  public void appendIndexes() {

    if (tagIndexes.isEmpty()) {
      return;
    }

    for (Object i : tagIndexes) {
      String s = (String) i;
      buf.append("\t\t<index>" + s + "</index>\n");
    }
  }

  // error handling overrides -> just exit
  protected void mismatch(IntStream input, int ttype, BitSet follow) throws RecognitionException {
    throw new MismatchedTokenException(ttype, input);
  }

  @Override
  public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
      throws RecognitionException {
    throw e;
  }

  public static class edifact_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final edifact_return edifact() throws RecognitionException {
    edifact_return retval = new edifact_return();
    retval.start = input.LT(1);

    try {
      int alt1 = 2;
      int LA1_0 = input.LA(1);
      if (LA1_0 >= 10 && LA1_0 <= 11) {
        alt1 = 1;
      }
      switch (alt1) {
        case 1:
          pushFollow(FOLLOW_una_in_edifact64);
          una();
          state._fsp--;
          break;
        default:
          break;
      }
      buf = new StringBuilder(8192);
      buf.append(XML_HEAD);
      buf.append(TAG_EDIFACT);

      loop2:
      do {
        int alt2 = 2;
        int LA2_0 = input.LA(1);

        if ((LA2_0 == TEXT_DATA)) {
          alt2 = 1;
        }

        switch (alt2) {
          case 1:
            pushFollow(FOLLOW_segment_in_edifact76);
            segment();
            state._fsp--;
            break;

          default:
            break loop2;
        }
      } while (true);
      buf.append(TAG_EDIFACT_END);
      retval.stop = input.LT(-1);
    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class una_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final una_return una() throws RecognitionException {
    una_return retval = new una_return();
    retval.start = input.LT(1);

    try {

      if ((input.LA(1) >= 10 && input.LA(1) <= 11)) {
        input.consume();
        state.errorRecovery = false;
      } else {
        throw new MismatchedSetException(null, input);
      }
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }

    return retval;
  }

  public static class segment_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final segment_return segment() throws RecognitionException {
    segment_return retval = new segment_return();
    retval.start = input.LT(1);

    tag_return tag1 = null;

    try {
      pushFollow(FOLLOW_tag_in_segment107);
      tag1 = tag();
      state._fsp--;
      buf.append("\t<" + (tag1 != null ? tag1.name : null) + ">\n");
      appendIndexes();
      loop3:
      do {
        int alt3 = 2;
        int LA3_0 = input.LA(1);

        if ((LA3_0 == ELEMENT_SEPARATOR)) {
          alt3 = 1;
        }

        switch (alt3) {
          case 1:
            pushFollow(FOLLOW_data_element_in_segment114);
            data_element();
            state._fsp--;
            break;

          default:
            break loop3;
        }
      } while (true);
      match(input, SEGMENT_TERMINATOR, FOLLOW_SEGMENT_TERMINATOR_in_segment117);
      loop4:
      do {
        int alt4 = 2;
        int LA4_0 = input.LA(1);

        if ((LA4_0 == 9 || (LA4_0 >= 12 && LA4_0 <= 14))) {
          alt4 = 1;
        }

        switch (alt4) {
          case 1:
            if (input.LA(1) == 9 || (input.LA(1) >= 12 && input.LA(1) <= 14)) {
              input.consume();
              state.errorRecovery = false;
            } else {
              throw new MismatchedSetException(null, input);
            }
            break;

          default:
            break loop4;
        }
      } while (true);
      buf.append("\t</" + (tag1 != null ? tag1.name : null) + ">\n");
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }

    return retval;
  }

  public static class data_element_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final data_element_return data_element() throws RecognitionException {
    data_element_return retval = new data_element_return();
    retval.start = input.LT(1);

    try {

      pushFollow(FOLLOW_ss_in_data_element143);
      ss();
      state._fsp--;
      pushFollow(FOLLOW_data_element_payload_in_data_element145);
      data_element_payload();
      state._fsp--;
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class data_element_payload_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final data_element_payload_return data_element_payload() throws RecognitionException {
    data_element_payload_return retval = new data_element_payload_return();
    retval.start = input.LT(1);

    try {

      buf.append(TAG_ELEMENT);
      loop5:
      do {
        int alt5 = 2;
        int LA5_0 = input.LA(1);

        if ((LA5_0 == TEXT_DATA)) {
          int LA5_1 = input.LA(2);

          if ((LA5_1 == COMPLEX_ELEMENT_ITEM_SEPARATOR)) {
            alt5 = 1;
          }

        } else if ((LA5_0 == COMPLEX_ELEMENT_ITEM_SEPARATOR)) {
          alt5 = 1;
        }

        switch (alt5) {
          case 1:
            pushFollow(FOLLOWComposite_data_item_in_data_element_payload160);
            composite_data_item();
            state._fsp--;
            pushFollow(FOLLOW_ds_in_data_element_payload162);
            ds();
            state._fsp--;
            break;

          default:
            break loop5;
        }
      } while (true);
      pushFollow(FOLLOWComposite_data_item_in_data_element_payload166);
      composite_data_item();
      state._fsp--;
      buf.append(TAG_ELEMENT_END);
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class composite_data_item_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final composite_data_item_return composite_data_item() throws RecognitionException {
    composite_data_item_return retval = new composite_data_item_return();
    retval.start = input.LT(1);

    composite_data_item_val_return composite_data_item_val2 = null;

    try {
      pushFollow(FOLLOWComposite_data_item_val_inComposite_data_item180);
      composite_data_item_val2 = composite_data_item_val();
      state._fsp--;
      buf.append(TAG_VALUE);
      buf.append(
          sanitizeText(
              (composite_data_item_val2 != null
                  ? input.toString(composite_data_item_val2.start, composite_data_item_val2.stop)
                  : null)));
      buf.append(TAG_VALUE_END);
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class composite_data_item_val_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final composite_data_item_val_return composite_data_item_val()
      throws RecognitionException {
    composite_data_item_val_return retval = new composite_data_item_val_return();
    retval.start = input.LT(1);

    try {
      int alt6 = 2;
      int LA6_0 = input.LA(1);

      if ((LA6_0 == TEXT_DATA)) {
        alt6 = 1;
      } else if (((LA6_0 >= COMPLEX_ELEMENT_ITEM_SEPARATOR && LA6_0 <= ELEMENT_SEPARATOR)
          || LA6_0 == SEGMENT_TERMINATOR)) {
        alt6 = 2;
      } else {
        throw new NoViableAltException("", 6, 0, input);
      }
      switch (alt6) {
        case 1:
          pushFollow(FOLLOW_txt_inComposite_data_item_val193);
          txt();
          state._fsp--;
          break;
        case 2:
          break;
        default:
          break;
      }
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class tag_return extends ParserRuleReturnScope {
    public String name;
    public List<Object> indexes;
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final tag_return tag() throws RecognitionException {
    tag_return retval = new tag_return();
    retval.start = input.LT(1);

    List<Object> list_i = null;
    tagName_return tagName3 = null;

    RuleReturnScope i = null;
    try {
      pushFollow(FOLLOW_tagName_in_tag208);
      tagName3 = tagName();
      state._fsp--;
      tagIndexes.clear();
      loop7:
      do {
        int alt7 = 2;
        int LA7_0 = input.LA(1);

        if ((LA7_0 == COMPLEX_ELEMENT_ITEM_SEPARATOR)) {
          alt7 = 1;
        }

        switch (alt7) {
          case 1:
            pushFollow(FOLLOW_ds_in_tag213);
            ds();
            state._fsp--;
            pushFollow(FOLLOW_tagIndex_id_in_tag217);
            i = tagIndex_id();
            state._fsp--;
            if (list_i == null) {
              list_i = new ArrayList<>();
            }
            list_i.add(i.getTemplate());
            break;

          default:
            break loop7;
        }
      } while (true);
      retval.name =
          (tagName3 != null ? input.toString(tagName3.start, tagName3.stop) : null).trim();
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class tagName_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final tagName_return tagName() throws RecognitionException {
    tagName_return retval = new tagName_return();
    retval.start = input.LT(1);

    try {

      pushFollow(FOLLOW_txt_in_tagName239);
      txt();
      state._fsp--;
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class tagIndex_id_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final tagIndex_id_return tagIndex_id() throws RecognitionException {
    tagIndex_id_return retval = new tagIndex_id_return();
    retval.start = input.LT(1);

    tagIndex_id_val_return tagIndex_id_val4 = null;

    try {

      pushFollow(FOLLOW_tagIndex_id_val_in_tagIndex_id249);
      tagIndex_id_val4 = tagIndex_id_val();
      state._fsp--;
      tagIndexes.add(
          (tagIndex_id_val4 != null
              ? input.toString(tagIndex_id_val4.start, tagIndex_id_val4.stop)
              : null));
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class tagIndex_id_val_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final tagIndex_id_val_return tagIndex_id_val() throws RecognitionException {
    tagIndex_id_val_return retval = new tagIndex_id_val_return();
    retval.start = input.LT(1);

    try {
      int alt8 = 2;
      int LA8_0 = input.LA(1);

      if ((LA8_0 == TEXT_DATA)) {
        alt8 = 1;
      } else if (((LA8_0 >= COMPLEX_ELEMENT_ITEM_SEPARATOR && LA8_0 <= ELEMENT_SEPARATOR)
          || LA8_0 == SEGMENT_TERMINATOR)) {
        alt8 = 2;
      } else {
        throw new NoViableAltException("", 8, 0, input);
      }
      switch (alt8) {
        case 1:
          pushFollow(FOLLOW_txt_in_tagIndex_id_val258);
          txt();
          state._fsp--;
          break;
        case 2:
          break;
        default:
          break;
      }
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class ds_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final ds_return ds() throws RecognitionException {
    ds_return retval = new ds_return();
    retval.start = input.LT(1);

    try {

      match(input, COMPLEX_ELEMENT_ITEM_SEPARATOR, FOLLOW_COMPLEX_ELEMENT_ITEM_SEPARATOR_in_ds271);
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class ss_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final ss_return ss() throws RecognitionException {
    ss_return retval = new ss_return();
    retval.start = input.LT(1);

    try {
      match(input, ELEMENT_SEPARATOR, FOLLOW_ELEMENT_SEPARATOR_in_ss280);
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  public static class txt_return extends ParserRuleReturnScope {
    public StringTemplate st;

    @Override
    public Object getTemplate() {
      return st;
    }

    public String toString() {
      return st == null ? null : st.toString();
    }
  }

  public final txt_return txt() throws RecognitionException {
    txt_return retval = new txt_return();
    retval.start = input.LT(1);

    try {
      match(input, TEXT_DATA, FOLLOW_TEXT_DATA_in_txt289);
      retval.stop = input.LT(-1);

    } catch (RecognitionException e) {
      // do not try to recover from parse errors, propagate the error instead
      throw e;
    }
    return retval;
  }

  // Delegated rules

  public static final BitSet FOLLOW_una_in_edifact64 = new BitSet(new long[] {0x0000000000000102L});
  public static final BitSet FOLLOW_segment_in_edifact76 =
      new BitSet(new long[] {0x0000000000000102L});
  public static final BitSet FOLLOW_tag_in_segment107 =
      new BitSet(new long[] {0x00000000000000A0L});
  public static final BitSet FOLLOW_data_element_in_segment114 =
      new BitSet(new long[] {0x00000000000000A0L});
  public static final BitSet FOLLOW_SEGMENT_TERMINATOR_in_segment117 =
      new BitSet(new long[] {0x0000000000007202L});
  public static final BitSet FOLLOW_ss_in_data_element143 =
      new BitSet(new long[] {0x0000000000000100L});
  public static final BitSet FOLLOW_data_element_payload_in_data_element145 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOWComposite_data_item_in_data_element_payload160 =
      new BitSet(new long[] {0x0000000000000010L});
  public static final BitSet FOLLOW_ds_in_data_element_payload162 =
      new BitSet(new long[] {0x0000000000000100L});
  public static final BitSet FOLLOWComposite_data_item_in_data_element_payload166 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOWComposite_data_item_val_inComposite_data_item180 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_txt_inComposite_data_item_val193 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_tagName_in_tag208 =
      new BitSet(new long[] {0x0000000000000012L});
  public static final BitSet FOLLOW_ds_in_tag213 = new BitSet(new long[] {0x0000000000000100L});
  public static final BitSet FOLLOW_tagIndex_id_in_tag217 =
      new BitSet(new long[] {0x0000000000000012L});
  public static final BitSet FOLLOW_txt_in_tagName239 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_tagIndex_id_val_in_tagIndex_id249 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_txt_in_tagIndex_id_val258 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_COMPLEX_ELEMENT_ITEM_SEPARATOR_in_ds271 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_ELEMENT_SEPARATOR_in_ss280 =
      new BitSet(new long[] {0x0000000000000002L});
  public static final BitSet FOLLOW_TEXT_DATA_in_txt289 =
      new BitSet(new long[] {0x0000000000000002L});
}
