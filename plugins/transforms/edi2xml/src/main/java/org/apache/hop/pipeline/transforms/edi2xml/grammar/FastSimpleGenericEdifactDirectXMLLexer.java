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

import org.antlr.runtime.CharStream;
import org.antlr.runtime.EarlyExitException;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;

public class FastSimpleGenericEdifactDirectXMLLexer extends Lexer {
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
  // delegators
  public Lexer[] getDelegates() {
    return new Lexer[] {};
  }

  public FastSimpleGenericEdifactDirectXMLLexer() {}

  public FastSimpleGenericEdifactDirectXMLLexer(CharStream input) {
    this(input, new RecognizerSharedState());
  }

  public FastSimpleGenericEdifactDirectXMLLexer(CharStream input, RecognizerSharedState state) {
    super(input, state);
  }

  @Override
  public String getGrammarFileName() {
    return "C:\\workspace-sts\\Hop trunk - "
        + "restruct\\engine\\src\\org\\project-hop\\di\\pipeline\\transforms\\edi2xml\\grammar\\FastSimpleGenericEdifactDirectXML.g";
  }

  public final void mT__9() throws RecognitionException {
    int Type = T_9;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match(' ');
    state.type = Type;
    state.channel = _channel;
  }

  // $ANTLR end "T__9"

  // $ANTLR start "T__10"
  public final void mT__10() throws RecognitionException {
    int Type = T_10;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match("UNA:+,? '");
    state.type = Type;
    state.channel = _channel;
  }

  public final void mT__11() throws RecognitionException {
    int Type = T_11;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match("UNA:+.? '");

    state.type = Type;
    state.channel = _channel;
  }

  public final void mT__12() throws RecognitionException {
    int Type = T_12;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match('\n');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mT__13() throws RecognitionException {
    int Type = T_13;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match('\r');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mT__14() throws RecognitionException {
    int Type = T_14;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match('\t');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mRELEASE_CHARACTER() throws RecognitionException {
    int Type = RELEASE_CHARACTER;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match('?');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mELEMENT_SEPARATOR() throws RecognitionException {
    int Type = ELEMENT_SEPARATOR;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match('+');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mSEGMENT_TERMINATOR() throws RecognitionException {
    int Type = SEGMENT_TERMINATOR;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match('\'');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mCOMPLEX_ELEMENT_ITEM_SEPARATOR() throws RecognitionException {
    int Type = COMPLEX_ELEMENT_ITEM_SEPARATOR;
    int _channel = DEFAULT_TOKEN_CHANNEL;

    match(':');
    state.type = Type;
    state.channel = _channel;
  }

  public final void mTEXT_DATA() throws RecognitionException {
    int Type = TEXT_DATA;
    int _channel = DEFAULT_TOKEN_CHANNEL;
    int cnt1 = 0;
    loop1:
    do {
      int alt1 = 6;
      int LA1_0 = input.LA(1);

      if (((LA1_0 >= '\u0000' && LA1_0 <= '&')
          || (LA1_0 >= '(' && LA1_0 <= '*')
          || (LA1_0 >= ',' && LA1_0 <= '9')
          || (LA1_0 >= ';' && LA1_0 <= '>')
          || (LA1_0 >= '@' && LA1_0 <= '\uFFFF'))) {
        alt1 = 1;
      } else if ((LA1_0 == '?')) {
        switch (input.LA(2)) {
          case '+':
            alt1 = 2;
            break;
          case '?':
            alt1 = 3;
            break;
          case ':':
            alt1 = 4;
            break;
          case '\'':
            alt1 = 5;
            break;
          default:
            break;
        }
      }

      switch (alt1) {
        case 1:
          if ((input.LA(1) >= '\u0000' && input.LA(1) <= '&')
              || (input.LA(1) >= '(' && input.LA(1) <= '*')
              || (input.LA(1) >= ',' && input.LA(1) <= '9')
              || (input.LA(1) >= ';' && input.LA(1) <= '>')
              || (input.LA(1) >= '@' && input.LA(1) <= '\uFFFF')) {
            input.consume();
          } else {
            MismatchedSetException mse = new MismatchedSetException(null, input);
            recover(mse);
            throw mse;
          }
          break;
        case 2:
          mRELEASE_CHARACTER();
          mELEMENT_SEPARATOR();
          break;
        case 3:
          mRELEASE_CHARACTER();
          mRELEASE_CHARACTER();
          break;
        case 4:
          mRELEASE_CHARACTER();
          mCOMPLEX_ELEMENT_ITEM_SEPARATOR();
          break;
        case 5:
          mRELEASE_CHARACTER();
          mSEGMENT_TERMINATOR();
          break;

        default:
          if (cnt1 >= 1) {
            break loop1;
          }
          EarlyExitException eee = new EarlyExitException(1, input);
          throw eee;
      }
      cnt1++;
    } while (true);
    state.type = Type;
    state.channel = _channel;
  }

  public void mTokens() throws RecognitionException {
    int alt2 = 11;
    int LA2_0 = input.LA(1);

    if ((LA2_0 == ' ')) {
      int LA2_1 = input.LA(2);

      if (((LA2_1 >= '\u0000' && LA2_1 <= '&')
          || (LA2_1 >= '(' && LA2_1 <= '*')
          || (LA2_1 >= ',' && LA2_1 <= '9')
          || (LA2_1 >= ';' && LA2_1 <= '\uFFFF'))) {
        alt2 = 11;
      } else {
        alt2 = 1;
      }
    } else if ((LA2_0 == 'U')) {
      int LA2_2 = input.LA(2);

      if ((LA2_2 == 'N')) {
        int LA2_12 = input.LA(3);

        if ((LA2_12 == 'A')) {
          int LA2_17 = input.LA(4);

          if ((LA2_17 == ':')) {
            int LA2_18 = input.LA(5);

            if ((LA2_18 == '+')) {
              int LA2_19 = input.LA(6);

              if ((LA2_19 == ',')) {
                alt2 = 2;
              } else if ((LA2_19 == '.')) {
                alt2 = 3;
              } else {
                throw new NoViableAltException("", 2, 19, input);
              }
            } else {
              throw new NoViableAltException("", 2, 18, input);
            }
          } else {
            alt2 = 11;
          }
        } else {
          alt2 = 11;
        }
      } else {
        alt2 = 11;
      }
    } else if ((LA2_0 == '\n')) {
      int LA2_3 = input.LA(2);

      if (((LA2_3 >= '\u0000' && LA2_3 <= '&')
          || (LA2_3 >= '(' && LA2_3 <= '*')
          || (LA2_3 >= ',' && LA2_3 <= '9')
          || (LA2_3 >= ';' && LA2_3 <= '\uFFFF'))) {
        alt2 = 11;
      } else {
        alt2 = 4;
      }
    } else if ((LA2_0 == '\r')) {
      int LA2_4 = input.LA(2);

      if (((LA2_4 >= '\u0000' && LA2_4 <= '&')
          || (LA2_4 >= '(' && LA2_4 <= '*')
          || (LA2_4 >= ',' && LA2_4 <= '9')
          || (LA2_4 >= ';' && LA2_4 <= '\uFFFF'))) {
        alt2 = 11;
      } else {
        alt2 = 5;
      }
    } else if ((LA2_0 == '\t')) {
      int LA2_5 = input.LA(2);

      if (((LA2_5 >= '\u0000' && LA2_5 <= '&')
          || (LA2_5 >= '(' && LA2_5 <= '*')
          || (LA2_5 >= ',' && LA2_5 <= '9')
          || (LA2_5 >= ';' && LA2_5 <= '\uFFFF'))) {
        alt2 = 11;
      } else {
        alt2 = 6;
      }
    } else if ((LA2_0 == '?')) {
      int LA2_6 = input.LA(2);

      if ((LA2_6 == '\'' || LA2_6 == '+' || LA2_6 == ':' || LA2_6 == '?')) {
        alt2 = 11;
      } else {
        alt2 = 7;
      }
    } else if ((LA2_0 == '+')) {
      alt2 = 8;
    } else if ((LA2_0 == '\'')) {
      alt2 = 9;
    } else if ((LA2_0 == ':')) {
      alt2 = 10;
    } else if (((LA2_0 >= '\u0000' && LA2_0 <= '\b')
        || (LA2_0 >= '\u000B' && LA2_0 <= '\f')
        || (LA2_0 >= '\u000E' && LA2_0 <= '\u001F')
        || (LA2_0 >= '!' && LA2_0 <= '&')
        || (LA2_0 >= '(' && LA2_0 <= '*')
        || (LA2_0 >= ',' && LA2_0 <= '9')
        || (LA2_0 >= ';' && LA2_0 <= '>')
        || (LA2_0 >= '@' && LA2_0 <= 'T')
        || (LA2_0 >= 'V' && LA2_0 <= '\uFFFF'))) {
      alt2 = 11;
    } else {
      throw new NoViableAltException("", 2, 0, input);
    }
    switch (alt2) {
      case 1:
        mT__9();
        break;
      case 2:
        mT__10();
        break;
      case 3:
        mT__11();
        break;
      case 4:
        mT__12();
        break;
      case 5:
        mT__13();
        break;
      case 6:
        mT__14();
        break;
      case 7:
        mRELEASE_CHARACTER();
        break;
      case 8:
        mELEMENT_SEPARATOR();
        break;
      case 9:
        mSEGMENT_TERMINATOR();
        break;
      case 10:
        mCOMPLEX_ELEMENT_ITEM_SEPARATOR();
        break;
      case 11:
        mTEXT_DATA();
        break;
      default:
        break;
    }
  }
}
