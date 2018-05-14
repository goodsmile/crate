/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import java.util.ArrayList;
import java.util.List;

import static io.crate.protocols.postgres.QueryStringSplitter.CommentType.LINE;
import static io.crate.protocols.postgres.QueryStringSplitter.CommentType.MULTI_LINE;
import static io.crate.protocols.postgres.QueryStringSplitter.CommentType.NO;

/**
 * Splits a query string by semicolon into multiple statements.
 */
class QueryStringSplitter {

    enum CommentType {
        NO,
        LINE,
        MULTI_LINE
    }

    enum QuoteType {
        NONE,
        SINGLE,
        DOUBLE
    }

    public static List<String> splitQuery(String query) {
        final List<String> queries = new ArrayList<>(2);

        CommentType commentType = NO;
        QuoteType quoteType = QuoteType.NONE;

        char[] chars = query.toCharArray();

        int offset = 0;
        char lastChar = ' ';
        for (int i = 0; i < chars.length; i++) {
            char aChar = chars[i];
            switch (aChar) {
                case '\'':
                    if (commentType == NO) {
                        if (lastChar == '\'') {
                            // quoting of ' via ''
                            quoteType = QuoteType.NONE;
                        } else {
                            quoteType = QuoteType.SINGLE;
                        }
                    }
                    break;
                case '"':
                    if (commentType == NO && quoteType == QuoteType.NONE) {
                        quoteType = QuoteType.DOUBLE;
                    }
                    break;
                case '-':
                    if (commentType == NO && lastChar == '-') {
                        commentType = LINE;
                    }
                    break;
                case '*':
                    if (commentType == NO && lastChar == '/') {
                        commentType = MULTI_LINE;
                    }
                    break;
                case '/':
                    if (commentType == MULTI_LINE && lastChar == '*') {
                        commentType = NO;
                        offset = i + 1;
                    }
                    break;
                case '\n':
                    if (commentType == LINE) {
                        commentType = NO;
                        offset = i + 1;
                    }
                    break;
                case ';':
                    if (commentType == NO) {
                        queries.add(new String(chars, offset, i - offset + 1));
                        offset = i + 1;
                    }
                    break;

                default:
            }
            lastChar = aChar;
        }
        // statement might not be terminated by semicolon
        if (offset < chars.length && commentType == NO) {
            queries.add(new String(chars, offset, chars.length - offset));
        }

        return queries;
    }
}
