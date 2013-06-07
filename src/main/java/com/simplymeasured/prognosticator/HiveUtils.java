/*
 * Copyright 2013 Simply Measured, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simplymeasured.prognosticator;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hcatalog.api.HCatTable;

import java.util.List;
import java.util.Map;

/**
 * Utility methods for dealing with HCatalog and HBase
 * <p/>
 * Some of this code comes from the Hive project's HiveUtils class.
 *
 * @author rob@simplymeasured.com
 * @since 5/4/13
 */
public class HiveUtils {
    private static final Log LOG = LogFactory.getLog(HiveUtils.class);

    public static final String HBASE_TABLE_NAME = "hbase.table.name";
    public static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";

    static final char[] escapeEscapeBytes = "\\\\".toCharArray();
    static final char[] escapeUnescapeBytes = "\\".toCharArray();
    static final char[] newLineEscapeBytes = "\\n".toCharArray();
    static final char[] newLineUnescapeBytes = "\n".toCharArray();
    static final char[] carriageReturnEscapeBytes = "\\r".toCharArray();
    static final char[] carriageReturnUnescapeBytes = "\r".toCharArray();
    static final char[] tabEscapeBytes = "\\t".toCharArray();
    static final char[] tabUnescapeBytes = "\t".toCharArray();
    static final char[] ctrlABytes = "\u0001".toCharArray();

    /**
     * Get the HBase table name from an HCatalog table, which can be different from the underlying HBase table name
     *
     * @param table table instance to retrieve name from
     * @return the HBase table name
     */
    public static String getTableName(HCatTable table) {
        final String result;

        Map<String, String> tableProperties = table.getTblProps();
        if (tableProperties.containsKey(HBASE_TABLE_NAME)) {
            result = tableProperties.get(HBASE_TABLE_NAME);
        } else {
            result = table.getTableName();
        }

        return result;
    }

    /**
     * Get the HBase column mappings for a table defined in HCatalog. Needed to know which column family
     * each column should reside in
     *
     * @param table table instance to retrieve mappings from
     * @return an ordered list of column family:column mappings
     */
    public static List<String> getColumnMappings(HCatTable table) {
        List<String> columnMappings = null;

        Map<String, String> tableProperties = table.getTblProps();
        if (!tableProperties.containsKey(HBASE_COLUMNS_MAPPING)) {
            LOG.warn("hbase.columns.mapping missing, assuming all column families are 'default'");
        } else {
            columnMappings = Lists.newArrayList(tableProperties.get(HBASE_COLUMNS_MAPPING).split(","));
        }

        return columnMappings;
    }

    /**
     * Gets the default separators used by Hive. HCatalog (0.4.0) doesn't reveal if any other separators
     * were used.
     * <p/>
     * TODO: Make this get these details from the Hive metastore
     *
     * @param table table instance to retrieve separators from - not presently used
     * @return an array of bytes, with each level of separator
     */
    public static byte[] getSeparators(HCatTable table) {
        byte[] separators = new byte[8];

        separators[0] = (byte) '\001';
        separators[1] = (byte) '\002';
        separators[2] = (byte) '\003';
        for (int i = 3; i < separators.length; i++) {
            separators[i] = (byte) (i + 1);
        }

        return separators;
    }

    public static String escapeString(String text) {
        if(text == null) {
            return null;
        }

        int length = text.length();
        char[] textChars = text.toCharArray();

        StringBuilder escape = new StringBuilder();

        for (int i = 0; i < length; ++i) {
            int c = text.charAt(i);
            char[] escaped;
            int start;
            int len;

            switch (c) {
                case '\\':
                    escaped = escapeEscapeBytes;
                    start = 0;
                    len = escaped.length;
                    break;

                case '\n':
                    escaped = newLineEscapeBytes;
                    start = 0;
                    len = escaped.length;
                    break;

                case '\r':
                    escaped = carriageReturnEscapeBytes;
                    start = 0;
                    len = escaped.length;
                    break;

                case '\t':
                    escaped = tabEscapeBytes;
                    start = 0;
                    len = escaped.length;
                    break;

                case '\u0001':
                    escaped = tabUnescapeBytes;
                    start = 0;
                    len = escaped.length;
                    break;

                default:
                    escaped = textChars;
                    start = i;
                    len = 1;
                    break;
            }

            escape.append(escaped, start, len);
        }

        return escape.toString();
    }

    public static String unescapeString(String text) {
        if(text == null) {
            return null;
        }

        StringBuilder result = new StringBuilder();

        int length = text.length();
        char[] textChars = text.toCharArray();

        boolean hadSlash = false;
        for (int i = 0; i < length; ++i) {
            int c = text.charAt(i);

            switch (c) {
                case '\\':
                    if (hadSlash) {
                        result.append(textChars, i, 1);
                        hadSlash = false;
                    } else {
                        hadSlash = true;
                    }

                    break;
                case 'n':
                    if (hadSlash) {
                        char[] newLine = newLineUnescapeBytes;
                        result.append(newLine, 0, newLine.length);
                    } else {
                        result.append(textChars, i, 1);
                    }
                    hadSlash = false;

                    break;
                case 'r':
                    if (hadSlash) {
                        char[] carriageReturn = carriageReturnUnescapeBytes;
                        result.append(carriageReturn, 0, carriageReturn.length);
                    } else {
                        result.append(textChars, i, 1);
                    }
                    hadSlash = false;

                    break;
                case 't':
                    if (hadSlash) {
                        char[] tab = tabUnescapeBytes;
                        result.append(tab, 0, tab.length);
                    } else {
                        result.append(textChars, i, 1);
                    }
                    hadSlash = false;

                    break;
                case '\t':
                    if (hadSlash) {
                        result.append(textChars, i - 1, 1);
                        hadSlash = false;
                    }

                    char[] ctrlA = ctrlABytes;
                    result.append(ctrlA, 0, ctrlA.length);

                    break;
                default:
                    if (hadSlash) {
                        result.append(textChars, i - 1, 1);
                        hadSlash = false;
                    }

                    result.append(textChars, i, 1);
                    break;
            }
        }

        return result.toString();
    }
}
