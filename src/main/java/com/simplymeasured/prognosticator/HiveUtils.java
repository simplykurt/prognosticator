/*
 * Copyright 2013 Simply Measured, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hcatalog.api.HCatTable;

import java.util.List;
import java.util.Map;

/**
 * Utility methods for dealing with HCatalog and HBase
 *
 * @author rob@simplymeasured.com
 * @since 5/4/13
 */
public class HiveUtils {
    private static final Log LOG = LogFactory.getLog(HiveUtils.class);

    public static final String HBASE_TABLE_NAME = "hbase.table.name";
    public static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";

    /**
     * Get the HBase table name from an HCatalog table, which can be different from the underlying HBase table name
     *
     * @param table table instance to retrieve name from
     * @return the HBase table name
     */
    public static String getTableName(HCatTable table) {
        final String result;

        Map<String, String> tableProperties = table.getTblProps();
        if(tableProperties.containsKey(HBASE_TABLE_NAME)) {
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
        if(!tableProperties.containsKey(HBASE_COLUMNS_MAPPING)) {
            LOG.warn("hbase.columns.mapping missing, assuming all column families are 'default'");
        } else {
            columnMappings = Lists.newArrayList(tableProperties.get(HBASE_COLUMNS_MAPPING).split(","));
        }

        return columnMappings;
    }

    /**
     * Gets the default separators used by Hive. HCatalog (0.4.0) doesn't reveal if any other separators
     * were used.
     *
     * TODO: Make this get these details from the Hive metastore
     *
     * @param table table instance to retrieve separators from - not presently used
     * @return an array of bytes, with each level of separator
     */
    public static byte[] getSeparators(HCatTable table) {
        byte[] separators = new byte[8];

        separators[0] = (byte)'\001';
        separators[1] = (byte)'\002';
        separators[2] = (byte)'\003';
        for (int i = 3; i < separators.length; i++) {
            separators[i] = (byte) (i + 1);
        }

        return separators;
    }
}
