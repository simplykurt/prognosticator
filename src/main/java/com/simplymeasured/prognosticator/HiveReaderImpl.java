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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.springframework.beans.factory.annotation.Required;

import java.util.List;
import java.util.Map;

/**
 * Reads rows back from HBase in a format that works with Hive.
 *
 * Does not use Hive directly - meant for serializing / deserializing individual objects
 *
 * @author rob@simplymeasured.com
 * @since 5/4/13
 */
public class HiveReaderImpl implements HiveReader {
    private static final Log LOG = LogFactory.getLog(HiveReaderImpl.class);

    private HCatClient hcatClient;
    private Configuration hbaseConfiguration;
    private HTableFactory tableFactory;

    public HiveReaderImpl(HCatClient hcatClient, Configuration hbaseConfiguration, HTableFactory tableFactory) {
        this.hcatClient = hcatClient;
        this.hbaseConfiguration = hbaseConfiguration;
        this.tableFactory = tableFactory;
    }

    @Override
    public Map<String, Object> readRow(String tableName, Object keyObject) throws Exception {
        if(!(keyObject instanceof Map) && !(keyObject instanceof String) && !(keyObject instanceof byte[])) {
            throw new IllegalArgumentException("Unsupported key type - " + keyObject.getClass().getName());
        }

        Map<String, Object> result;

        HCatTable table = hcatClient.getTable("default", tableName);
        String hbaseTableName = HiveUtils.getTableName(table);

        HTableInterface tableInterface = tableFactory.getTable(hbaseConfiguration, hbaseTableName);

        try {
            List<HCatFieldSchema> columns = table.getCols();

            HCatFieldSchema keyColumn = columns.get(0);

            // we use the serializer to build the row key
            HiveSerializer serializer = new HiveSerializer(table);
            final byte[] rowKey;

            if(keyObject instanceof Map) {
                rowKey = serializer.serializeHiveType(keyColumn, null, keyObject, 0);
            } else if(keyObject instanceof String) {
                rowKey = Bytes.toBytes((String)keyObject);
            } else {
                rowKey = (byte[])keyObject;
            }

            Get get = new Get(rowKey);
            get.setCacheBlocks(true);
            get.setMaxVersions(1);

            Result dbResult = tableInterface.get(get);

            HiveDeserializer deserializer = new HiveDeserializer(table, dbResult);

            result = deserializer.deserialize();

            result.put("__rowkey", rowKey);
        } finally {
            tableInterface.close();
        }

        return result;
    }
}
