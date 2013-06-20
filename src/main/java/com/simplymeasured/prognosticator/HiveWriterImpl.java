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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author rob@simplymeasured.com
 * @since 5/2/13
 */
public class HiveWriterImpl implements HiveWriter {
    private static final Log LOG = LogFactory.getLog(HiveWriterImpl.class);

    private HCatClient hcatClient;
    private Configuration hbaseConfiguration;
    private HTableFactory tableFactory;

    private static final Cache<String, HCatTable> TABLE_HANDLE_CACHE;

    public static final String TABLE_CACHE_SIZE = "prognosticator.table_cache_size";
    public static final String TABLE_CACHE_EXPIRATION_MINUTES = "prognosticator.table_cache_expiration_minutes";

    static {
        TABLE_HANDLE_CACHE = CacheBuilder.newBuilder()
                .maximumSize(Long.getLong(TABLE_CACHE_SIZE, 100))
                .expireAfterWrite(Long.getLong(TABLE_CACHE_EXPIRATION_MINUTES, 10), TimeUnit.MINUTES)
                .build();
    }

    public HiveWriterImpl(HCatClient hcatClient, Configuration hbaseConfiguration, HTableFactory tableFactory) {
        this.hcatClient = hcatClient;
        this.hbaseConfiguration = hbaseConfiguration;
        this.tableFactory = tableFactory;
    }

    @Override
    public void writeRow(final String tableName, Map<String, Object> entity) throws Exception {
        HCatTable table = TABLE_HANDLE_CACHE.get(tableName, new Callable<HCatTable>() {
            @Override
            public HCatTable call() throws Exception {
                LOG.info(String.format("Cache miss for table handle, retrieving %s", tableName));

                return hcatClient.getTable("default", tableName);
            }
        });

        String hbaseTableName = HiveUtils.getTableName(table);

        HTableInterface tableInterface = tableFactory.getTable(hbaseConfiguration, hbaseTableName);
        tableInterface.setAutoFlush(true);

        HiveSerializer serializer = new HiveSerializer(table);

        try {
            List<HCatFieldSchema> columns = table.getCols();

            HCatFieldSchema keyColumn = columns.get(0);

            final byte[] rowkey;

            final List<String> keyRequiredColumns = Lists.newArrayList();

            if(keyColumn.getType() == HCatFieldSchema.Type.STRUCT) {
                HCatSchema keySchema = keyColumn.getStructSubSchema();

                for(HCatFieldSchema fieldSchema: keySchema.getFields()) {
                    keyRequiredColumns.add(fieldSchema.getName());
                }

                rowkey = serializer.serializeHiveType(keyColumn, null, entity, 1);
            } else {
                keyRequiredColumns.add(keyColumn.getName());

                rowkey = serializer.serializeHiveType(keyColumn, null, entity.get(keyColumn.getName()), 1);
            }

            if(rowkey == null || rowkey.length == 0) {
                throw new IllegalArgumentException(String.format("Rowkey is null, required key fields missing: %s",
                        keyRequiredColumns));
            }

            Put put = new Put(rowkey);
            Delete delete = new Delete(rowkey);

            for(int i = 1; i < columns.size(); i++) {
                HCatFieldSchema columnSchema = columns.get(i);

                serializer.serialize(columnSchema, i, put, delete, entity.get(columnSchema.getName()), 1);
            }

            if(!put.isEmpty()) {
                tableInterface.put(put);
            }

            if(!delete.isEmpty()) {
                tableInterface.delete(delete);
            }
        } finally {
            tableInterface.close();
        }
    }
}
