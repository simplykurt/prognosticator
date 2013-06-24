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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A simple, forward-only cursor for retrieving rows from Hive.
 *
 * It looks at the datatypes for a row to determine if the value is JSON that needs
 * to be parsed.
 *
 * Requires HIVE-4519 to be applied to your Hive source.
 *
 * @author rob@simplymeasured.com
 * @since 6/24/13
 */
public class HiveQueryCursorImpl implements QueryCursor<Map<String, Object>> {
    private static final Log LOG = LogFactory.getLog(HiveQueryCursorImpl.class);

    private SqlRowSet rowSet;
    private Map<String, Object> cachedRow;
    private ObjectMapper objectMapper = new ObjectMapper();

    protected HiveQueryCursorImpl(SqlRowSet rowSet) {
        this.rowSet = rowSet;
    }

    @Override
    public boolean next() {
        return rowSet.next();
    }

    @Override
    public Map<String, Object> get() {
        Map<String, Object> result = Maps.newHashMap();

        final SqlRowSetMetaData metadata = rowSet.getMetaData();

        for(int i = 1; i <= metadata.getColumnCount(); i++) {
            String columnTypeName = metadata.getColumnTypeName(i);

            final Object value;

            if("array".equalsIgnoreCase(columnTypeName)) {
                value = parseJson(rowSet.getString(i), List.class);
            } else if("map".equalsIgnoreCase(columnTypeName)
                    || "struct".equalsIgnoreCase(columnTypeName)) {
                value = parseJson(rowSet.getString(i), Map.class);
            } else if("string".equalsIgnoreCase(columnTypeName)) {
                value = HiveUtils.unescapeString(rowSet.getString(i));
            } else {
                value = rowSet.getObject(i);
            }

            result.put(metadata.getColumnName(i), value);
        }

        return result;
    }

    protected Object parseJson(String stringValue, Class clazz) {
        if(stringValue == null) {
            return null;
        }

        Object result = null;
        Exception capturedException = null;

        try {
            result = objectMapper.readValue(stringValue, clazz);
        } catch(JsonParseException jpe) {
            capturedException = jpe;
        } catch(JsonMappingException jme) {
            capturedException = jme;
        } catch(IOException ioe) {
            capturedException = ioe;
        }

        if(capturedException != null) {
            LOG.warn("Unable to parse JSON returned by Hive!", capturedException);
            throw new RuntimeException(capturedException);
        }

        return result;
    }
}
