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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author rob@simplymeasured.com
 * @since 6/24/13
 */
@RunWith(PowerMockRunner.class)
public class HiveQueryCursorImplTest {
    private SqlRowSet rowSet;
    private HiveQueryCursorImpl cursor;

    @Before
    public void setUp() {
        rowSet = mock(SqlRowSet.class);

        this.cursor = new HiveQueryCursorImpl(rowSet);
    }

    @Test
    public void testNext() {
        when(rowSet.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);

        Assert.assertTrue(cursor.next());
        Assert.assertTrue(cursor.next());
        Assert.assertFalse(cursor.next());
    }

    @Test
    public void testGet() {
        // build up a result inside the mock, test it.

        // column 1 = string, named STR
        // column 2 = long, named LNG
        // column 3 = Map, named MAP
        // column 4 = Array, named LIST
        // column 5 = Struct (serialized to a Java Map), named STRUCT

        SqlRowSetMetaData metadata = mock(SqlRowSetMetaData.class);

        when(metadata.getColumnCount()).thenReturn(5);

        when(metadata.getColumnName(1)).thenReturn("STR");
        when(metadata.getColumnTypeName(1)).thenReturn("STRING");

        when(metadata.getColumnName(2)).thenReturn("LNG");
        when(metadata.getColumnTypeName(2)).thenReturn("BIGINT");

        when(metadata.getColumnName(3)).thenReturn("MAP");
        when(metadata.getColumnTypeName(3)).thenReturn("MAP");

        when(metadata.getColumnName(4)).thenReturn("LIST");
        when(metadata.getColumnTypeName(4)).thenReturn("ARRAY");

        when(metadata.getColumnName(5)).thenReturn("STRUCT");
        when(metadata.getColumnTypeName(5)).thenReturn("STRUCT");

        when(rowSet.getMetaData()).thenReturn(metadata);

        when(rowSet.getString(1)).thenReturn("My string");
        when(rowSet.getObject(2)).thenReturn(123L);
        when(rowSet.getString(3)).thenReturn("{\"foo\":\"str1\", \"bar\":\"str2\"}");
        when(rowSet.getString(4)).thenReturn("[1, 2, 3]");
        when(rowSet.getString(5)).thenReturn("{\"struct1\": \"asdf\", \"struct2\": 1234 }");

        Map<String, Object> result = cursor.get();

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isEmpty());

        Map<String, Object> expected = new HashMap<String, Object>() {
            {
                put("STR", "My string");
                put("LNG", 123L);
                put("MAP", new HashMap<String, String>() {
                    {
                        put("foo", "str1");
                        put("bar", "str2");
                    }
                });
                put("LIST", new ArrayList<Integer>() {
                    {
                        add(1);
                        add(2);
                        add(3);
                    }
                });
                put("STRUCT", new HashMap<String, Object>() {
                    {
                        put("struct1", "asdf");
                        put("struct2", 1234);
                    }
                });
            }
        };

        Assert.assertEquals(expected, result);
    }
}
