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

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author rob@simplymeasured.com
 * @since 5/10/13
 */
public class ThreadedQueryRunnable implements Runnable {
    private static final Log LOG = LogFactory.getLog(ThreadedQueryRunnable.class);

    private ObjectMapper objectMapper = new ObjectMapper();
    private DataSource dataSource;
    private String query;
    private Map<String, Object> parameters;
    private ArrayBlockingQueue<Map<String, Object>> resultQueue;

    public ThreadedQueryRunnable(DataSource dataSource, String query, Map<String, Object> parameters,
                                 ArrayBlockingQueue<Map<String, Object>> resultQueue) {
        this.dataSource = dataSource;
        this.query = query;
        this.parameters = parameters;
        this.resultQueue = resultQueue;
    }

    @Override
    public void run() {
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);

        try {
            template.query(query, parameters, new RowCallbackHandler() {
                @Override
                public void processRow(ResultSet resultSet) throws SQLException {
                    try {
                        Map<String, Object> result = Maps.newHashMap();

                        final ResultSetMetaData metadata = resultSet.getMetaData();

                        for(int i = 1; i <= metadata.getColumnCount(); i++) {
                            String columnTypeName = metadata.getColumnTypeName(i);

                            final Object value;

                            if("array".equalsIgnoreCase(columnTypeName)) {
                                String stringValue = resultSet.getString(i);
                                
                                if(stringValue != null) {
                                    value = objectMapper.readValue(stringValue, List.class);
                                } else {
                                    value = null;
                                }
                            } else if("map".equalsIgnoreCase(columnTypeName)
                                    || "struct".equalsIgnoreCase(columnTypeName)) {
                                String stringValue = resultSet.getString(i);

                                if(stringValue != null) {
                                    value = objectMapper.readValue(stringValue, Map.class);
                                } else {
                                    value = null;
                                }
                            } else {
                                value = resultSet.getObject(i);
                            }

                            result.put(metadata.getColumnName(i), value);
                        }

                        resultQueue.put(result);
                    } catch(SQLException se) {
                        LOG.warn("Database error!", se);
                        throw new RuntimeException("Database error!", se);
                    } catch(InterruptedException ie) {
                        LOG.warn("Query killed!", ie);
                        throw new RuntimeException("Query killed!", ie);
                    } catch(Exception ex) {
                        LOG.warn("Unable to parse row!", ex);
                        throw new RuntimeException("Unable to parse row!", ex);
                    }
                }
            });

            resultQueue.put(Collections.<String, Object>emptyMap());
        } catch(DataAccessException dae) {
            try {
                resultQueue.put(Collections.<String, Object>emptyMap());
            } catch(InterruptedException ie) {
                LOG.warn("Queue is dead!", ie);
            }

            LOG.warn("Unable to execute query - attempting to clean up", dae);
        } catch(InterruptedException ie) {
            LOG.warn("Queue is dead!", ie);
        }
    }
}
