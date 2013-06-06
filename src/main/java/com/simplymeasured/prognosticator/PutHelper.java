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

/**
 * @author rob@simplymeasured.com
 * @since 6/2/13
 */
public class PutHelper {
    public static Float valueAsFloat(Object value) {
        if (value instanceof Long) {
            return ((Long) value).floatValue();
        } else if (value instanceof Integer) {
            return ((Integer) value).floatValue();
        } else if (value instanceof Double) {
            return ((Double)value).floatValue();
        } else if (value instanceof Float) {
            return (Float) value;
        } else {
            throw new IllegalArgumentException(String.format("Unable to convert %s to Double", value));
        }
    }

    public static Double valueAsDouble(Object value) {
        if (value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if (value instanceof Integer) {
            return ((Integer) value).doubleValue();
        } else if (value instanceof Double) {
            return (Double) value;
        } else {
            throw new IllegalArgumentException(String.format("Unable to convert %s to Double", value));
        }
    }

    public static Integer valueAsInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else {
            throw new IllegalArgumentException(String.format("Unable to convert %s to Long", value));
        }
    }

    public static Long valueAsLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else {
            throw new IllegalArgumentException(String.format("Unable to convert %s to Long", value));
        }
    }
}
