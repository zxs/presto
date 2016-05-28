/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc;

import com.google.common.collect.Iterators;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by catilla on 2/24/16.
 */
public class TestLambda
{
    @Test
    public void testFunction()
    {
        List<Object> parameters = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        singleValues.add(1);
        singleValues.add(2);
        singleValues.add("abc");
        Integer columnType = 1;
        Iterators.addAll(parameters, singleValues.stream().map((value) -> encode(value, columnType)).iterator());

        System.out.println(parameters);
    }

    public Object encode(Object value, Integer columnType)
    {
        if (value instanceof Integer) {
            return columnType + (Integer) value;
        }
        else {
            return value;
        }
    }
}
