/*
 * Copyright 2025, Incremental Steps Software Solution
 *
 *
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

package com.incsteps.nftest.parquet

import groovy.transform.CompileStatic
import com.jerolba.carpet.CarpetReader

/**
 *
 * @author Jorge Aguilera <jorge@incsteps.com>
 */
@CompileStatic
class ParquetWrapper {

    private final File parquetFile
    private final Map arguments

    ParquetWrapper(File parquetFile, Map arguments) {
        this.parquetFile = parquetFile
        this.arguments = arguments
    }

    int getRowCount(){
        return rowCount()
    }

    int rowCount(){
        final reader = new CarpetReader(parquetFile,Map)
        reader.stream().count()
    }

    int getColumnCount(){
        return columnCount()
    }

    int columnCount(){
        final reader = new CarpetReader(parquetFile,Map)
        final first = reader.bufferedIterator().next() as Map
        return first.keySet().size()
    }

    Map<String,Class>getSchema(){
        return schema()
    }

    Map<String,Class>schema(){
        final reader = new CarpetReader(parquetFile,Map)
        final first = reader.bufferedIterator().next() as Map<String,Object>
        return first.entrySet().collectEntries{
            [(it.key):it.class]
        }
    }

    String[]getColumnNames(){
        return columnNames()
    }

    String[] columnNames(){
        final reader = new CarpetReader(parquetFile,Map)
        final first = reader.bufferedIterator().next() as Map<String,Object>
        return first.keySet() as String[]
    }

    List<Object[]>getRows(){
        return rows()
    }

    List<Object[]>rows(){
        final reader = new CarpetReader<Map<String, Object>>(parquetFile,Map)
        reader.bufferedIterator().collect { Map<String, Object> row->
            row.values() as Object[]
        }
    }

    Object[]getRowIndex(int index){
        return rowIndex(index)
    }

    Object[]rowIndex(int index){
        final reader = new CarpetReader<Map<String, Object>>(parquetFile,Map)
        reader.stream()
                .skip(index ? index-1 : index)
                .map {row -> row.values()}
                .limit(1)
                .findFirst()
                .get() as Object[]
    }

}
