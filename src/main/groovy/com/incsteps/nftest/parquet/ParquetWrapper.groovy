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
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type


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

    long getRowCount(){
        return rowCount()
    }

    long rowCount(){
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(parquetFile.toPath()))) {
            return reader.recordCount
        }
    }

    int getColumnCount(){
        return columnCount()
    }

    int columnCount(){
        return columnNames().length
    }

    Map<String,Class<?>>getSchema(){
        return schema()
    }

    Map<String, Class<?>> schema() {
        def inputFile = new LocalInputFile(parquetFile.toPath())
        Map<String, Class<?>> result = [:]

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            def metadata = reader.getFooter()
            def messageType = metadata.getFileMetaData().getSchema()

            messageType.fields.each { Type type ->
                extractFields(type, "", result)
            }
        }
        return result
    }

    String[]getColumnNames(){
        return columnNames()
    }

    String[] columnNames(){
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(parquetFile.toPath()))) {
            return reader.footer.fileMetaData.schema.fields.collect { Type type -> type.name } as String[]
        }
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

    private void extractFields(Type type, String currentPath, Map<String, Class<?>> result) {
        String fieldName = currentPath.isEmpty() ? type.name : "${currentPath}.${type.name}"

        if (type.isPrimitive()) {
            PrimitiveType prim = type.asPrimitiveType()
            result[fieldName] = getJavaClass(prim)
        } else {
            GroupType group = type.asGroupType()
            group.fields.each { childType ->
                extractFields(childType, fieldName, result)
            }
        }
    }

    private Class<?> getJavaClass(PrimitiveType prim) {
        def originalType = prim.originalType

        switch (prim.primitiveTypeName) {
            case PrimitiveType.PrimitiveTypeName.BOOLEAN:
                return Boolean.class
            case PrimitiveType.PrimitiveTypeName.INT32:
                return Integer.class
            case PrimitiveType.PrimitiveTypeName.INT64:
                return Long.class
            case PrimitiveType.PrimitiveTypeName.FLOAT:
                return Float.class
            case PrimitiveType.PrimitiveTypeName.DOUBLE:
                return Double.class
            case PrimitiveType.PrimitiveTypeName.BINARY:
            case PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY:
                if (originalType == OriginalType.UTF8) {
                    return String.class
                }
                return byte[].class
            default:
                return Object.class
        }
    }
}
