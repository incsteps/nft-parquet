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
}
