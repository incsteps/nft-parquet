include { toParquet } from 'plugin/nf-parquet'

import myrecords.*


workflow MAIN {
    main:
    channel.of(1,2,3)
            .map( {
                new CustomRecord(it, "the $it record", it, it, it)
            })
            .toParquet("${workDir}/demo.parquet", [record: CustomRecord])

    emit:
    "${workDir}/demo.parquet"
}
