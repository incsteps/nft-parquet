include { toParquet } from 'plugin/nf-parquet'

workflow MAIN {
    main:
    channel.of(1..10_000)
            .map( {
                new myrecords.CustomRecord(it, "the $it record", it, it, it)
            })
            .toParquet("${workDir}/demo.parquet", [record: myrecords.CustomRecord])

    emit:
    "${workDir}/demo.parquet"
}
