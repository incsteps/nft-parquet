include { toParquet } from 'plugin/nf-parquet'

workflow MAIN {
    main:
    channel.of(1..10)
            .filter( { false } )
            .map( {
                new myrecords.CustomRecord(it, "the $it record", it, it, it)
            })
            .toParquet("${workDir}/empty.parquet", [record: myrecords.CustomRecord])

    emit:
    "${workDir}/empty.parquet"
}
