include { toParquet } from 'plugin/nf-parquet'

workflow MAIN {
    main:
    channel.of(1..10_000)
            .map( {
                new myrecords.Person(it, new myrecords.Job("company ${it}", "DevOps", 10), new myrecords.Address("alcala","28000", "madrid") )
            })
            .toParquet("${workDir}/people.parquet", [record: myrecords.Person])

    emit:
    "${workDir}/people.parquet"
}
