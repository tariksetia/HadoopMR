package com.weblog.parse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

fun main(args:Array<String>){
    val input = args[0]
    val output = args[1]

    val conf = Configuration()
    val job = Job(conf,"Web-Log-Parser")

    job.mapperClass = WebLogParserMapper::class.java

    //This is a new Concept
    job.numReduceTasks = 0

    //This is a new Concept
    job.inputFormatClass = TextInputFormat::class.java

    job.mapOutputKeyClass = NullWritable::class.java
    job.mapOutputValueClass = Text::class.java
    job.outputKeyClass = NullWritable::class.java
    job.outputValueClass = NullWritable::class.java

    //This is a new Concept
    MultipleOutputs.addNamedOutput(job, "ParsedRecords", TextOutputFormat::class.java , NullWritable::class.java, Text::class.java);
    MultipleOutputs.addNamedOutput(job, "BadRecords", TextOutputFormat::class.java  , NullWritable::class.java, Text::class.java);

    FileInputFormat.addInputPath(job,Path(input))
    FileOutputFormat.setOutputPath(job, Path(output))

    System.exit( if (job.waitForCompletion(true)) 0 else 1)

}