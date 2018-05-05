package com.weblog.kpi.pageview.user

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

fun main(args:Array<String>){
    val (input,output) = args

    val conf =Configuration()
    val job  = Job(conf,"Pageview-User")

    job.mapperClass = PageViewMapper::class.java
    job.combinerClass = PageViewCombiner::class.java
    job.reducerClass = PageViewReducer::class.java

    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = IntWritable::class.java
    job.outputKeyClass = Text::class.java
    job.outputValueClass = IntWritable::class.java


    MultipleOutputs.addNamedOutput(job, "PageViews", TextOutputFormat::class.java , Text::class.java, IntWritable::class.java);

    FileInputFormat.addInputPath(job,Path(input))
    FileOutputFormat.setOutputPath(job,Path(output))

    System.exit(if(job.waitForCompletion(true)) 0 else 1)
}