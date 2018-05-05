package com.weblog.kpi.pageview.category

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

fun main(args:Array<String>){
    val (input,output) = args

    val conf = Configuration()
    val job = Job(conf,"CategoryViews")

    job.mapperClass = CategoryMapper::class.java
    job.reducerClass = CategoryReducer::class.java
    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = IntWritable::class.java
    job.outputKeyClass = Text::class.java
    job.outputValueClass = IntWritable::class.java

    FileInputFormat.addInputPath(job,Path(input))
    FileOutputFormat.setOutputPath(job,Path(output))

    System.exit( if(job.waitForCompletion(true)) 0 else 1)
}
