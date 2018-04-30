import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.util.GenericOptionsParser;

fun main(args:Array<String>){
    val conf = Configuration()
    val job = Job(conf, "NumberSum")

    job.setMapperClass(SumMapper::class.java)
    job.setReducerClass(SumReducer::class.java)

    job.setMapOutputKeyClass(Text::class.java)
    job.setMapOutputValueClass(IntWritable::class.java)
    job.setOutputKeyClass(NullWritable::class.java)
    job.setOutputValueClass(IntWritable::class.java)

    FileInputFormat.addInputPath(job, Path(args[0]))
    FileOutputFormat.setOutputPath(job, Path(args[1]))

    System.exit(if ( job.waitForCompletion(true)) 0 else 1)

    }