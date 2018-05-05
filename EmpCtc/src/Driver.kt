import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

fun main(args:Array<String>){
    val (input,output) = args

    val conf = Configuration()
    val job = Job(conf,"MaxCtc")

    job.mapperClass =DataMapper::class.java
    job.reducerClass = DataReducer::class.java
    job.partitionerClass =DataPartitioner::class.java
    job.numReduceTasks = 2

    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = Text::class.java
    job.outputValueClass = Text::class.java
    job.outputKeyClass = NullWritable::class.java

    FileInputFormat.addInputPath(job,Path(input))
    FileOutputFormat.setOutputPath(job,Path(output))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)

}