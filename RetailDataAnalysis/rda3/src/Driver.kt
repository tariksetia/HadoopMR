import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.NullWritable

fun main(args: Array<String>){
    val input = args[0]
    val output = args[1]

    val conf = Configuration()
    val job = Job(conf,"RDA-ALL-SALES")

    job.mapperClass = DataMapper::class.java
    job.reducerClass = DataReducer::class.java
    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = FloatWritable::class.java
    job.outputKeyClass = NullWritable::class.java
    job.outputValueClass = FloatWritable::class.java

    FileInputFormat.addInputPath(job, Path(input))
    FileOutputFormat.setOutputPath(job, Path(output))

    System.exit(if(job.waitForCompletion(true)) 0 else 1)

}