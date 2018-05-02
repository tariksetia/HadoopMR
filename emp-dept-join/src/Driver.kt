import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import JoinWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import java.util.*

fun main(args:Array<String>){
    val empFile = args[0]
    val deptFile = args[1]
    val output = args[2]

    val conf = Configuration()
    val job = Job(conf,"emp-dept-join-by-id")

    job.mapperClass = DataMapper::class.java
    job.reducerClass = DataReducer::class.java
    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = JoinWritable::class.java
    job.outputValueClass = Text::class.java
    job.outputKeyClass = NullWritable::class.java

    FileInputFormat.addInputPath(job,Path(empFile))
    FileInputFormat.addInputPath(job,Path(deptFile))
    FileOutputFormat.setOutputPath(job, Path(output))

    System.exit( if(job.waitForCompletion(true)) 0 else 1)

}