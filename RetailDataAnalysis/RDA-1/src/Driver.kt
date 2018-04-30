import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.FloatWritable


fun main(args:Array<String>){
    val input = args[0]
    val output = args[1]

    val conf = Configuration()
    val job = Job(conf,"Category-Value")

    job.setMapperClass(DataMapper::class.java)
    job.setReducerClass(DataReducer::class.java)

    job.setMapOutputKeyClass(Text::class.java)
    job.setMapOutputValueClass(FloatWritable::class.java)
    job.setOutputKeyClass(Text::class.java)
    job.setOutputValueClass(FloatWritable::class.java)

    FileInputFormat.addInputPath(job,Path(input))
    FileOutputFormat.setOutputPath(job, Path(output))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
}