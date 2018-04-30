import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Mapper.Context

import java.io.*
import java.util.*

class SumMapper : Mapper<LongWritable, Text, Text, IntWritable> () {

    override fun map(key:LongWritable, value:Text, context: Context){
        val token = StringTokenizer(value.toString())
        var sum = 0
        while (token.hasMoreTokens()){
            sum += Integer.parseInt(token.nextToken())
        }
        context.write(Text("sum"),IntWritable(sum))
}
}