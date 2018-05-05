import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Mapper.Context

class DataMapper:Mapper<LongWritable, Text, Text, Text>(){
    override fun map(key:LongWritable,value:Text, context: Context){
        val row = value.toString().split('\t')
        context.write(Text(row[4]),value)
    }
}