import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class DataMapper:Mapper<LongWritable,Text,Text,FloatWritable>(){
    override fun map(key: LongWritable?, value: Text?, context: Context?) {
        val row = value.toString().split("\\s+".toRegex())
        val host = row[0]
        val year = row[1].split(',')[0]
        val idle = row[9].toFloat()
        val utilized = 100.0f-idle

        if (context != null) {
            context.write(Text(host+"\t"+year), FloatWritable(utilized))
        }
    }
}