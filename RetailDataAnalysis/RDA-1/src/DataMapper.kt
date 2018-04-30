import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Mapper.Context

import java.util.*

class DataMapper: Mapper<LongWritable, Text, Text, FloatWritable>(){

    override fun map(key:LongWritable, value: Text, context: Context) {
        val row = value.toString().split('\t')
        val productCategory = row[3]
        val saleValue = row[4].toFloat()

        context.write(Text(productCategory), FloatWritable(saleValue))

    }
}