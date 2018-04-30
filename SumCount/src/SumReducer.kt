import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context

import java.io.*

class SumReducer: Reducer<Text, IntWritable, NullWritable, IntWritable>(){
    override fun reduce(key: Text, value:Iterable<IntWritable>, context:Context) {

        var sum = 0
        for (int in value){
            sum += int.get()
        }

        context.write(NullWritable.get(), IntWritable(sum))
    }
}
