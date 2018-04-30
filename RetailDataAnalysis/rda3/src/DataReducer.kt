import org.apache.hadoop.io.Text
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context
import java.util.*

class DataReducer: Reducer<Text, FloatWritable, NullWritable, FloatWritable>() {
    override fun reduce(key:Text,value:Iterable<FloatWritable>, context: Context){
        var sum = 0.0f
        for (sale in value){
            sum += sale.get()
        }

        context.write(NullWritable.get(), FloatWritable(sum))
    }

}