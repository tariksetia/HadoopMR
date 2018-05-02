import org.apache.hadoop.io.Text
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.mapreduce.Reducer

class DataReducer:Reducer<Text,FloatWritable,Text,FloatWritable>(){
    override fun reduce(key: Text?, values: MutableIterable<FloatWritable>?, context: Context?) {
        var vals = values?.map { it -> it.get() }?.toList()
        context?.write(key, vals?.average()?.toFloat()?.let { FloatWritable(it) })

    }
}