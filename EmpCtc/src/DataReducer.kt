
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context
import java.util.*

class DataReducer:Reducer<Text, Text, NullWritable, Text>(){
    override fun reduce(key:Text,value:Iterable<Text>, context: Context){
        val strVal = value.map { it->it.toString() }
        var ctcMap = strVal.map { it.split('\t')[5].toInt() to it }.toMap()
        var max = ctcMap.maxBy { it.key }
        context.write(NullWritable.get(), Text(max?.value))
    }
}