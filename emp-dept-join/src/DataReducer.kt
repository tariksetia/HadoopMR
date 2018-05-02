import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import JoinWritable
import org.apache.hadoop.examples.Join
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context
import java.util.*

class DataReducer:Reducer<Text, JoinWritable, NullWritable,Text>(){
    override fun reduce(key: Text?, values: MutableIterable<JoinWritable>?, context: Context?) {
        val id = key.toString()
        var dept = ""
        var name = ""
        if (values != null) {
            for (value in values) {
                if (value.fileName.toString() == "dept.txt"){
                    dept = value.value.toString()
                }else{
                    name = value.value.toString()
                }
            }
        }

        val data: List<String> = listOf(id,name,dept)
        val out = Text(data.joinToString())
        if (context != null) {
            context.write(NullWritable.get(), out)
        }
    }
}