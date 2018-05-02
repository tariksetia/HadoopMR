import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import JoinWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Mapper.Context
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import java.io.File
import java.util.*

class DataMapper: Mapper<LongWritable, Text, Text, JoinWritable>(){

    var inputFileName = ""

    override fun setup(context: Context){
        val fileSplit =  context.inputSplit as FileSplit
        inputFileName = fileSplit.path.name
    }

    override fun map(key:LongWritable, value:Text, context: Context) {
        val row = value.toString().split(',')
        val id = Text(row[0])
        val nameOrDept = Text(row[1])
        val fileName = Text(inputFileName)
        context.write(id,JoinWritable(nameOrDept,fileName))
    }
}