import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Partitioner

class DataPartitioner:Partitioner<Text,Text>(){
    override fun getPartition(key: Text, value: Text, numReducers: Int): Int {
        val row = value.toString().split('\t')
        val gender = row[3].toLowerCase()
        var result = 0
        if (numReducers!=0){
            if (gender.equals("female")) result = 0
            if (gender.equals("male")) result = 1
        }
        return result
    }
}