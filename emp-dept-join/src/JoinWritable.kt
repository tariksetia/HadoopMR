import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException



data class JoinWritable(val value:Text = Text(), val fileName:Text=Text()):Writable{
    override fun readFields(p0: DataInput?) {
        value.readFields(p0)
        fileName.readFields(p0)
    }

    override fun write(p0: DataOutput?) {
        value.write(p0)
        fileName.write(p0)
    }
}