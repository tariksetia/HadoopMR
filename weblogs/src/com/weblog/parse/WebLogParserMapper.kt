package com.weblog.parse

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs


class WebLogParserMapper:Mapper<LongWritable, Text, NullWritable, Text>() {
    val Pattern = "^(\\S+) (\\S+) (\\S+) \\[(.+?)\\] \"([^\"]*)\" (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\"".toRegex()
    var mos: MultipleOutputs<NullWritable, Text>? = null
    val NUM_FIELDS = 9

    //This is a new Concept
    override fun setup(context: Context?) {
        mos = MultipleOutputs<NullWritable, Text>(context)
    }

    //This is a new Concept
    override fun cleanup(context: Context?) {
        mos?.close()
    }

    override fun map(key:LongWritable, value:Text, context:Context ){
        val log = value.toString().replace("\t"," ").trim()
        val results = Pattern?.find(input = log)?.destructured?.toList()

        if (results != null && Pattern.matches(log) && results?.count()==9  ){
                val (method,url,protocol) = results[4].split(" ")
                val parsedUrl = ParseUrl(url).joinToString("\t")
                var result_list = results.slice(0 until 5) + listOf(parsedUrl) + results.slice(5 until 9)

            //This is a new Concept
            mos!!.write(
                    "ParsedRecords",
                    NullWritable.get(),
                    Text(result_list.joinToString("\t"))
            )

        }else{
            //This is a new Concept
            mos!!.write("BadRecords",NullWritable.get(),value)
        }

    }


    fun ParseUrl(url:String):List<String>{
        val url_split= url.split("?")

        val route = url_split[0]
        var params = "-"
        if (url_split.count()>1){
            params = url_split.slice(1 until url_split.count()).joinToString("")
        }

        val route_split = ParseRoute(route)
        var result = (route_split + params)
        return result


    }

    fun ParseRoute(route:String):List<String>{
        var route_split = route.split("/")
        route_split = route_split.slice(1 until route_split.count())
        val page = route_split[route_split.count()-1]
        var category_temp = route_split.slice(0 until route_split.count()-1)
        var categories =  Array(5, { i -> "-" })


        if (category_temp.count()>4){
            categories[0] = category_temp[0]
            categories[1] = category_temp[1]
            categories[2] = category_temp[2]
            categories[3] = category_temp.slice(3 until category_temp.count()).joinToString("/")

        }
        if (category_temp.count() < 4 && category_temp.count()!=0){
            var count = 0
            while (true){
                if (count>= category_temp.count()){
                    break
                }
                categories[count] = category_temp[count]
                count++
            }
        }

        if(category_temp.count() ==4) {
            categories[0] = category_temp[0]
            categories[1] = category_temp[1]
            categories[2] = category_temp[2]
            categories[3] = category_temp[3]
        }

        categories[4] = page
        return categories.toList()

    }


}
