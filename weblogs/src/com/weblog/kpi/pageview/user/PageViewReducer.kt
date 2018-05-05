package com.weblog.kpi.pageview.user

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer

class PageViewReducer:Reducer<Text, IntWritable, Text, IntWritable>(){
    override fun reduce(key:Text, value:Iterable<IntWritable>,context: Context) {
        val sum = value.map { it -> it.get() }.reduce { a, b -> a + b }
        context.write(key,IntWritable(sum))
    }
}