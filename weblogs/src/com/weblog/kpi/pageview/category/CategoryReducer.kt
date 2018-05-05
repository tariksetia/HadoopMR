package com.weblog.kpi.pageview.category

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context

class CategoryReducer:Reducer<Text,IntWritable,Text,IntWritable>(){
    override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
        val sum = values.map { it -> it.get() }.reduce { a, b -> a + b }
        context.write(key,IntWritable(sum))
    }
}