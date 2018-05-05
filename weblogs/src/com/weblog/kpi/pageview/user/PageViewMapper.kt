package com.weblog.kpi.pageview.user

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Mapper


class PageViewMapper:Mapper<LongWritable, Text, Text, IntWritable>(){
    override fun map(key:LongWritable, value:Text, context: Context){
        val row = value.toString().split('\t')
        val user = row[0]
        context.write(Text(user), IntWritable(1))
    }
}

