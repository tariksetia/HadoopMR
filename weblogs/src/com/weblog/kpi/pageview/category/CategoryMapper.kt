package com.weblog.kpi.pageview.category

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Mapper.Context

class CategoryMapper:Mapper<LongWritable,Text,Text,IntWritable>(){
    override fun map(key:LongWritable,value: Text,context: Context){
        val row = value.toString().split('\t')
        val category  = Text(row[5])
        context.write(category,IntWritable(1))
    }
}
