/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Get z-scores of all data-points in the dataset
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.math._


/**
 * Top-level methods for zScore.
 */
object zScore {
  
  /**
   * Functions to maintain counter for line-numbers to uniquely identify data points while
   * computing frequency of each data point in an RDD
   */
 
  private var count= -1

  //function to increment the value of count
  private def incrementCount() : Int = {
    count += 1
    count
  }
  
  //function to set value  of count to -1
  private def reset(){
    count = -1
  }
 
  /**
   * Computes the mean (u1 , u2, ... uk) for k-dimensional data
   * 
   * @param rdd : RDD of Vectors of String
   * @return a RDD of mean values for each dimension
   */
  private def computeMean(rdd : RDD[Vector[String]]): RDD[Double] = {
   
    //reset the counter
    reset()
    
    //dimension of data point
    val attributeLength = rdd.first.length
    //no.of data points
    val size = rdd.count.toDouble
    
    val mean = rdd.flatMap(line => line.seq).
    				map(word => (incrementCount()%attributeLength) -> word.toDouble ).groupByKey.
    				map(word => word._1 -> ((word._2.foldLeft(0.0)(_+_))/size)).
    				sortByKey(true).map(word => word._2)
    mean
    
  }
  
  /**
   * Computes the variance (v1 , v2, ... vk) for k-dimensional data
   * 
   * @param rdd : RDD of Vectors of String
   * @param mean : RDD of mean values for each dimension
   * @return a RDD of variance values for each dimension
   */
  private def computeVariance(rdd : RDD[Vector[String]], mean : RDD[Double]) : RDD[Double] = {
    
    //reset the counter    
    reset()
    
    //dimension of data point
    val attributeLength = rdd.first.length
    //no.of data points
    val size = rdd.count
    
    
    val mean_temp = mean.collect
    
    val variance = rdd.flatMap(line => line.seq).
    				map(word => (incrementCount()%attributeLength) -> pow((word.toDouble - mean_temp(count%attributeLength)),2)).
    				groupByKey.
    				map(word => word._1 -> ((word._2.foldLeft(0.0)(_+_))/size)).
    				sortByKey(true).map(word => word._2)
   variance
  }
  
  
  /**
   * Computes the z-score for each data-point within the dataset
   * 
   * @param rdd : RDD of Vectors of String
   * @param mean : RDD of mean values for each dimension
   * @param mean : RDD of variance values for each dimension
   * @return a RDD of z-scores for each data-point
   */
   
  private def computeScore(rdd : RDD[Vector[String]], mean : RDD[Double], variance : RDD[Double]) : RDD[Double] = {
    
     
    //reset the counter    
    reset()
    
    //dimension of data point
    val attributeLength = rdd.first.length
    
    val mean_temp = mean.collect
    val variance_temp = variance.collect
    
    
    val score = rdd.flatMap(line => line.seq).
    			map(word => (incrementCount()/attributeLength) -> (pow((word.toDouble - mean_temp(count%attributeLength)),2 )/ variance_temp(count%attributeLength) )).
    			groupByKey.
    			map(word => word._1 -> sqrt(word._2.foldLeft(0.0)(_+_))).
    			sortByKey(true).map(word => word._2)
 
    score
  }

    
  /**
   * This function acts as an entry point to compute the z-scores of the data-points
   *
   * @param rdd : RDD of Vectors of String 
   * 
   * @return a RDD of z-scores for each data-point
   */    
  def compute(rdd : RDD[Vector[String]]) : RDD[Double] = {

    val mean = computeMean(rdd)
    val variance = computeVariance(rdd, mean)
    val score = computeScore(rdd, mean, variance)
    
    score
  }
   
}
