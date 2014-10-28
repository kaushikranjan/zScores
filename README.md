zScores
=======


##What's this? 
This algorithm computes multi-dimensional z-scores for datapoints with the given dataset.
The z-score of a data point X : 

    z = (x - μ) / σ

where:

    μ is the mean of the population;
    σ is the standard deviation of the population

##How to Run
You should have spark already build as a jar file in your build library path. It has a scala file 'zScore'


From your main call the function "compute" of this class, with following parameters
```
val score = zScore.compute(rdd : RDD[Vector[String]])

the function returns a RDD[Double] containing the z-score of each data-point in the sequence
present within the data-set
