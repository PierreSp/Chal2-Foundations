
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


def makeDistanceExpression(lat1 : Column, lat2 : Column, long1 : Column, long2 : Column) : Column = {
val deltaphi = abs(lat1 - lat2)
val deltalamda = abs(long1 - long2)
val deltasigma = lit(2)*asin(sqrt(pow(sin(toRadians(deltaphi/lit(2))),2) + cos(toRadians(lat1))*cos(toRadians(lat2))*pow(sin(toRadians(deltalamda/lit(2))), 2)))        
return deltasigma*lit(6371000)  //spark gibt automatisch das zurück was als letztes ausgewertet wird, dh. return nicht zwingend nötig
}
// As assume a earth radius of 6371 km , we want to use buckets of a size of about 105 m(this means we have 64000 buckets). Luckely we have deciaml coordinates
// Thus 105 meters are  decimal degree. Thus we create two new columns, which are added ( withcolumns) to the main dataset.

def setbucket_lat(lat : Column) : Column = {
val latbucket = floor((lat + lit(180)) * lit(64000/360))
return latbucket
}

def setbucket_long(long : Column) : Column = {
val longbucket = floor((long + lit(180)) * lit(64000/360))
return longbucket
}


object ReturnTrips {
  def compute(trips : Dataset[Row], spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._

    trips
  }
}


def setbucket_lat(lat : Column) : Column = {
val latbucket = floor((lat + lit(180)) * lit(64000/360))
return latbucket
}

def setbucket_long(long : Column) : Column = {
val longbucket = floor((long + lit(180)) * lit(64000/360))
return longbucket
}

var tripbucket = trips.withColumn("LOBD", setbucket_long($"dropoff_longitude")).withColumn("LABD", setbucket_long($"dropoff_latitude")).withColumn("LOBP", setbucket_long($"pickup_longitude")).withColumn("LABP", setbucket_long($"pickup_latitude"))


var tripbucket2 = trips.withColumn("LOBD", setbucket_long($"dropoff_longitude")).withColumn("LABD", setbucket_long($"dropoff_latitude")).withColumn("LOBP", setbucket_long($"pickup_longitude")).withColumn("LABP", setbucket_long($"pickup_latitude"))

// SUPERBUCKET
val superbuckettrip = tripbucket.union(tripbucket.withColumn("LOBD",$"LOBD"-1).withColumn("LABD",$"LABD"-1)).union(tripbucket.withColumn("LOBD",$"LOBD"-1).withColumn("LABD",$"LABD"-0)).union(tripbucket.withColumn("LOBD",$"LOBD"-1).withColumn("LABD",$"LABD"+1)).union(tripbucket.withColumn("LOBD",$"LOBD"+0).withColumn("LABD",$"LABD"-1)).union(tripbucket.withColumn("LOBD",$"LOBD"-0).withColumn("LABD",$"LABD"+1)).union(tripbucket.withColumn("LOBD",$"LOBD"+1).withColumn("LABD",$"LABD"-1)).union(tripbucket.withColumn("LOBD",$"LOBD"+1).withColumn("LABD",$"LABD"-0)).union(tripbucket.withColumn("LOBD",$"LOBD"+1).withColumn("LABD",$"LABD"+1))
 val superbuckettrip = tripbucket.union(tripbucket.withColumn("LOBD",$"LOBD"-1).withColumn("LABD",$"LABD"-1))
 .union(tripbucket.withColumn("LOBD",$"LOBD"-1).withColumn("LABD",$"LABD"-0))
 .union(tripbucket.withColumn("LOBD",$"LOBD"-1).withColumn("LABD",$"LABD"+1))
 .union(tripbucket.withColumn("LOBD",$"LOBD"+0).withColumn("LABD",$"LABD"-1))
 .union(tripbucket.withColumn("LOBD",$"LOBD"-0).withColumn("LABD",$"LABD"+1))
 .union(tripbucket.withColumn("LOBD",$"LOBD"+1).withColumn("LABD",$"LABD"-1))
 .union(tripbucket.withColumn("LOBD",$"LOBD"+1).withColumn("LABD",$"LABD"-0))
 .union(tripbucket.withColumn("LOBD",$"LOBD"+1).withColumn("LABD",$"LABD"+1))

var joinedtest2 = superbuckettrip.joinWith(superbuckettrip2, superbuckettrip.col("LOBD")===superbuckettrip2.col("LOBP")) // JOIN


val timesuperbuckettrip2 = timesuperbuckettrip

var joinedtest2 = timesuperbuckettrip.joinWith(timesuperbuckettrip2, timesuperbuckettrip.col("LOBD")===timesuperbuckettrip2.col("LOBP") &&  timesuperbuckettrip.col("LABD")===timesuperbuckettrip2.col("LABP")) // JOIN



var wjoin = superbuckettrip.join(tripbucket, superbuckettrip.col("LOBD")===tripbucket.col("LOBP") && superbuckettrip.col("LABD")===tripbucket.col("LABP"),"left") // JOIN

val timedstuff = wjoin.where( _1$"tpep_dropoff_datetime".cast("long") < _2$"tpep_pickup_datetime".cast("long")).where((( _1$"tpep_dropoff_datetime".cast("long")-_2$"tpep_pickup_datetime".cast("long")) / 3600D)>8)// ZEIT FILER


var exactdist = wjoin.where(makeDistanceExpression(_1$"pickup_latitude",_2$"dropoff_latitude",_1$"pickup_longitude",_2$"dropoff_longitude")).where(makeDistanceExpression(_2$"pickup_latitude",_1$"dropoff_latitude",_2$"pickup_longitude",_1$"dropoff_longitude"))
trips.select((col("tpep_dropoff_datetime").cast("long")-col("tpep_pickup_datetime").cast("long")) / 3600D).show