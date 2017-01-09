import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row



object ReturnTrips {
  def compute(trips : Dataset[Row], spark : SparkSession) : Dataset[Row] = {

  }
}


//Formula for exact distance computation via the great circle distance 
def makeDistanceExpression(lat1 : Column, lat2 : Column, long1 : Column, long2 : Column) : Column = {
    //val deltaphi = abs(lat1 - lat2).toRadians
   // val deltalamda = abs(long1 - long2).toRadians

    val dLat=toRadians(lat1 - lat2)
    val dLon=toRadians(long1 - long2)
 
    val cterm = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
    val deltasigma = lit(2) * asin(sqrt(cterm))

    //val deltasigma = lit(2)*asin(sqrt(pow(sin(toRadians(deltaphi/lit(2))),2) + cos(toRadians(lat1))*cos(toRadians(lat2))*pow(sin(toRadians(deltalamda/lit(2))), 2)))        
    return deltasigma*lit(6371000)  
}
val tripsdist = trips.withColumn("Distanceexact", makeDistanceExpression($"pickup_latitude",$"dropoff_latitude",$"pickup_longitude",$"dropoff_longitude"))
val tripsdist = trips.withColumn("Pickuptime", $"tpep_pickup_datetime".cast("long")).withColumn("Droptime", $"tpep_dropoff_datetime".cast("long"))

val tripsdist = tripsdist.withColumn("Droptime", $"tpep_dropoff_datetime".cast("long"))

//As stated in the description, we assume that the earth is a sphere with radius 6371km.
//In order to find interessting points, we want to classify our points via their coordinates into buckets. 
//Imagine we put a grid over the earth with quadrats of about 100m length use 63720
val num_buckets = 63710/360 
def initBucketLat(lat: Column) : Column = {
    val BucketLat = floor((lat + lit(180))*lit(num_buckets))
    return BucketLat
}

def initBucketLong(long: Column) : Column = {   
    val BucketLong = floor((long + lit(180))*lit(num_buckets))
    return BucketLong
}
def initBucketTime(time: Column) : Column = {   
    val BucketTime = floor((time.cast("long")/28800D))
    return BucketTime
}
//Add the Bucket Columns to our Dataset
val tripsBuckets = trips.
withColumn("Pickup_Long_Bucket", initBucketLong($"pickup_longitude")).
withColumn("Pickup_Lat_Bucket", initBucketLat($"pickup_latitude")).
withColumn("Dropoff_Long_Bucket", initBucketLong($"dropoff_longitude")).
withColumn("Dropoff_Lat_Bucket", initBucketLat($"dropoff_latitude")).
withColumn("Pickup_Time_Bucket", initBucketTime($"tpep_pickup_datetime")).
withColumn("Dropoff_Time_Bucket", initBucketTime($"tpep_dropoff_datetime"))


//We now multiply our dataset 9 times to get all the neighbour buckets (because there might be interessting points in neighbour buckets as well)
val tripsClones = tripsBuckets.
withColumn("Pickup_Long_Bucket", explode(array($"Pickup_Long_Bucket"-1, $"Pickup_Long_Bucket", $"Pickup_Long_Bucket"+1))).
withColumn("Pickup_Lat_Bucket", explode(array($"Pickup_Lat_Bucket"-1, $"Pickup_Lat_Bucket", $"Pickup_Lat_Bucket"+1))).
withColumn("Pickup_Time_Bucket", explode(array($"Pickup_Time_Bucket", $"Pickup_Time_Bucket"+1)))

//Join with trips where buckets of pickup and dropoff are the same
val JoinedTrips = tripsClones.as("to").
join(tripsBuckets.as("back"), $"to.Pickup_Long_Bucket" === $"back.Dropoff_Long_Bucket" && $"to.Pickup_Lat_Bucket" === $"back.Dropoff_Lat_Bucket" && $"to.Dropoff_Time_Bucket" === $"back.Pickup_Time_Bucket")

//Now we need to check the hour rxestringation and only return those where b's pickup time is within 8 hours after a's dropoff time.
val exactJoinedTripsTime = JoinedTrips.
where((($"to.tpep_dropoff_datetime").cast("long")) < (($"back.tpep_pickup_datetime").cast("long"))).
where((($"to.tpep_dropoff_datetime").cast("long")) + 28800D > (($"back.tpep_pickup_datetime").cast("long")))

//For those trips where buckets are +/-1 equal, we calculate the exact distance and only consider those where the distance really is smaller or equal to 100m.
val exactJoinedTrips = exactJoinedTripsTime.
where(makeDistanceExpression($"back.Pickup_Lat_Bucket", $"to.Dropoff_Lat_Bucket", $"back.Pickup_Long_Bucket", $"to.Dropoff_Long_Bucket")<= 100).
where(makeDistanceExpression($"to.Pickup_Lat_Bucket", $"back.Dropoff_Lat_Bucket", $"to.Pickup_Long_Bucket", $"back.Dropoff_Long_Bucket")<= 100)


val tripsdist = exactJoinedTripsTime.withColumn("Droptime", ($"to.tpep_dropoff_datetime").cast("long") - ($"back.tpep_pickup_datetime").cast("long") )
