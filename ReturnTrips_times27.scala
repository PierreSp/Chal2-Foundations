//Formula for exact distance computation via the great circle distance 
def makeDistanceExpression(lat1 : Column, lat2 : Column, long1 : Column, long2 : Column) : Column = {

    val dLat=toRadians(lat1 - lat2)
    val dLon=toRadians(long1 - long2)
 
    val cterm = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
    val deltasigma = lit(2) * asin(sqrt(cterm))

    return deltasigma*lit(6371e3)
}

//As stated in the description, we assume that the earth is a sphere with radius 6371km.
//In order to find interessting points, we want to classify our points via their coordinates into buckets. 
//Imagine we put a grid over the earth with quadrats of about 100m length use 63720
val num_buckets = 63710 / 360

def initBucketLat(lat: Column) : Column = {
    val BucketLat = floor((lat + lit(180)) * lit(num_buckets))
    return BucketLat
}

def initBucketLong(long: Column) : Column = {   
    val BucketLong = floor((long + lit(180)) * lit(num_buckets))
    return BucketLong
}

def initBucketTime(time: Column) : Column = {   
    val BucketTime = floor((time.cast("long") / 28800D))
    return BucketTime
}

// /////////////////////////////////////////////////////////////
// LIMIT IS DEFINED HERE
val trips2 = trips.
    drop($"VendorID").
    drop($"passenger_count").
    drop($"trip_distance").
    drop($"RatecodeID").
    drop($"store_and_fwd_flag").
    drop($"payment_type").
    drop($"fare_amount").
    drop($"extra").
    drop($"mta_tax").
    drop($"tolls_amount").
    drop($"improvement_surcharge").
    drop($"total_amount").
    drop($"tip_amount")


//Add the Bucket Columns to our Dataset
val tripsBuckets = trips2.
    withColumn("Pickup_Long_Bucket", initBucketLong($"pickup_longitude")).
    withColumn("Pickup_Lat_Bucket", initBucketLat($"pickup_latitude")).
    withColumn("Dropoff_Long_Bucket", initBucketLong($"dropoff_longitude")).
    withColumn("Dropoff_Lat_Bucket", initBucketLat($"dropoff_latitude")).
    withColumn("Pickup_Time_Bucket", initBucketTime($"tpep_pickup_datetime")).
    withColumn("Dropoff_Time_Bucket", initBucketTime($"tpep_dropoff_datetime"))

//We now multiply our dataset 9 times to get all the neighbour buckets (because there might be interessting points in neighbour buckets as well)
val tripsClones = tripsBuckets.
    withColumn(
        "Pickup_Long_Bucket", 
        explode(array($"Pickup_Long_Bucket"-1, $"Pickup_Long_Bucket", $"Pickup_Long_Bucket"+1))).
    withColumn(
            "Pickup_Lat_Bucket", 
            explode(array($"Pickup_Lat_Bucket"-1, $"Pickup_Lat_Bucket", $"Pickup_Lat_Bucket"+1))).
    withColumn(
            "Pickup_Time_Bucket", 
            explode(array($"Pickup_Time_Bucket", $"Pickup_Time_Bucket"-1)))


//Join with trips where buckets of pickup and dropoff are the same
val JoinedTrips = tripsClones.as("to").
    join(
        tripsClones.as("back"), 
        $"to.Pickup_Long_Bucket" === $"back.Dropoff_Long_Bucket" &&
        $"to.Pickup_Lat_Bucket" === $"back.Dropoff_Lat_Bucket" &&
        $"to.Dropoff_Time_Bucket" === $"back.Pickup_Time_Bucket" &&
        $"back.Pickup_Long_Bucket" === $"to.Dropoff_Long_Bucket" &&
        $"back.Pickup_Lat_Bucket" === $"to.Dropoff_Lat_Bucket", "inner")

val Joinedfilter = JoinedTrips.
    drop($"to.Pickup_Long_Bucket").
    drop($"to.Pickup_Lat_Bucket").
    drop($"to.Dropoff_Long_Bucket").
    drop($"to.Dropoff_Lat_Bucket").
    drop($"to.Pickup_Time_Bucket").
    drop($"to.Dropoff_Time_Bucket").
    drop($"back.Pickup_Long_Bucket").
    drop($"back.Pickup_Lat_Bucket").
    drop($"back.Dropoff_Long_Bucket").
    drop($"back.Dropoff_Lat_Bucket").
    drop($"back.Pickup_Time_Bucket").
    drop($"back.Dropoff_Time_Bucket").
    drop($"back.tpep_dropoff_datetime").
    drop($"to.tpep_pickup_datetime")

val exactJoinedTripsTime = Joinedfilter.
    where((($"to.tpep_dropoff_datetime").cast("long")) < (($"back.tpep_pickup_datetime").cast("long")))

val JoinedUnique = exactJoinedTripsTime.dropDuplicates

//Now we need to check the hour rxestringation and only return those where b's pickup time is within 8 hours after a's dropoff time.
val exactJoinedTripsTime2 = JoinedUnique.
    where((($"to.tpep_dropoff_datetime").cast("long")) + 28800D > (($"back.tpep_pickup_datetime").cast("long")))

//For those trips where buckets are +/-1 equal, we calculate the exact distance and only consider those where the distance really is smaller or equal to 100m.
val exactJoinedTrips = exactJoinedTripsTime.
    where(
        makeDistanceExpression(
            $"back.pickup_latitude", 
            $"to.dropoff_latitude", 
            $"back.pickup_longitude", 
            $"to.dropoff_longitude") <= 100).
    where(
        makeDistanceExpression(
            $"to.pickup_latitude", 
            $"back.dropoff_latitude", 
            $"to.pickup_longitude", 
            $"back.dropoff_longitude") <= 100)

exactJoinedTrips.agg(count("*")).show()
