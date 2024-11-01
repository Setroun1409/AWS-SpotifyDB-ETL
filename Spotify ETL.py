import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Artist
Artist_node1730435882310 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-setroun/staging/artists.csv"], "recurse": True}, transformation_ctx="Artist_node1730435882310")

# Script generated for node Album
Album_node1730435881057 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-setroun/staging/albums.csv"], "recurse": True}, transformation_ctx="Album_node1730435881057")

# Script generated for node Track
Track_node1730435882715 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-setroun/staging/track.csv"], "recurse": True}, transformation_ctx="Track_node1730435882715")

# Script generated for node Join Album & Artist 
Album_node1730435881057DF = Album_node1730435881057.toDF()
Artist_node1730435882310DF = Artist_node1730435882310.toDF()
JoinAlbumArtist_node1730435949713 = DynamicFrame.fromDF(Album_node1730435881057DF.join(Artist_node1730435882310DF, (Album_node1730435881057DF['artist_id'] == Artist_node1730435882310DF['id']), "left"), glueContext, "JoinAlbumArtist_node1730435949713")

# Script generated for node Join With Tracks
JoinAlbumArtist_node1730435949713DF = JoinAlbumArtist_node1730435949713.toDF()
Track_node1730435882715DF = Track_node1730435882715.toDF()
JoinWithTracks_node1730436118378 = DynamicFrame.fromDF(JoinAlbumArtist_node1730435949713DF.join(Track_node1730435882715DF, (JoinAlbumArtist_node1730435949713DF['track_id'] == Track_node1730435882715DF['track_id']), "left"), glueContext, "JoinWithTracks_node1730436118378")

# Script generated for node Drop Fields
DropFields_node1730436303946 = DropFields.apply(frame=JoinWithTracks_node1730436118378, paths=["track_id", "id", "artist_id", "album_id"], transformation_ctx="DropFields_node1730436303946")

# Script generated for node Destination
Destination_node1730436363271 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1730436303946, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-setroun/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1730436363271")

job.commit()