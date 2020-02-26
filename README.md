# bigdata

sbt -mem 2000 -Dspark.master=local[*] \
-Dspark.booster.uv.path="s3a://big-data-benchmark/pavlo/text/1node/uservisits" \
-Dspark.booster.geoip.blocks.path=/home/alex/Projects/apanchenko-bgd02/src/main/resources/GeoLiteCity_20151103/GeoLiteCity-Blocks.csv \
-Dspark.booster.geoip.location.path=/home/alex/Projects/apanchenko-bgd02/src/main/resources/GeoLiteCity_20151103/GeoLiteCity-Location.csv \
-Dspark.booster.output.path=/tmp/myresult.csv \
run
