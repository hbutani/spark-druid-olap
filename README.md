Sparkline BI Accelerator
====

**Latest release: [0.2.0](https://github.com/SparklineData/spark-druid-olap/wiki/Release-0.2.0)** <br/>
**Documentation:** [Overview](https://github.com/SparklineData/spark-druid-olap/wiki/Overview), [Quick Start Guide](https://github.com/SparklineData/spark-druid-olap/wiki/Quick-Start-Guide), [User Guide](https://github.com/SparklineData/spark-druid-olap/wiki/User-Guide), [Dev. Guide](https://github.com/SparklineData/spark-druid-olap/wiki/Dev.-Guide) <br/>
**Mailing List:** [User Mailing List](http://groups.google.com/group/sparklinedata) <br/>
**License:** [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0) <br/>
**Company:** [Sparkline Data](http://sparklinedata.com/)

The Sparkline BI Accelerator is a [Spark](http://spark.apache.org/) native [Business Intelligence Stack](https://en.wikipedia.org/wiki/Business_intelligence) geared towards providing fast ad-hoc querying over a Logical Cube(aka Star-Schema). It simplifies how enterprises can provide
an ad-hoc query layer on top of a **Hadoop/Spark(Big Open Data Stack)**.
* we provide the ad-hoc query capability by extending the Spark SQL
  layer, through SQL extensions and an extended Optimizer(both
  logical and Physical optimizations).
* we use OLAP indexing vs. pre-materialization as a technique to
  achieve query performance. OLAP Indexing is a well-known technique
  that is far superior to materialized views to support ad-hoc
  querying. We utilize another Open-Source Apache licensed Big Data component
  for the OLAP Indexing capability.



![Overall Picture](https://github.com/SparklineData/spark-druid-olap/blob/master/docs/images/images.001.png)
