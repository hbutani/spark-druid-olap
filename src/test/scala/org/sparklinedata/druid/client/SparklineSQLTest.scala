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

package org.sparklinedata.druid.client

import org.apache.spark.sql.hive.test.sparklinedata.TestHive._

class SparklineSQLTest extends BaseTest {

  test("druidrelations") { td =>

    sql("select * from `d$druidrelations`").show(500, false)
  }

  test("druidservers") { td =>

    sql("select * from `d$druidservers`").show(500, false)
  }

  test("druidsegments") { td =>

    sql("select * from `d$druidsegments`").show(500, false)
  }

  test("druidserverassignments") { td =>

    sql("select * from `d$druidserverassignments`").show(500, false)
  }

  test("clearCache") { td =>

    sql("clear druid cache localhost").show()
  }

  test("clearCacheAll") { td =>

    sql("clear druid cache").show()
  }



}
