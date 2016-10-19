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

package org.apache.spark.sql.hive.sparklinedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection

object StuffReflect {

  import ScalaReflection.universe
  import ScalaReflection.mirror

  def changeSessionStateClass : Unit = {
    val spkSessionCSymbol = mirror.classSymbol(classOf[SparkSession])
    val spkSessionModSymbol = spkSessionCSymbol.companionSymbol.asModule
    val spkSessionModMirror = mirror.reflectModule(spkSessionModSymbol)
    val spkSessionModule = spkSessionModMirror.instance
    val spkSessionModuleMirror = mirror.reflect(spkSessionModMirror)
    val spkSessionModuleTyp = spkSessionModuleMirror.symbol.selfType
    val termSessionState = spkSessionModuleTyp.declaration(
      universe.newTermName("HIVE_SESSION_STATE_CLASS_NAME")).asTerm.accessed.asTerm
    val sessionStateField = spkSessionModuleMirror.reflectField(termSessionState)
    sessionStateField.set("org.apache.spark.sql.hive.sparklinedata.SPLSessionState")
  }

}