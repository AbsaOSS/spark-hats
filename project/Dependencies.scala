/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

object Dependencies {

  val defaultSparkVersionForScala211 = "2.4.8"
  val defaultSparkVersionForScala212 = "3.3.2"
  val defaultSparkVersionForScala213 = "3.4.1"

  private val sparkHofsVersion = "0.4.0"
  private val scalatestVersion = "3.2.14"

  def getScalaDependency(scalaVersion: String): ModuleID = "org.scala-lang" % "scala-library" % scalaVersion % Provided

  def getSparkHatsDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    // provided
    "org.apache.spark" %% "spark-core"       % sparkVersion(scalaVersion) % Provided,
    "org.apache.spark" %% "spark-sql"        % sparkVersion(scalaVersion) % Provided,
    "org.apache.spark" %% "spark-catalyst"   % sparkVersion(scalaVersion) % Provided,

    // test
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  )

  def getHofsDependency(scalaVersion: String): Seq[ModuleID] = if (scalaVersion.startsWith("2.11.")) {
    Seq("za.co.absa" %% "spark-hofs" % sparkHofsVersion)
  } else {
    Seq.empty
  }

  def sparkVersion(scalaVersion: String): String = sys.props.getOrElse("SPARK_VERSION", sparkFallbackVersion(scalaVersion))

  def sparkFallbackVersion(scalaVersion: String): String = {
    if (scalaVersion.startsWith("2.11.")) {
      defaultSparkVersionForScala211
    } else if (scalaVersion.startsWith("2.12.")) {
      defaultSparkVersionForScala212
    } else if (scalaVersion.startsWith("2.13.")) {
      defaultSparkVersionForScala213
    } else {
      throw new IllegalArgumentException(s"Scala $scalaVersion not supported.")
    }
  }
}
