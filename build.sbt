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

import Dependencies._

val scala211 = "2.11.12"
val scala212 = "2.12.18"
val scala213 = "2.13.11"

ThisBuild / organization := "za.co.absa"

ThisBuild / scalaVersion := scala212
ThisBuild / crossScalaVersions := Seq(scala211, scala212, scala213)

ThisBuild / scalacOptions := Seq("-unchecked", "-deprecation")

// Scala shouldn't be packaged so it is explicitly added as a provided dependency below
ThisBuild / autoScalaLibrary := false

lazy val printSparkVersion = taskKey[Unit]("Print Spark version spark-cobol is building against.")

lazy val hats = (project in file("."))
  .settings(
    name := "spark-hats",
    printSparkVersion := {
      val log = streams.value.log
      val effectiveSparkVersion = sparkVersion(scalaVersion.value)
      log.info(s"Building with Spark $effectiveSparkVersion")
      effectiveSparkVersion
    },
    Compile / compile := ((Compile / compile) dependsOn printSparkVersion).value,
    Compile / unmanagedSourceDirectories += {
      val sourceDir = (Compile / sourceDirectory).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n == 11 => sourceDir / "scala_2.11"
        case Some((2, n)) if n == 12 => sourceDir / "scala_2.12"
        case Some((2, n)) if n == 13 => sourceDir / "scala_2.13"
        case _ => throw new RuntimeException("Unsupported Scala version")
      }
    },
    libraryDependencies ++= getSparkHatsDependencies(scalaVersion.value) ++ getHofsDependency(scalaVersion.value) :+ getScalaDependency(scalaVersion.value),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    Test / fork := true
  ).enablePlugins(AutomateHeaderPlugin)

// release settings
releaseCrossBuild := true
addCommandAlias("releaseNow", ";set releaseVersionBump := sbtrelease.Version.Bump.Bugfix; release with-defaults")

// JaCoCo code coverage
Test / jacocoReportSettings := JacocoReportSettings(
  title = s"spark-hats Jacoco Report - ${scalaVersion.value}",
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

// exclude example
Test / jacocoExcludes := Seq(
//  "za.co.absa.spark.hats.transformations.NestedArrayTransformation*", // class and related objects
//  "za.co.absa.spark.hats.transformations.ArrayContext" // class only
)
