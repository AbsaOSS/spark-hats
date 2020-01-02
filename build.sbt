/*
 * Copyright 2018-2020 ABSA Group Limited
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
val scala212 = "2.12.10"

ThisBuild / organization := "za.co.absa"

ThisBuild / scalaVersion := scala211
ThisBuild / crossScalaVersions := Seq(scala211, scala212)

// Scala shouldn't be packaged so it is explicitly added as a provided dependency below
ThisBuild / autoScalaLibrary := false

lazy val hofs = (project in file("."))
  .settings(
    name := "spark-hats",
    libraryDependencies ++= SparkHatsDependencies :+ getScalaDependency(scalaVersion.value),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    Test / fork := true
  )

// release settings
releaseCrossBuild := true
addCommandAlias("releaseNow", ";set releaseVersionBump := sbtrelease.Version.Bump.Bugfix; release with-defaults")
