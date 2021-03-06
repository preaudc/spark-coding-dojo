organization := "com.kelkoogroup.dojo.spark"

name := "spark-coding-dojo"

normalizedName := name.value

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.11.12","2.12.10")

//scala flags are here to have cleaner code
//some may be suppressed in specific cases

// scala 2.11 recommended compilation flags, c.f. https://tpolecat.github.io/2014/04/11/scalac-flags.html
val scalacOptions_2_11 = Seq(
   "-deprecation",
   "-encoding", "UTF-8",       // yes, this is 2 args
   "-feature",
   "-language:existentials",
   "-language:higherKinds",
   "-language:implicitConversions",
   "-unchecked",
   "-Xfatal-warnings",
   "-Xlint",
   "-Yno-adapted-args",
   "-Ywarn-dead-code",        // N.B. doesn't work well with the ??? hole
   "-Ywarn-numeric-widen",
   "-Ywarn-value-discard",
   "-Xfuture",
   "-Ywarn-unused-import"     // 2.11 only
)

//scala 2.12 recommended compilation flags, c.f. https://tpolecat.github.io/2017/04/25/scalac-flags.html
val scalacOptions_2_12 = Seq(
   "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
   "-encoding", "utf-8",                // Specify character encoding used by source files.
   "-explaintypes",                     // Explain type errors in more detail.
   "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
   "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
   "-language:experimental.macros",     // Allow macro definition (besides implementation and application)
   "-language:higherKinds",             // Allow higher-kinded types
   "-language:implicitConversions",     // Allow definition of implicit functions called views
   "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
   "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
   "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
   "-Xfuture",                          // Turn on future language features.
   "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
   "-Xlint:by-name-right-associative",  // By-name parameter of right associative operator.
   "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
   "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
   "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
   "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
   "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
   "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
   "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
   "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
   "-Xlint:option-implicit",            // Option.apply used implicit view.
   "-Xlint:package-object-classes",     // Class or object defined in package object.
   "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
   "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
   "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
   "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
   "-Xlint:unsound-match",              // Pattern match may not be typesafe.
   "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
   "-Ypartial-unification",             // Enable partial unification in type constructor inference
   "-Ywarn-dead-code",                  // Warn when dead code is identified.
   "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
   "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
   "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
   "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
   "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
   "-Ywarn-numeric-widen",              // Warn when numerics are widened.
   "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
   "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
   "-Ywarn-unused:locals",              // Warn if a local definition is unused.
   "-Ywarn-unused:params",              // Warn if a value parameter is unused.
   "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
   "-Ywarn-unused:privates",            // Warn if a private member is unused.
   "-Ywarn-value-discard"               // Warn when non-Unit expression results are unused.
)

def scalacOptionsVersion(scalaVersion: String): Seq[String] = {
   CrossVersion.partialVersion(scalaVersion) match {
      case Some((2,11)) => scalacOptions_2_11
      case Some((2,12)) => scalacOptions_2_12
      case _ => Nil
   }
}

scalacOptions ++= scalacOptionsVersion(scalaVersion.value)

sources in (Compile, doc) := Seq.empty 
publishArtifact in (Compile, packageDoc) := false

val hadoopVersion = "2.8.3"
val sparkVersion = "2.4.6"

parallelExecution in Test := false

libraryDependencies ++= Seq(
   "org.scalatest" %% "scalatest" % "3.0.8" % Test,
   "org.mockito" % "mockito-core" % "3.1.0" % Test,
   "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
   "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test,
   "org.hsqldb" % "hsqldb" % "2.4.0" % Test,
   "org.scalatra" %% "scalatra-scalatest" % "2.6.3" % Test
)
