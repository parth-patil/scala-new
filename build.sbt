organization  := "com.parthpatil"

version       := "0.1"

scalaVersion  := "2.11.5"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "rediscala" at "http://dl.bintray.com/etaty/maven"

libraryDependencies ++= {
  val akkaV = "2.3.+"
  Seq(
    "com.typesafe.akka"   %% "akka-actor"           % akkaV,
    "com.typesafe.akka"   %% "akka-testkit"         % akkaV   % "test",
    "com.typesafe.akka"   %% "akka-contrib"         % akkaV,
    "org.scalacheck"      %% "scalacheck"           % "1.12.2",
    "io.reactivex"        %% "rxscala"              % "0.23.0",
    "org.json4s"          %% "json4s-jackson"       % "3.2.11",
    "org.scala-lang.modules" %% "scala-async" % "0.9.2",
    "org.slf4j"           %  "slf4j-simple"         % "1.7.+", // slf4j implementation
    "ch.qos.logback"      %  "logback-classic"      % "1.1.+",
    "commons-io"          %  "commons-io"           % "2.4",
    "biz.paluch.redis"    %  "lettuce"              % "3.1.Final",
    "commons-codec"       %  "commons-codec"        % "1.10",
    "com.google.guava"    %  "guava"                % "18.0",
    "org.roaringbitmap"   %  "RoaringBitmap"        % "0.4.9"
  )
}

retrieveManaged := true
