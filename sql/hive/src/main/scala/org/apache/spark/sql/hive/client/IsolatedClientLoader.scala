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

package org.apache.spark.sql.hive.client

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util

import org.apache.spark.util.Utils
import sun.misc.URLClassPath

import scala.language.reflectiveCalls
import scala.util.Try

import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkSubmitUtils

import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.HiveContext

/** Factory for `IsolatedClientLoader` with specific versions of hive. */
private[hive] object IsolatedClientLoader {
  /**
   * Creates isolated Hive client loaders by downloading the requested version from maven.
   */
  def forVersion(
      version: String,
      config: Map[String, String] = Map.empty): IsolatedClientLoader = synchronized {
    val resolvedVersion = hiveVersion(version)
    val files = resolvedVersions.getOrElseUpdate(resolvedVersion, downloadVersion(resolvedVersion))
    new IsolatedClientLoader(hiveVersion(version), files)
  }

  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
  }

  private def downloadVersion(version: HiveVersion): Seq[URL] = {
    val hiveArtifacts =
      (Seq("hive-metastore", "hive-exec", "hive-common", "hive-serde") ++
        (if (version.hasBuiltinsJar) "hive-builtins" :: Nil else Nil))
        .map(a => s"org.apache.hive:$a:${version.fullVersion}") :+
        "com.google.guava:guava:14.0.1" :+
        "org.apache.hadoop:hadoop-client:2.4.0"

    val classpath = quietly {
      SparkSubmitUtils.resolveMavenCoordinates(
        hiveArtifacts.mkString(","),
        Some("http://www.datanucleus.org/downloads/maven2"),
        None)
    }
    val allFiles = classpath.split(",").map(new File(_)).toSet

    // TODO: Remove copy logic.
    val tempDir = File.createTempFile("hive", "v" + version.toString)
    tempDir.delete()
    tempDir.mkdir()

    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))
    tempDir.listFiles().map(_.toURL)
  }

  private def resolvedVersions = new scala.collection.mutable.HashMap[HiveVersion, Seq[URL]]
}

/**
 * Creates a Hive `ClientInterface` using a classloader that works according to the following rules:
 *  - Shared classes: Java, Scala, logging, and Spark classes are delegated to `baseClassLoader`
 *    allowing the results of calls to the `ClientInterface` to be visible externally.
 *  - Hive classes: new instances are loaded from `execJars`.  These classes are not
 *    accessible externally due to their custom loading.
 *  - ClientWrapper: a new copy is created for each instance of `IsolatedClassLoader`.
 *    This new instance is able to see a specific version of hive without using reflection. Since
 *    this is a unique instance, it is not visible externally other than as a generic
 *    `ClientInterface`, unless `isolationOn` is set to `false`.
 *
 * @param version The version of hive on the classpath.  used to pick specific function signatures
 *                that are not compatibile accross versions.
 * @param execJars A collection of jar files that must include hive and hadoop.
 *
 */
private[hive] class IsolatedClientLoader(
    val version: HiveVersion,
    val execJars: Seq[URL] = Seq.empty)
  extends Logging {

  /** All jars used by the hive specific classloader. */
  protected def allJars = execJars.toArray

  /** The isolated client interface to Hive. */
  val client: ClientInterface = {
    val baseClassLoader: ClassLoader = Utils.getContextOrSparkClassLoader

    try {
      val classLoader: ClassLoader = new URLClassLoader(allJars, baseClassLoader) {
        private val ucp: URLClassPath = new URLClassPath(allJars)

        override def findClass(name: String): Class[_] = {
          val classFileName = name.replace('.', '/').concat(".class")
          val ins = ucp.getResource(classFileName, false)

          if (ins != null) {
            val bytes = IOUtils.toByteArray(ins.getInputStream)
            logDebug(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
            defineClass(name, bytes, 0, bytes.length)
          } else {
            null
          }
        }

        /** The classloader that is used to load an isolated version of Hive metastore. */
        override def loadClass(name: String, resolve: Boolean): Class[_] = {
          var clazz = this.findLoadedClass(name)
          if (clazz == null) {
            clazz = findClass(name)

            if (clazz != null && resolve) {
              resolveClass(clazz)
            }
          }

          if (clazz == null) {
            clazz = super.loadClass(name, resolve)
          }

          clazz
        }
      }

      // Pre-reflective instantiation setup.
      logDebug("Initializing the logger to avoid disaster...")
      Thread.currentThread.setContextClassLoader(classLoader)

      classLoader
        .loadClass("org.apache.spark.sql.hive.client.ClientWrapper")
        .getConstructors.head
        .newInstance(version)
        .asInstanceOf[ClientInterface]
    } catch {
      case ReflectionException(cnf: NoClassDefFoundError) =>
        throw new ClassNotFoundException(
          s"$cnf when creating Hive client using classpath: ${execJars.mkString(", ")}\n" +
            "Please make sure that jars for your version of hive and hadoop are included in the " +
            s"paths passed to ${HiveContext.HIVE_METASTORE_JARS}.")
    } finally {
      Thread.currentThread.setContextClassLoader(baseClassLoader)
    }
  }
}
