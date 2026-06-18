import org.gradle.api.provider.Provider
import org.gradle.plugin.use.PluginDependency

plugins { `kotlin-dsl` }

// A PluginDependency from the catalog -> its marker artifact coordinate, so the
// convention plugins below can apply it with `id("...")` and no version.
fun pluginMarker(p: Provider<PluginDependency>): String =
  p.get().run { "$pluginId:$pluginId.gradle.plugin:${version.requiredVersion}" }

dependencies {
  implementation(pluginMarker(libs.plugins.spotless))
  implementation(pluginMarker(libs.plugins.maven.publish))
  implementation(pluginMarker(libs.plugins.versions))
  // Kotlin must share the buildSrc classloader with the publish plugin above, which
  // inspects the Kotlin plugin when configuring publications.
  implementation(pluginMarker(libs.plugins.kotlin.jvm))

  // Makes the type-safe `libs` accessor available inside precompiled script plugins.
  implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
