Embulk::JavaPlugin.register_filter(
  "copy", "org.embulk.filter.copy.CopyFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))

Embulk::JavaPlugin.register_input(
    "internal_forward",
    "org.embulk.filter.copy.plugin.InternalForwardInputPlugin",
    File.expand_path('../../../../classpath', __FILE__))