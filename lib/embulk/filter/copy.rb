Embulk::JavaPlugin.register_filter(
  "copy", "org.embulk.filter.copy.CopyFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
