Embulk::JavaPlugin.register_input(
  "copy", "org.embulk.input.copy.CopyInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
