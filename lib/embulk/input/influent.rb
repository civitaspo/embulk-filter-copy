Embulk::JavaPlugin.register_input(
  "influent", "org.embulk.input.influent.InfluentInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
