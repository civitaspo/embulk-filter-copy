Embulk::JavaPlugin.register_input(
  "page", "org.embulk.input.page.PageInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
