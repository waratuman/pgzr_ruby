# frozen_string_literal: true

require "rbconfig"
require "rake/testtask"

EXT_DIR = File.expand_path("ext/pgzr", __dir__)
EXT_LIB = File.join(EXT_DIR, "pgzr_ext.#{RbConfig::CONFIG.fetch("DLEXT")}")

file EXT_LIB => FileList["ext/pgzr/extconf.rb", "ext/pgzr/pgzr_ext.c"] do
  sh RbConfig.ruby, "extconf.rb", chdir: EXT_DIR
  sh "make", chdir: EXT_DIR
end

task :compile => EXT_LIB

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

task test: :compile

task default: :test
