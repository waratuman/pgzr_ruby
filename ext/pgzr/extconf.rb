# frozen_string_literal: true

require "mkmf"

abort "missing dlfcn.h" unless have_header("dlfcn.h")

unless /darwin/ =~ RUBY_PLATFORM
  have_library("dl", "dlopen")
end

create_makefile("pgzr/pgzr_ext")
