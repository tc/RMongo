.onLoad <- function(libname, pkgname) {
  .jpackage(pkgname, lib.loc = libname)
}

library('rjson')
