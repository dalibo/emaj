# File: Makefile
# E-Maj extension

# This Makefile doesn't use PGXS because PGXS fails to remove all doc and bin
#   files at uninstall time when these files come from subdirectories.
# But it places the components at the same place, as if we had:
# EXTENSION    = emaj
# MODULEDIR    = emaj
# DATA         = $(wildcard sql/*)
# DOCS         = $(wildcard doc/*)
# SCRIPTS      = $(wildcard client/*)
# PG_CONFIG   ?= pg_config
# PGXS := $(shell $(PG_CONFIG) --pgxs)
# include $(PGXS)

PG_CONFIG   ?= pg_config
PG_SHAREDIR := $(shell $(PG_CONFIG) --sharedir)
PG_BINDIR   := $(shell $(PG_CONFIG) --bindir)
PG_DOCDIR   := $(shell $(PG_CONFIG) --docdir)

all:

install:
	mkdir -p $(PG_SHAREDIR)/extension
	mkdir -p $(PG_SHAREDIR)/emaj
	mkdir -p $(PG_DOCDIR)/emaj
	cp sql/* $(PG_SHAREDIR)/emaj/.
	sed -r "s|^#directory\s+=.*$$|directory = 'emaj'|" emaj.control >$(PG_SHAREDIR)/extension/emaj.control
	-cp doc/* $(PG_DOCDIR)/emaj/.
	cp client/* $(PG_BINDIR)/.

uninstall:
	rm -f $(PG_BINDIR)/emaj*
	rm -rf $(PG_DOCDIR)/emaj
	rm -rf $(PG_SHAREDIR)/emaj
	rm -f $(PG_SHAREDIR)/extension/emaj.control
