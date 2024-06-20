# File: Makefile
# E-Maj extension

EXTENSION    = emaj
MODULEDIR    = emaj
DATA         = $(wildcard sql/*)
SCRIPTS      = $(wildcard client/*)
ifneq ($(wildcard doc/*),) 
DOCS         = $(wildcard doc/*)
endif
PG_CONFIG   ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Additional target to remove bin and docs files, which PGXS does not do.
uninstall: uninstall_more

uninstall_more:
	rm -f $(DESTDIR)$(shell $(PG_CONFIG) --bindir)/emaj*
	rm -rf $(DESTDIR)$(shell $(PG_CONFIG) --docdir)/emaj
