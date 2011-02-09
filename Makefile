.PHONY: all
all:

.PHONY: clean
clean:
	find . -name '*.pyc' | xargs -n 1 -- rm -f
