NOECHO = @
TEST_FILES = tests/*.test

test:
	$(NOECHO) prove $(TEST_FILES)
