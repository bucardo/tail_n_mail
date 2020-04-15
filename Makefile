NOECHO = @
TEST_FILES = tests/*.test

test:
	$(NOECHO) prove $(TEST_FILES)

fulltest:
	$(NOECHO) RELEASE_TESTING=1 prove $(TEST_FILES)
