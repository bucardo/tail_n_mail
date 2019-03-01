NOECHO = @
TEST_FILES = t/*.t

test:
	$(NOECHO) prove $(TEST_FILES)
