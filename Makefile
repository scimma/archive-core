.PHONY: help
help :
	@echo
	@echo 'Commands:'
	@echo
	@echo '  make test                  run unit tests'
	@echo

.PHONY: test
test :
	python -m pytest -v --cov=archive --cov-report=term-missing $(PYTEST_ARGS) tests