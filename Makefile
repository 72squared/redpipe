
help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean           remove temporary files created by build tools"
	@echo "  cleanmeta       removes all META-* and egg-info/ files created by build tools"
	@echo "  cleantox        removes tox files"
	@echo "  cleancov        remove all files related to coverage reports"
	@echo "  cleandocs       remove all files related to docs"
	@echo "  cleanall        all the above + tmp files from development tools"
	@echo "  test            run test suite"
	@echo "  documentation   build documentation"
	@echo "  sdist           make a source distribution"
	@echo "  bdist           make an egg distribution"
	@echo "  install         install package"
	@echo "  tox             run all tox environments and combine coverage report after"

clean:
	-rm -f MANIFEST
	-rm -rf dist/
	-rm -rf build/

cleancov:
	-rm -rf htmlcov/
	-coverage combine
	-coverage erase

cleanmeta:
	-rm -rf redpipe.egg-info/

cleandocs:
	-rm -rf docs/_build

cleanall: clean cleancov cleanmeta cleandocs
	-find . -type f -name "*~" -exec rm -f "{}" \;
	-find . -type f -name "*.orig" -exec rm -f "{}" \;
	-find . -type f -name "*.rej" -exec rm -f "{}" \;
	-find . -type f -name "*.pyc" -exec rm -f "{}" \;
	-find . -type f -name "*.c" -exec rm -f "{}" \;
	-find . -type f -name "*.so" -exec rm -f "{}" \;
	-find . -type f -name "*.parse-index" -exec rm -f "{}" \;
	-rm -rf .tox/

sdist: cleanmeta
	python setup.py sdist

bdist: cleanmeta
	python setup.py bdist_egg

install:
	python setup.py install

local:
	python setup.py build_ext --inplace

documentation:
	pip install sphinx -q
	sphinx-build -M html "./docs" "./docs/_build"

test:
	make tox

tox:
	coverage erase
	tox
	coverage combine
	coverage report

.PHONY: test
