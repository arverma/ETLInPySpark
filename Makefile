remove_unwanted_files:
	rm -rf build
	rm -rf dist
	rm -rf etlinpyspark.egg-info

build: remove_unwanted_files
	rm -rf dist
	rm -rf *.egg-info
	/usr/bin/python3 setup.py bdist_egg