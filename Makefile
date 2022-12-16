remove_unwanted_files:
	rm -r build
	rm -r dist
	rm -r etlinpyspark.egg-info

build: remove_unwanted_files
	rm -rf dist
	rm -rf *.egg-info
	/usr/bin/python3 setup.py bdist_egg