build:
	rm -rf ./dist && mkdir ./dist
	cp ./main.py ./dist
	cp ./config.json ./dist
	zip -r dist/model.zip model
	zip -r dist/output.zip output
	zip -r dist/resources.zip resources
	pip install -r requirements.txt -t ./dist/libs
	cd ./dist/libs && zip -r -D ../libs.zip .