venv/bin/activate: requirements.txt
	python -m venv venv
	./venv/bin/pip install -r requirements.txt

run: venv/bin/activate
	./venv/bin/python main.py 

clean: 
	rm -rf __pycache__/
	rm -rf AthenaCPASOperator/__pycache__/
	rm -rf venv