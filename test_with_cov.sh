coverage run -m unittest discover test -p *_test.py
coverage report --include=*wario* --omit=*test*
