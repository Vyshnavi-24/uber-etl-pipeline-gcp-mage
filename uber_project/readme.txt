To create a python virtual environment below are the commands used:
-------------------------------------------------------------------

1.) python -m venv tutorial-env     #It creates a python env with name tutorial-env

2.) source tutorial-env/bin/activate #To activate the python env above command created

3.) python -m pip install requests==2.6.0  #To install any libraries for example requests. If you need pandas just replace pandas and its version

4.) python -m pip freeze > requirements.txt  #It created a requirements.txt file in the folder to make sure all the libraries updated.


