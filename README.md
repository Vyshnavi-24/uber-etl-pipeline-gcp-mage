# uber-etl-pipeline-gcp-mage


Step 1:
To create a Python virtual environment below are the commands used:
-------------------------------------------------------------------

1.) python -m venv tutorial-env     #It creates a python env with the name tutorial-env

2.) source tutorial-env/bin/activate #To activate the python env above command created

3.) python -m pip install requests==2.6.0  #To install any libraries for example requests. If you need pandas just replace pandas and its version

4.) python -m pip freeze > requirements.txt  #It created a requirements.txt file in the folder to make sure all the libraries are updated.

Step 2:
Information about the project:
------------------------------

1.) Download the data from the link "data/uber_data.csv"

2.) Create a project and move to virtual enev.

3.) Write the code with the help of Lucid with the below link which helps in the pictorial representation of star schema method

Link to lucid:
https://lucid.app/documents/view/0b4a944f-cd17-436f-b593-0b63e596da02

4.) Code is available in the Uber_project

5.) Create a Google Cloud Platform, create the instance with public access,  upload data, and create the bucket.

6.) Open the SSH terminal mv the file to the virtual machine and connect to the mage-ai with external IP and port number

Step 3:
Commands to install and set up in virtual machine:
--------------------------------------------------
# Install Python and pip 

sudo apt-get install update

sudo apt-get install python3-distutils

sudo apt-get install python3-apt

sudo apt-get install wget

wget https://bootstrap.pypa.io/get-pip.py

sudo python3 get-pip.py

# Install Mage
sudo pip3 install mage-ai

# Install Pandas
sudo pip3 install pandas

# Install Google Cloud Library to VM
sudo pip3 install google-cloud

sudo pip3 install google-cloud-bigquery

Step 4:
Final steps:
------------ 

1.) Create the Extract, Transform and Load.

2.) Load the data into the mage-ai and run the code.

3.) Check the destination BigQuery all the data has loaded.

4.) Apply the queries to a big query and produce the output.

5.) Finally create the dashboard by using the Looker.

Link to Looker:
https://lookerstudio.google.com/s/q_i8Twr0vIg
