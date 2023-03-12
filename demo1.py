from visa.exception import CustomException
from visa.loger import logging
from flask import Flask
import os,sys

app = Flask(__name__)

@app.route('/',methods = ['GET','POST'])
def index():
    try:
        #raise Exception("we are testing custom exception")
        return "sumit"
    except Exception as e:
        visa = CustomException(e,sys)
        logging.info(visa.error_message)
        logging.info("we are testing loggingfile")
        return "Hello World"

if __name__ == "__main__":
    app.run(debug = True)