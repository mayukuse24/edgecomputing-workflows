From python:3
Add main.py /
RUN pip install -U SoundFile
RUN pip install -U cffi
RUN apt-get update 
RUN apt-get upgrade -y 
RUN apt-get install -y
RUN apt-get -y install libsndfile1 
RUN pip install -U nltk
RUN pip install -U rake-nltk
RUN pip install -U matplotlib
RUN pip install -U spacy
RUN pip install -U Flask
RUN pip install -U numpy
RUN pip install -U librosa
RUN pip install -U tensorflow
RUN pip install -U keras
RUN pip install -U numpy
RUN pip install -U sklearn
EXPOSE 5005 
CMD ["python","./main.py"]
