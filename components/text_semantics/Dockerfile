From python:3
Add main.py /
Copy ./sst-roberta-large-2020.06.08.tar.gz .
RUN pip install nltk
RUN pip install rake-nltk
RUN pip install -U spacy
RUN pip install Flask
RUN python -m nltk.downloader 'stopwords'
RUN pip3 install allennlp==1.0.0 allennlp-models==1.0.0
RUN python -m nltk.downloader 'wordnet'
RUN python -m nltk.downloader 'sentiwordnet'
RUN python -m nltk.downloader 'punkt'
RUN python -m spacy download en_core_web_sm
EXPOSE 5000 
CMD ["python","./main.py"]
