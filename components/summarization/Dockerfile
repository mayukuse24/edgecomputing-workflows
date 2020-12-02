From python:3
Add summary.py /
RUN pip install nltk
RUN pip install Flask
RUN python -m nltk.downloader 'stopwords'
RUN python -m nltk.downloader 'wordnet'
RUN python -m nltk.downloader 'punkt'
EXPOSE 5000
CMD ["python","./summary.py"]
