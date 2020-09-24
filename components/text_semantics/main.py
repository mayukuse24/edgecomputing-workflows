import nltk
from allennlp.predictors.predictor import Predictor
import allennlp_models.classification
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import sentiwordnet as swn
from rake_nltk import Rake
import re
import spacy
import json
from flask import Flask, request,jsonify

app=Flask(__name__)

pred = Predictor.from_path("https://storage.googleapis.com/allennlp-public-models/sst-roberta-large-2020.06.08.tar.gz")


def get_keywords(text):
    """
    get a list of keywords and phrases from the given text
    :param text: a string of the text to be processed
    :return: formatted word list and formatted string
    """

    cy_model = spacy.load("en_core_web_sm")

    text_words = re.sub("[\"\'\,~`_0$%\-@<>\{\}\\/#\$\^&*\n\t]", " ", text)
    r = Rake()
    r.extract_keywords_from_text(text)
    kw = r.get_ranked_phrases()
    kw_selected = kw
    #temp = []
    for k in kw:
        doc = cy_model(k)
        flag = False
        for w in doc:
            if w.tag_ in ["NNP","VBG","NN","FW","NNPS","NNS","PRP","PRP$","VBD","VBN","VBP","VBZ"]:
                flag=True
                break
        if not flag:
            kw_selected = [a for a in kw_selected if a != k]
    #print(kw)

    text_words = text_words.lstrip()
    remove_words = stopwords.words('english')
    sentences = sent_tokenize(text_words)
    sentence_sentiments =[]
    chunks =[]
    for sent in sentences:
        #print(sent,0)
        sentence_sentiments += [pred.predict(sent)['probs']]
        chunks += [[c for c in word_tokenize(sent) if c.lower() not in remove_words and len(c) > 1]]
    keywords = []
    count =0
    for c in chunks:
        doc = cy_model(sentences[count])
        count+=1
        #print(c)
        sect ={}
        for w in c:
            pos = ""
            for token in doc:
                if str.lower(token.text) == str.lower(w):
                    pos = token.pos_
            if pos in ["INTJ","ADV","ADJ"]:
                continue
            sent_pos =0
            sent_neg =0
            for sn in swn.senti_synsets(w):
                if sn.neg_score() >.2 and sn.pos_score()< sn.neg_score():
                    sent_neg += sn.neg_score()
                elif sn.pos_score() >.2 and sn.pos_score()> sn.neg_score():
                    sent_pos += sn.pos_score()
            sect[w] =(pos,sent_pos,sent_neg)
        keywords+= [sect]#[{w:(nltk.pos_tag(w),swn.senti_synsets(w)) for w in c }]
    #print(sentence_sentiments)
    #print(keywords)
    #print(kw_selected)
    return keywords,sentence_sentiments,kw_selected

@app.route("/text_keywords",methods=['POST'])
def component_interface():
    content = request.get_json(force=True)
    #print(content)
    #print(request)
    try:
        data = content["data"]
    except KeyError:
        return jsonify({"error":"Bad key for data passed to find keywords"})

    #text_data = json.dumps(data).encode('utf-8')
    print(data)
    kw,ss,pg_kw=get_keywords(data)
    return jsonify({"Sentence Key Words":kw,"Sentence Sentiments":ss,"Paragraph Key phrase":pg_kw})


if __name__ == '__main__':
    app.run(debug=True)
    #get_keywords(" The cat stretched. Jacob stood on his tiptoes.::$%$ The car turned the corner. Kelly twirled in circles. She opened the door. Aaron made a picture.\n  I'm sorry. I danced.\n   Sarah and Ira drove to the store.  Jenny and I opened all the gifts.  The cat and dog ate.  My parents and I went to a movie.  Mrs. Juarez and Mr. Smith are dancing gracefully.  Samantha, Elizabeth, and Joan are on the committee.   The mangy, scrawny stray dog hurriedly gobbled down the grain-free, organic dog food.  I quickly put on my red winter jacket, black snow pants, waterproof boots, homemade mittens, and handknit scarf.  The incessant ticking and chiming echoed off the weathered walls of the clock repair shop.  Nervously, I unfolded the wrinkled and stained letter from my long-dead ancestor.  Into the suitcase, I carelessly threw a pair of ripped jeans, my favorite sweater from high school, an old pair of tube socks with stripes, and $20,000 in cash. The ham, green beans, mashed potatoes, and corn are gluten-free.  The paper and pencil sat idle on the desk.")


