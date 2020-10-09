import librosa
import statistics
import numpy as np
import matplotlib as plt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models
from tensorflow.keras.models import Sequential
from tensorflow.keras import layers
from tensorflow.keras.layers import Dense
import nltk
from nltk.corpus import sentiwordnet as swn
from rake_nltk import Rake
import re
import spacy
import json
from flask import Flask, request,jsonify
import sklearn
from sklearn.metrics import mean_squared_error

app=Flask(__name__)

def audio_tone_val(file):
    np.random.seed(33)
    #filename = "angry_test.wav"#librosa.example('angry_test.wav')
    y, sr = librosa.load(file)

    tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr)
    beat_times = librosa.frames_to_time(beat_frames, sr=sr)
    b_trips = [beat_times[i:i+3] for i in range(0,len(beat_times), 3) ]
    sd = statistics.pstdev(beat_times)
    nd = sklearn.cluster.k_means(b_trips,n_clusters=4)
    #print(nd)
    #don't ask
    try:
        mse = mean_squared_error(beat_times,[0])
    except:
        mse=-2
    bb = np.asarray(b_trips)
    data = bb[:, [0,1]]
    res = bb[:,2]
    model = Sequential()
    model.add(Dense(32,activation='sigmoid',input_dim=2))
    model.add(Dense(32, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))
    model.compile(optimizer='rmsprop',loss='binary_crossentropy',metrics=['accuracy'])
    model.fit(data,res,epochs=5,batch_size=32)
    pred = model.predict(data)
    #print(pred)
    #tm = keras.applications.Xception()
    #imp = keras.Input(shape=(2,))


    #print(tempo,beat_times,sd,mse)
    return tempo,beat_times,sd,mse

@app.route('/audio_analysis',methods=["POST",'PUT'])
def app_manager():
    try:
        print(request.files)
        data = request.files["file"]
       # data = content["data"]
    except KeyError:
        return jsonify({"error": "Bad key for data passed to find keywords"})
    tmp,t2,t3,t4 =audio_tone_val(data)
    return jsonify({"Tone Info": str(tmp),"Val2":str(t2),"Val3":str(t3),"Val4":str(t4)})



if __name__ == '__main__':
    #audio_tone_val('PyCharm')
    app.run(host="0.0.0.0", port=5005)


