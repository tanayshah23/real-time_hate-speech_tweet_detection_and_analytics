import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from pprint import pprint
import socket
import json

consumer_key    = ''
consumer_secret = ''
access_token    = ''
access_secret   = ''

class TweetsListener(StreamListener):
  def __init__(self, csocket):
    self.client_socket = csocket
  def on_data(self, data):
    try:  
      msg = json.loads( data )
      if "extended_tweet" in msg:
        splitter = " splitterT23 "
        user_id = msg["id_str"]
        message = ""
        try:
          message = msg["retweeted_status"]['extended_tweet']['full_text']
        except:
          message = msg['extended_tweet']['full_text']

        message = ' '.join(message.split())
        followers_count = str(msg["user"]["followers_count"])
        try:
          actual_tweeter = msg["retweeted_status"]["id_str"]
        except:
          actual_tweeter = str(None)

        message_string = user_id+splitter+message+splitter+followers_count+splitter+actual_tweeter
        
        self.client_socket\
            .send((message_string+"t_end\n")\
            .encode('utf-8'))         
        print(message_string)
      else:
        message = ""
        try:
          message = msg["retweeted_status"]['extended_tweet']['full_text']
        except:
          message = msg['text']
        message = ' '.join(message.split())
        splitter = " splitterT23 "
        user_id = msg["id_str"]
        followers_count = str(msg["user"]["followers_count"])
        try:
          actual_tweeter = msg["retweeted_status"]["id_str"]
        except:
          actual_tweeter = str(None)

        message_string = user_id+splitter+message+splitter+followers_count+splitter+actual_tweeter
        self.client_socket\
            .send((message_string+"t_end\n")\
            .encode('utf-8'))
        print(message_string)
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True
  def on_error(self, status):
    print(status)
    return True

def sendData(c_socket, keyword):
  print('start sending data from Twitter to socket')
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track = keyword, languages=["en"])


if __name__ == "__main__":
    s = socket.socket()
    host = "0.0.0.0"    
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    s.listen(4)
    print('socket is listening')
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    sendData(c_socket, keyword = ['parliament','elections','government'])
