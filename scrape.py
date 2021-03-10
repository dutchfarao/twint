import twint
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


import nltk

nltk.download('wordnet')

from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import re



import string

# Saving in dataframe


def twint_to_pd(columns):
  yield twint.output.panda.Tweets_df[columns]








# read csv

Company_df = pd.read_csv (r'C:\Users\daelsayed\Documents\SentimentAnalysis\Companies\Top500nIndustriesUnitedStates.csv', delimiter=';')
# delete companies that have no ticker symbol
Company_df.dropna(subset = ["AcquirorTickerSymbol"], inplace=True)
i = 0
for i, rowData in Company_df.iterrows():
   try:
      twint.storage.panda.clean()
      TargetName = rowData['Target Name']
      AcquirorName = rowData['Acquiror Name']
      tickersymbol = '$'
      AcquirorTicker = str(tickersymbol) + str(rowData['AcquirorTickerSymbol'])
      AcquirorTicker_nocash = str(rowData['AcquirorTickerSymbol'])
      # # scrape tweets and write csv file for post period
      # c_post = twint.Config()
      # c_post.Search = AcquirorTicker
      # c_post.Since = "2018-01-01 00:00:00"
      # c_post.Until = "2019-01-01 00:00:00"
      # c_post.Limit = 100000
      # c_post.Pandas = True
      # c_post.Hide_output = True
      # twint.run.Search(c_post)
      # post_Tweets_df = twint.storage.panda.Tweets_df
      #
      #
      #
      # # only keep the actual tweets
      # post_Tweets_df = twint.storage.panda.Tweets_df
      # post_Tweets_df = pd.DataFrame(post_Tweets_df['tweet'])
      # print('Columns are now:', post_Tweets_df.columns)
      #
      #
      # # remove url
      # def remove_url(text):
      #    text = re.sub(r"http\S+", '', text)
      #    return text
      #
      #
      # post_Tweets_df['tweet'] = post_Tweets_df['tweet'].apply(lambda x: remove_url(x))
      #
      #
      # def remove_punct(text):
      #    text = "".join([char for char in text if char not in string.punctuation])
      #    text = re.sub('[0-9]+', '', text)
      #    return text
      #
      #
      # post_Tweets_df['tweet'] = post_Tweets_df['tweet'].apply(lambda x: remove_punct(x))
      #
      #
      # def remove_ticker(text):
      #    text = re.sub(AcquirorTicker_nocash, '', text)
      #    return text
      #
      #
      # post_Tweets_df['tweet'] = post_Tweets_df['tweet'].apply(lambda x: remove_ticker(x))
      #
      #
      # # tokenize
      #
      # def tokenize(text):
      #    tokens = re.split("\W+", text)
      #    return tokens
      #
      #
      # post_Tweets_df['tweet'] = post_Tweets_df['tweet'].apply(lambda x: tokenize(x.lower()))
      #
      #
      # # remove stopwords
      #
      # def remove_stopword(text):
      #    nltk.download('stopwords')
      #    stopword = stopwords.words('english')
      #    text_nostopword = [word for word in text if word not in stopword]
      #    return text_nostopword
      #
      #
      # post_Tweets_df['tweet'] = post_Tweets_df['tweet'].apply(lambda x: remove_stopword(x))
      #
      #
      # # stemming
      #
      # def stem(tweet_no_stopword):
      #    nltk.download('wordnet')
      #    wn = WordNetLemmatizer()
      #    text = [wn.lemmatize(word) for word in tweet_no_stopword]
      #    return text
      #
      #
      # post_Tweets_df['tweet'] = post_Tweets_df["tweet"].apply(lambda x: stem(x))
      # print(len(post_Tweets_df.index))

      # scrape tweets and write csv file for pre period
      c_pre = twint.Config()
      c_pre.Search = AcquirorTicker
      c_pre.Since = "2015-01-01 00:00:00"
      c_pre.Until = "2016-01-01 00:00:00"
      c_pre.Limit = 10000
      c_pre.Pandas = True
      c_pre.Hide_output = True
      twint.run.Search(c_pre)

      # only keep the actual tweets
      pre_Tweets_df = twint.storage.panda.Tweets_df
      pre_Tweets_df = pd.DataFrame(pre_Tweets_df['tweet'])
      print('Columns are now:', pre_Tweets_df.columns)


      # remove url
      def remove_url(text):
         text = re.sub(r"http\S+", '', text)
         return text


      pre_Tweets_df['tweet'] = pre_Tweets_df['tweet'].apply(lambda x: remove_url(x))


      def remove_punct(text):
         text = "".join([char for char in text if char not in string.punctuation])
         text = re.sub('[0-9]+', '', text)
         return text

      pre_Tweets_df['tweet'] = pre_Tweets_df['tweet'].apply(lambda x: remove_punct(x))

      def remove_ticker(text):
         text = re.sub(AcquirorTicker_nocash, '', text)
         return text

      pre_Tweets_df['tweet'] = pre_Tweets_df['tweet'].apply(lambda x: remove_ticker(x))

      #tokenize

      def tokenize(text):
         tokens = re.split("\W+", text)
         return tokens


      pre_Tweets_df['tweet'] = pre_Tweets_df['tweet'].apply(lambda x: tokenize(x.lower()))



      # remove stopwords

      def remove_stopword(text):
         nltk.download('stopwords')
         stopword = stopwords.words('english')
         text_nostopword = [word for word in text if word not in stopword]
         return text_nostopword
      pre_Tweets_df['tweet']= pre_Tweets_df['tweet'].apply(lambda x: remove_stopword(x))

      # stemming

      def stem(tweet_no_stopword):
         nltk.download('wordnet')
         wn = WordNetLemmatizer()
         text = [wn.lemmatize(word) for word in tweet_no_stopword]
         return text


      pre_Tweets_df['tweet'] = pre_Tweets_df["tweet"].apply(lambda x: stem(x))
      min_length = int(len(pre_Tweets_df.index))
      print(min_length)

   # write file only if there are enough tweets
      if min_length > 200:
         # write pre file
         filename_pre = r"C:\Users\daelsayed\Documents\SentimentAnalysis\test\_" + AcquirorName + "_ticker__PRE.csv"
         pre_Tweets_df.to_csv(filename_pre)
         i += 1




      # add to iteration

   except Exception as e:
      #re-iterate after exception
      pass

