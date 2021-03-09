import twint
import pandas as pd

# read csv

Company_df = pd.read_csv (r'C:\Users\daria\OneDrive\Documenten\SentimentAnalysis\Companies\Top500nIndustriesUnitedStates.csv', delimiter=';')
# delete companies that have no ticker symbol
Company_df.dropna(subset = ["AcquirorTickerSymbol"], inplace=True)
i = 0
for i, rowData in Company_df.iterrows():
   try:
      TargetName = rowData['Target Name']
      AcquirorName = rowData['Acquiror Name']
      tickersymbol = '$'
      AcquirorTicker = str(tickersymbol) + str(rowData['AcquirorTickerSymbol'])

      # scrape tweets and write csv file for post period
      c_post = twint.Config()
      c_post.Search = AcquirorTicker
      c_post.Since = "2018-01-01 00:00:00"
      c_post.Until = "2019-01-01 00:00:00"
      c_post.Limit = 100000
      c_post.Pandas = True
      twint.run.Search(c_post)
      post_Tweets_df = twint.storage.panda.Tweets_df

      # write file
      filename = r"C:\Users\daria\OneDrive\Documenten\SentimentAnalysis\CSVoutputsPOST_ticker\_" + AcquirorName + "_ticker__POST.csv"
      post_Tweets_df.to_csv(filename)

      # scrape tweets and write csv file for pre period
      c_pre = twint.Config()
      c_pre.Search = AcquirorTicker
      c_pre.Since = "2015-01-01 00:00:00"
      c_pre.Until = "2016-01-01 00:00:00"
      c_pre.Limit = 100000
      c_pre.Pandas = True
      twint.run.Search(c_pre)
      pre_Tweets_df = twint.storage.panda.Tweets_df

      # write file
      filename = r"C:\Users\daria\OneDrive\Documenten\SentimentAnalysis\CSVoutputsPRE_ticker\_" + AcquirorName + "_ticker__PRE.csv"
      pre_Tweets_df.to_csv(filename)

      # add to iteration
      i+=1
   except Exception as e:
      #re-iterate after exception
      pass

