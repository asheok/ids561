#!/usr/bin/env python
# coding: utf-8

# # Task 1

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window
import pandas as pd # our main data management package
import string # used for preprocessing
import re # used for preprocessing
import numpy as np # used for managing NaNs


# In[2]:


df=spark.read.csv('gs://buck_561/ass2/Amazon_Responded_Oct05.csv',header=True, inferSchema=True)## reading csv file
df.count(),df.show(2,False)


# In[3]:


df1=df.select('id_str', 'tweet_created_at', 'user_verified', 'favorite_count', 'retweet_count', 'text_').withColumn("year",F.substring(F.col("tweet_created_at"),27,7))## selecting the required 6 columns and exracting year from tweet_created_at column
df1.show(5,False),df1.count()


# In[4]:


df1.filter('year != "2016"').show(5,False) ## noise data withoutanyy value


# In[5]:


df2=df1.filter('year="2016"') ## filtering out the noise data
df2.show(5),df2.count()


# ### Step 1

# In[6]:


df3=df2.filter(F.col('user_verified') == "True") # filtering out the verified users
df3.show(5),df3.count()


# In[7]:


df3=df3.withColumn("date",F.substring(F.col("tweet_created_at"),5,6)) ## extracting date from tweet_created_at column
df3.count(),df3.show(5)


# ### Step 2

# In[8]:


df4=df3.groupBy('date').agg(F.count('id_str').alias('num_tweets')).orderBy('num_tweets',ascending=False) ## counting number of tweets per date by grouping on date
df4.count(),df4.show(5)


# In[9]:


dtt=df4.select('date').collect()[0]['date'] ## extracting the date with highest number of tweets
dtt


# In[10]:


df5=df3.filter(F.col('date')==dtt) ## filtering the data with date having highest number of tweets
df5.count(),df5.show(5)


# In[12]:


w = Window().orderBy(F.desc('total'))
df6=df5.withColumn('total',F.coalesce(F.col('favorite_count'),F.lit(0)) + F.coalesce(F.col('retweet_count'),F.lit(0))).orderBy('total',ascending=False).withColumn("index", F.row_number().over(w)).filter('index <= 100')  ## using coalesce to relace any null value with 0  ## assigning values to index column in order with their rank  ## filtering top 100 tweets having maximum sum of favorit_count and retweet_count 
df6.count(),df6.show(5)


# In[13]:


# remove urls, handles, and the hashtag from hashtags (taken from https://stackoverflow.com/questions/8376691/how-to-remove-hashtag-user-link-of-a-tweet-using-regular-expression)
def remove_urls(text):
    new_text = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",text).split())
    return new_text

#make all text lowercase
def text_lowercase(text):
    return text.lower()

# remove numbers
def remove_numbers(text):
    result = re.sub(r'\d+', '', text)
    return result

# remove punctuation
def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)

# defining preprocessing function to combine all above functions to basic clean data
def preprocessing(text):
    text = text_lowercase(text)
    text = remove_urls(text)
    text = remove_numbers(text)
    text = remove_punctuation(text)
    text = ''.join(text)
    return text


# In[14]:


df7=df6.toPandas() ## converting spark dataframe to pandas dataframe to use preprocesisng function
print(df7.shape)
df7.head()


# In[15]:


pp_text = [] # our preprocessed text column
for text_data in df7['text_']:
    pp_text_data = preprocessing(text_data) ## using preprocesing function
    pp_text.append(pp_text_data)
df7['text_'] = pp_text # add the preprocessed text as a column
print(df7.shape)
df7.head()


# In[16]:


df8=spark.createDataFrame(df7) ## converting the clean pandas dataset back to pyspark dataset
df8.show(5),df8.count()


# In[17]:


asd=df8.select('text_').collect() ## collecting all the text values
asd[:5]


# In[18]:


a=[]
for row in asd:
    a.append(row['text_']) ## combining all the rows of text to a single list
a[:5]


# In[19]:


xx=' '.join(a)
xx=xx.split() ## spliting the sentences' list to words' list
xx[:10]


# In[20]:


yy=[]
for a in xx:
    yy.append([a,1]) ## combining words with 1 (mapping with 1)
yy[:10] 


# In[21]:


df9=spark.createDataFrame(yy,schema=['Word','Count']) # creating the dataframe from list
df9.show(5),df9.count()


# ### Step 3

# In[22]:


df10=df9.groupBy('Word').agg(F.sum('count').alias('Frequency')) ## aggregating the dataset to find words frequency
df10.show(5),df10.count()


# In[23]:


df10.coalesce(1).write.mode('overwrite').csv('gs://buck_561/ass2/find_text_final.csv',header=True) ## writing csv file to google cloud storage by coalescing into 1 file


# # Task 2

# In[24]:


dff=spark.read.csv('gs://buck_561/ass2/find_text.csv',header=True, inferSchema=True) ## reading second file with exmpty text column
dff.count(),dff.show(2)


# In[25]:


dff1=dff.join(df2,'id_str').select('id_str','text_') ## joining df2(cleaned by filters only) with dff to import text values
dff1.count(),dff1.show(2)


# In[ ]:




