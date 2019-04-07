# hardcore-sentiment-classifer
When was the last time you had "create" a sentiment classifier from scratch?

I worked on this project in the summer of 2018 when I was going through Andrew Ng's CS229 video lectures on YouTube.

This classifier uses Naïve Bayes classifer with Laplace smoothing to classify the tweets. All relevent data is stored in the `dictionary.db` database, which is **extremly inefficient**.

To get rolling, you can simply import `main.py` which has three main functions `create_database`, `update_database` and `classify`.

Before you stop your end for the ultimate tweet sentiment classifer, you should read my observations and thoughts on this project and why your quest for that ultimate classifer is far from over...
