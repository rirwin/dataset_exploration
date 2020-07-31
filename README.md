TODO:

- take the biz categories from the user biz interactions and create normalized feature vector per user
    re-write or write new user vector.
    look for trending businesses with categories matching to user
- recommendation strategy, collaborative filtering 
- refactor unit & itests 
  - get spark runs twice 
  - split out tests
  - make parquet reader part of itest


ML ideas:
- predict if rfn based on review text
- predict star rating

Search ideas:
- inverted index
- load into es

Data ideas: 
- test joining different column groups
- find a dataset with city, state lat lon append to each business
- one hot encode business categories ie, has_restaurants



create a time dimension, run for year x (and data < x)


Steps for adding a column group:
- try joining with existing df in an interactive session
- print out schema, save it in schemas directory
- regenerate python client to join all column groups
- test client
