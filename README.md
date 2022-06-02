# README
## subreddit analysis task
As explained in given pdf I have implemeted data fetching and implementing score calculation logic for subreddits.

##### command to execute luigi local schedular
python -m luigi --module sub_reddit_analysis GetData --local-scheduler --data-file-name "report.csv" --num-top-subreddit 2 --post-limit 10  --comment-limit 5

##### input parameters:
**data-file-name**  name of the file where fetched data and calculated scores are saved in csv file 
**num-top-subreddit** number of top subreddits to be fetched
**post-limit** number of posts for subreddit to be fetched
**comment-limit** number of comments to be fetched

##### caluculated values
postRS and subRS are calulated values in csv

##### python version used
3.7.*
