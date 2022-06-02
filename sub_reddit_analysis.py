import luigi
import praw
import pandas as pd

"""
luigi implementation for python logic
"""
class GetData(luigi.Task):

    data_file_name = luigi.Parameter()
    num_top_subreddit = luigi.IntParameter()
    post_limit = luigi.IntParameter()
    comment_limit = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(self.data_file_name)

    def run(self):
        # - configure authentication
        CLIENT_ID = "UG7NpmgOWRHLQy2SoHGChA"
        SECRET = "vsC6ISU5yos9ca71g1F7UWes4-lOMw"
        reddit_read_only = praw.Reddit(client_id=CLIENT_ID,
                                       client_secret=SECRET,
                                       user_agent="My User Agent")

        posts_dict = {"subreddit": [],
                      "post_text": [],
                      "num_comments_on_post": [],
                      "comment_body": [],
                      "comment_score": [],
                      "post_score": []
                      }
        # - get popular subreddits
        top_subreddit = []
        srs = praw.models.Subreddits(reddit_read_only, {})

        for i in srs.popular(limit=self.num_top_subreddit):
            top_subreddit.append(i.display_name)

        # - fetch posts for subreddits
        for sub_reddit in top_subreddit:
            post_count = 0
            for post in reddit_read_only.subreddit(sub_reddit).top(limit=self.post_limit):
                post_score = 0
                post_count += 1
                # - fetch comments for top posts
                url = "https://www.reddit.com" + post.permalink
                # - fetch comments for subreddits
                submission = reddit_read_only.submission(url=url)
                submission.comment_sort = 'score'
                submission.comments.replace_more(limit=0)
                submission.comment_limit = self.comment_limit
                for comm in submission.comments[:self.comment_limit]:
                    post_score = (comm.score / post.num_comments)
                    posts_dict['subreddit'].append(sub_reddit)
                    posts_dict['post_text'].append(post.title)
                    posts_dict['num_comments_on_post'].append(post.num_comments)
                    posts_dict['comment_body'].append(comm.body)
                    posts_dict['comment_score'].append(comm.score)
                    posts_dict['post_score'].append(post_score)

        #- implement logic to calculate subRS and postRS
        top_posts = pd.DataFrame(posts_dict)
        df = top_posts
        sub_df = df.groupby(['subreddit'])
        for subr in sub_df:
            sum_postRS = 0
            post_df = df.groupby(['post_text'])
            for post in post_df:
                num_of_post = post[1].shape[0]
                df.loc[df['subreddit'] == subr[0], 'num_of_post'] = num_of_post
                postRS = sum(post[1]['post_score'])
                df.loc[df['post_text']==post[0] , 'postRS'] = postRS
                sum_postRS += postRS
                df.loc[df['subreddit']==post[1].iloc[0]['subreddit'] , 'subRS'] = sum_postRS/num_of_post

        df.to_csv(self.output().path)
