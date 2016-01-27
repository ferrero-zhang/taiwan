# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class TwitterItem(Item):
    # define the fields for your item here like:
    # name = Field()
    screen_name = Field()
    text = Field()
    user = Field()
    created_at = Field()
    retweet_count = Field()
    favourites_count = Field()
    keyword = Field()
    id_tweet = Field()
    first_in = Field()
    last_modify = Field()
    lang = Field()
    candidate = Field()

    RESP_TWEET_WEB = ['screen_name','text','created_at','user','id_tweet','lang','candidate','retweet_count','favourites_count']

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if isinstance(v, TwitterItem):
                d[k] = v.to_dict()
            else:
                d[k] = v
        return d

