from sshtunnel import SSHTunnelForwarder
from snownlp import SnowNLP
import jieba.analyse
import pymongo
from bson import ObjectId
import re

# 通过ssh跳板登陆mongo
def get_mongodb_client():
    ssh_address_or_host = ("39.105.96.64",22) ##服务器地址与ssh_port
    ssh_username = 'root' ##登录服务器的用户
    ssh_password = 'Mongo729' ##登录服务器的密码
    remote_bind_address = ('172.17.241.114', 27017)
    #mongo_user = 'xxxx'  ## 访问数据库的用户名
    # mongo_password = 'xxxxx'  #访问数据库的密码
    server = SSHTunnelForwarder(
        ssh_address_or_host=ssh_address_or_host,
        ssh_username = ssh_username,
        ssh_password = ssh_password ,
        remote_bind_address = remote_bind_address)
    server.start()
    ## 这里一定要填入ssh映射到本地的端口 # 通过xshell端口转发mongdb端口到本地
    client = pymongo.MongoClient('127.0.0.1',8888)
    db = client.admin
    return client
client = get_mongodb_client()
db = client.aituwen

# 评论情感分析
def sentiment(sentence):
    sentence = sentence
    if sentence != None:
        score = SnowNLP(sentence).sentiments
    else:
        score = 0
    return score

"""得到user_likes"""
likes = db['likes']

author_id = {}
#key:author_id;value:target_id
for x in likes.find({}, {"author_id":1,"target_id":1}):
    author = x.get("author_id")
    target = x.get("target_id")
    if author_id.__contains__(str(author)):
        author_id[str(author)].append(target)
    else:
        author_id[str(author)] = [target]
# 写入user_likes
user_likes = db['user_likes']
for key,value in author_id.items():
    dic = {'author_id':ObjectId(key),'likes':value}
    user_likes.insert_one(dic)
print('user_like is done')

"""得到user_comment"""

comments = db['comments']
num = comments.count()
print(num)
author_id = {}
#key:author_id;value:target_id,content

for x in comments.find({}, {"author_id":1,"target_id":1,"content":1}):
    author = x.get("author_id")
    target = x.get("target_id")
    content = x.get("content")
    comment = {'post_id': target, 'content': content}
    if author_id.__contains__(str(author)):
        author_id[str(author)].append(comment)
    else:
        author_id[str(author)] = [comment]
# 写入user_comment表
user_comment = db['user_comment']
for key,value in author_id.items():
    dic = {'author_id':ObjectId(key),'comment':value}
    user_comment.insert_one(dic)

print('user_comment is done')

"""通过user_likes建立用户user_like_tags"""

# 导入标签库——place
place = {}
file = open('place.txt',encoding = 'utf-8')
for line in file.readlines():
    line = line.strip()
    place[line] = 1
posts = db['posts']
user_tags = db['user_likes_tags']
author_id = {}
id_label = {}
topK = 20
post_tags = {}
#key:author_id;value:likes
# 每一个用户
try:
    for x in user_likes.find({}, {"author_id":1,"likes":1}):
        author = x.get("author_id")
        likes = x.get("likes")
        author_id[str(author)] = [likes]
        label = {} # 每一个用户对应一个label标签库
        # 每一篇帖子
        for post in likes:
            # 判断 post是否已经出现过；如果已经分词过，则直接用库里存的标签；
            if post_tags.__contains__(post):
                tags = post_tags.get(post)
            else:
                t = list(posts.find({"_id":ObjectId(post)},{"title":1,"description":1,"media":1}))
                if t == []:
                    continue
                else:
                    t = t[0]
                title = t.get("title")
                description = x.get("description")
                media = t.get("media")
                # 正则匹配获得文本数据
                try:
                    title = ''.join(
                        re.findall(r"[\u4e00-\u9fa5-\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]",
                                   title))
                    sentence = title + '。'
                except:
                    sentence = ''
                for i in range(len(media)):
                    body = media[i]
                    body = body.get('body')
                    if body != None:
                        body = ''.join(re.findall (r"[\u4e00-\u9fa5-\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]", body))
                        sentence = sentence + body
                try:
                    description = ''.join(re.findall (r"[\u4e00-\u9fa5-\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]", description))
                except:
                    pass

                if description != None:
                    sentence = sentence + '。'+ description +'!'
                # 得到sentence；让sentence去分词提取标签。在标签中的词则生成词表与用户map；
                try:
                    tagsidf = jieba.analyse.extract_tags(sentence, topK=topK, allowPOS=('ns', 's', 'n', 'vn', 'nsf'))
                except:
                    tagsidf = []
                    # 得到该句子的标签
                tags = list(set(tagsidf))
                post_tags[post] = tags

            # 存入id_label并计数 id:{label1:1,label2:4,labeln:x}，label出现的次数代表用户喜欢程度高；
            for tag in tags:
                if place.__contains__(tag):
                    if label.__contains__(tag):
                        label[tag] += 1
                    else:
                        label[tag] = 1
                else:
                    pass

        id_label[ObjectId(author)] = label
        # 将帖子存入user_label 存入新的集合中；
except:
    pass

for key,values in id_label.items():
    if values == {}:
        continue
    dic = {'author_id':ObjectId(key),'tags':value}
    user_tags.insert_one(dic)

print('user_like_tags is done')

"""根据user_comment获得user_likeAndComment标签"""

# 用新的id_label
id_label = {}

try:
    for x in user_comment.find({}, {"author_id":1,"comment":1}):
        author = x.get("author_id")
        post_id = []
        comments = x.get("comment")
        # 对每一条评论
        for comment in comments:
            comments_id = comment.get("post_id")  # 评论帖子对象
            comments_content = comment.get("content") # 评论内容
            if sentiment(comments_content) >= 0.5: # 情感分析为正则将用户评论过的帖子加入user_comment-id
                post_id.append(comments_id)
            label = {} # 每一个用户对应一个label标签库
            author_id[str(author)] = post_id
            #如果评论和点赞重复了，标签权重增加；
            for post in post_id:
                # 判断 post是否已经出现过；如果已经分词过，则直接用库里存的标签；
                if post_tags.__contains__(post):
                    tags = post_tags.get(post)
                else:
                    t = list(posts.find({"_id":ObjectId(post)},{"title":1,"description":1,"media":1}))
                    if t == []:
                        continue
                    else:
                        t = t[0]
                    title = t.get("title")
                    description = x.get("description")
                    media = t.get("media")
                    # 正则匹配获得文本数据
                    try:
                        title = ''.join(
                            re.findall(r"[\u4e00-\u9fa5-\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]",
                                       title))
                        sentence = title + '。'
                    except:
                        sentence = ''
                    for i in range(len(media)):
                        body = media[i]
                        body = body.get('body')
                        if body != None:
                            body = ''.join(re.findall (r"[\u4e00-\u9fa5-\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]", body))
                            sentence = sentence + body
                    try:
                        description = ''.join(re.findall (r"[\u4e00-\u9fa5-\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]", description))
                    except:
                        pass

                    if description != None:
                        sentence = sentence + '。'+ description +'!'
                    # 得到sentence；让sentence去分词提取标签。在标签中的词则生成词表与用户map；
                    try:
                        tagsidf = jieba.analyse.extract_tags(sentence, topK=topK, allowPOS=('ns', 's', 'n', 'vn', 'nsf'))
                    except:
                        tagsidf = []
                        # 得到该句子的标签
                    tags = list(set(tagsidf))
                    post_tags[post] = tags

                # 存入id_label并计数 id:{label1:1,label2:4,labeln:x}，label出现的次数代表用户喜欢程度高；
                for tag in tags:
                    if place.__contains__(tag):
                        if label.__contains__(tag):
                            label[tag] += 1
                        else:
                            label[tag] = 1
                    else:
                        pass

            id_label[ObjectId(author)] = label
            # 将帖子存入user_label 存入新的集合中；
except:
    pass

# 在现有标签库中找用户；如果有则将label叠加；若无，则添加新用户；
for x in user_tags.find({}, {"author_id":1,"tags":1}):
    author = x.get("author_id")
    tags = x.get("tags") # type:dict
    if id_label.__contains__(ObjectId(author)):
        label = id_label[author] # label也是个dict
        print(label)
        # 把 id_label中的标签加入到 tags中；
        for key_like,value_like in tags.items():
            if label.__contains__(key_like):
                label[key_like] += value_like
            else:
                label[key_like] = value_like
        print(label)
    else:
        id_label[ObjectId(author)] = tags


# 存入数据库
user_likeAndcomment_tags = db['user_likeAndcomment_tags']
for key,values in id_label.items():
    if values == {}:
        continue
    dic = {'author_id':ObjectId(key),'tags':values}
    user_likeAndcomment_tags.insert_one(dic)

print('done')
