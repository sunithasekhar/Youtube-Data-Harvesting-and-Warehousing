#import build function from the googleapiclient.discovery module for interacting 
#with various Google APIs.
#pymongo module is used to connect MongoDB with python
#The psycopg2 module is a PostgreSQL adapter for python.
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection to retrieve data

def Api_connect():
    Api_Id="AIzaSyBOV1WIAhejTwJ65j1aNGPwKV6YJx_ssmo"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube
youtube=Api_connect()


def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    ) 
    response=request.execute()
    
    for i in response['items']:  
             data=dict(channel_name=i["snippet"]["title"],
                  channe_id=i["id"],
                  Subscribers=i["statistics"]["subscriberCount"],
                  views=i["statistics"]["viewCount"],
                  Total_videos=i["statistics"]["videoCount"],
                  channel_description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
                  
    return data   


#get video ids of the channel selected

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                part='contentDetails').execute()
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None
    while True:
        
        response1=youtube.playlistItems().list(
                                part='snippet',
                                playlistId=playlist_Id,
                                maxResults=50,pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids 


#get video information of the channel selected
def get_video_info(video_Ids):
    video_data=[]
    for video_id in video_Ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id)
        response=request.execute()
        #show the channel name
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnails=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Fovorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails'].get('definiton'),
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)

    return video_data


#get comment information for videos
def  get_comment_info(video_Ids):
    Comment_data=[]
    try:
        for video_id in video_Ids:
            request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                                )
            
            response=request.execute()


            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Auther=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
            
                Comment_data.append(data)
            
    except:
        pass
    return Comment_data


#get_playlists_details

def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
                                      )
        response=request.execute()
        for item in response['items']: 
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount']
                    )
            All_data.append(data)
        next_page_token=response.get('nextPageToken')    
        if next_page_token is None:
            break
    return All_data    


#upload the data to mongoDB

client=pymongo.MongoClient("mongodb+srv://sunitham:shrihanm@cluster0.45oemq0.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]


#create a mongodb database

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload successful"


#table creation for channels,videos,comments

def channels_table():
    
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sunitham",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()




    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_videos int,
                                                                Channel_description text,
                                                                Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created")    

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},
                            {"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_videos,
                                            Channel_description,
                                            Playlist_Id) 
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channe_id'],
                row['Subscribers'],
                row['views'],
                row['Total_videos'],
                row['channel_description'],
                row['Playlist_Id'])
    
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Channels values are already inserted")
    


def videos_table():
    
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sunitham",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                            Channel_Id varchar(100),
                                            Video_Id varchar(100) primary key,
                                            Title text,
                                            Tags text,
                                            Thumbnails varchar(150),
                                            Description text,
                                            Published_Date timestamp,
                                            Duration interval,
                                            Views bigint,
                                            likes bigint,
                                            Comments int,
                                            Fovorite_Count int,
                                            Definition text,
                                            Caption_Status text
                                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},
                            {"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)




    for index,row in df2.iterrows():
                insert_query='''insert into videos(
                                            Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnails,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            likes,
                                            Comments,
                                            Fovorite_Count,
                                            Definition,
                                            Caption_Status) 

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                    
                values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnails'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['likes'],
                    row['Comments'],
                    row['Fovorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])


                cursor.execute(insert_query,values) 
                mydb.commit()




#table of comments in postgresql
                
def comments_table():
    
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sunitham",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments( comment_id varchar(100) primary key,
                                                        Video_Id varchar(80),
                                                        Comment_Text text,
                                                        Comment_Auther varchar(150),
                                                        Comment_Published timestamp
                                                                
                                                                )'''
    cursor.execute(create_query)
    mydb.commit()

       

    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},
                            {"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)


    for index,row in df3.iterrows():
        insert_query='''insert into comments(comment_id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Auther,
                                            Comment_Published ) 
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Auther'],
                row['Comment_Published'])
    
        
        cursor.execute(insert_query,values)
        mydb.commit()
 

def tables():
    channels_table()
    videos_table()
    comments_table()

    return "tables are created"


def display_channel_table():

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},
                            {"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df


def display_video_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},
                            {"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2


def display_comments_table():
      com_list=[]
      db=client["youtube_data"]
      coll1=db["channel_details"]
      for com_data in coll1.find({},
                              {"_id":0,"comment_information":1}):
            for i in range(len(com_data["comment_information"])):
                  com_list.append(com_data["comment_information"][i])
      df3=st.dataframe(com_list)

      return df3


#streamlit input 

with st.sidebar:
    st.title(":red[YOUTUBE - ***DATA HARVESTING AND WAREHOUSING***]")
    st.header(":grey[skill takeaway]")
    st.caption("python scripting")
    st.caption("API Integration")
    st.caption("data collection")
    st.caption("data analysis using MongoDB and SQL")

channel_id=st.text_input(":blue[**enter the *channel ID***]")


if st.button(":green[collect and store data]"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channe_id"])
    if channel_id in ch_ids:
        st.success("this channel id details already exists")
    else:
        insert=channel_details(channel_id)    
        st.success(insert)


if st.button(":green[migrate to SQL]"):
    display_table=tables()
    st.success(display_table)

show_display_table=st.radio(":violet[select the TABLE for view]",("CHANNELS","VIDEOS","COMMENTS")) 

if show_display_table=="CHANNELS":
    display_channel_table()

elif show_display_table=="VIDEOS":
    display_video_table()

elif show_display_table=="COMMENTS":
    display_comments_table()


#SQL connection
    
mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="sunitham",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()


question=st.selectbox(":blue[**select** ***question***]",("1. All the videos and their corresponding channels",
                      "2. Channels with most number of videos, and how many videos they have",
                      "3. 10 most viewed videos and their respective channels",
                      "4. Comments on each video, with video name",
                      "5. Videos with highest number of likes and corresponding channel names",
                      "6. Number of likes and dislikes for each video, with video names", 
                      "7. Total No.of views for each channel with channel names",
                      "8. Names of channels which published videos in 2022",
                      "9. Average duration of videos for each channel with channel names",
                      "10. Videos with highest NO.of comments with channel name"
                             ))

if question=="1. All the videos and their corresponding channels":
    query1='''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    tbl1=cursor.fetchall()
    df=pd.DataFrame(tbl1,columns=["video title","channel name"])
    st.write(df)


elif question=="2. Channels with most number of videos, and how many videos they have":
    query2='''select channel_name as channelname,total_videos as nmbr_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    tbl2=cursor.fetchall()
    df2=pd.DataFrame(tbl2,columns=["channelname","nmbr_videos"])
    st.write(df2)  


elif question=="3. 10 most viewed videos and their respective channels":
    query3='''select views as views, channel_name as channelname,title as videotitle from videos
            where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    tbl3=cursor.fetchall()
    df3=pd.DataFrame(tbl3,columns=["views","channel name","video title"])
    st.write(df3)  

elif question=="4. Comments on each video, with video name":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    tbl4=cursor.fetchall()
    df4=pd.DataFrame(tbl4,columns=["views","channel name"])
    st.write(df4)
    

elif question=="5. Videos with highest number of likes and corresponding channel names":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
            from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    
    tbl5=cursor.fetchall()
    df5=pd.DataFrame(tbl5,columns=["videotitle","channel name","likecount"])
    st.write(df5) 


elif question=="6. Number of likes and dislikes for each video, with video names":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()

    tbl6=cursor.fetchall()
    df6=pd.DataFrame(tbl6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. Total No.of views for each channel with channel names":
    query7='''select channel_name as channelname,views as totalview from channels'''
    cursor.execute(query7)
    mydb.commit()
    tbl7=cursor.fetchall()
    df7=pd.DataFrame(tbl7,columns=["channelname","totalviews"])
    st.write(df7)
    
elif question=="8. Names of channels which published videos in 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channename
            from videos where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    tbl8=cursor.fetchall()
    df8=pd.DataFrame(tbl8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. Average duration of videos for each channel with channel names":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    tbl9=cursor.fetchall()
    df9=pd.DataFrame(tbl9,columns=["channelname","averageduration"])


    t9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        t9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df11=pd.DataFrame(t9)   
    st.write(df11) 



elif question=="10. Videos with highest NO.of comments with channel name":
    query10='''select title as videotitle,channel_name as channelname,comments as comments
            from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    tbl10=cursor.fetchall()
    df10=pd.DataFrame(tbl10,columns=["videotitle","channelname","comments"])
    st.write(df10)

st.markdown('''**Thank You!** :rose:''')



























  