import datetime
import logging
import pandas
import requests

import azure.functions as func

from bs4 import BeautifulSoup

def main(mytimer: func.TimerRequest, msg: func.Out[func.QueueMessage]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    raw_data = crawlVotesData()

    president = composeVote(raw_data.at[0, 0], raw_data.at[1, 0])
    taipei3 = composeVote(raw_data.at[2, 0], raw_data.at[3, 0])
    taipei4 = composeVote(raw_data.at[4, 0], raw_data.at[5, 0])
    taipei5 = composeVote(raw_data.at[6, 0], raw_data.at[7, 0])
    taichung3 = composeVote(raw_data.at[8, 0], raw_data.at[9, 0])
    hualien = composeVote(raw_data.at[10, 0], raw_data.at[11, 0])

    votes_info = composeVotesInfo(president, taipei3, taipei4, taipei5, taichung3, hualien)
    # print(votes_info)
    logging.info('[VotesCrawler] Crawled votes %s', votes_info)
    msg.set(votes_info)

def crawlVotesData():
    votes = []
    votes.append(getPresidentVotesData())
    votes.append(getTaipei3VotesData())
    votes.append(getTaipei4VotesData())
    votes.append(getTaipei5VotesData())
    votes.append(getTaichung3VotesData())
    votes.append(getHualienVotesData())
    result = pandas.concat(votes)
    result.reset_index(drop=True, inplace=True)
    # print(result)
    return result

def getPresidentVotesData():
    url = 'https://www.cec.gov.tw/pc/zh_TW/P1/n00000000000000000.html'
    dfs = getDataFrameFromURL(url)

    votes = dfs[3]

    votes.drop([0, 1, 4], inplace=True)
    votes.drop([0, 1, 2, 3, 5, 6], inplace=True, axis=1)
    votes.reset_index(drop=True, inplace=True)
    votes.columns = range(votes.shape[1])

    return votes

def getTaipei3VotesData():
    url = 'https://www.cec.gov.tw/pc/zh_TW/L1/n63000030000000000.html'
    dfs = getDataFrameFromURL(url)
    
    votes = dfs[3]

    votes.drop([0, 1, 3, 5], inplace=True)
    votes.drop([0, 1, 2, 3, 5, 6], inplace=True, axis=1)
    votes.reset_index(drop=True, inplace=True)
    votes.columns = range(votes.shape[1])
    green, blue = votes.iloc[0].copy(), votes.iloc[1].copy()
    votes.iloc[0], votes.iloc[1] = blue, green

    return votes

def getTaipei4VotesData():
    url = 'https://www.cec.gov.tw/pc/zh_TW/L1/n63000040000000000.html'
    dfs = getDataFrameFromURL(url)
    
    votes = dfs[3]

    votes.drop([0, 1, 3, 4, 6, 7, 8], inplace=True)
    votes.drop([0, 1, 2, 3, 5, 6], inplace=True, axis=1)
    votes.reset_index(drop=True, inplace=True)
    votes.columns = range(votes.shape[1])

    return votes

def getTaipei5VotesData():
    url = 'https://www.cec.gov.tw/pc/zh_TW/L1/n63000050000000000.html'
    dfs = getDataFrameFromURL(url)
    
    votes = dfs[3]

    votes.drop([0, 1, 2, 4, 5, 7, 8, 9], inplace=True)
    votes.drop([0, 1, 2, 3, 5, 6], inplace=True, axis=1)
    votes.reset_index(drop=True, inplace=True)
    votes.columns = range(votes.shape[1])

    return votes

def getTaichung3VotesData():
    url = 'https://www.cec.gov.tw/pc/zh_TW/L1/n66000030000000000.html'
    dfs = getDataFrameFromURL(url)
    
    votes = dfs[3]

    votes.drop([0, 1, 2, 4, 5, 7], inplace=True)
    votes.drop([0, 1, 2, 3, 5, 6], inplace=True, axis=1)
    votes.reset_index(drop=True, inplace=True)
    votes.columns = range(votes.shape[1])
    green, blue = votes.iloc[0].copy(), votes.iloc[1].copy()
    votes.iloc[0], votes.iloc[1] = blue, green

    return votes

def getHualienVotesData():
    url = 'https://www.cec.gov.tw/pc/zh_TW/L1/n10015000000000000.html'
    dfs = getDataFrameFromURL(url)
    
    votes = dfs[3]

    votes.drop([0, 1, 3, 4, 5, 7], inplace=True)
    votes.drop([0, 1, 2, 3, 5, 6], inplace=True, axis=1)
    votes.reset_index(drop=True, inplace=True)
    votes.columns = range(votes.shape[1])
    green, blue = votes.iloc[0].copy(), votes.iloc[1].copy()
    votes.iloc[0], votes.iloc[1] = blue, green

    return votes

def getDataFrameFromURL(url):
    res = requests.get(url, headers={'Cache-Control': 'no-cache'})
    soup = BeautifulSoup(res.text.encode("utf8").decode("cp950", "ignore"), 'lxml')
    dfs = pandas.read_html(res.text)
    return dfs

def getBlueVotes(dict):
    return dict['blue']

def getGreenVotes(dict):
    return dict['green']

def composeVote(blueVotes, greenVotes):
    return {"blue" : blueVotes, "green" : greenVotes}

def composeVotesInfo(president, taipei3, taipei4, taipei5, taichung3, hualien):
    return (
            f"{{ \"time\": \"{datetime.datetime.now()}\","
            f"\"votes\" : {{"
                f"\"president\" : {{ \"blue\" : {getBlueVotes(president)}, \"green\" : {getGreenVotes(president)} }},"
                f"\"taipei3\" : {{ \"blue\" : {getBlueVotes(taipei3)}, \"green\" : {getGreenVotes(taipei3)} }},"
                f"\"taipei4\" : {{ \"blue\" : {getBlueVotes(taipei4)}, \"green\" : {getGreenVotes(taipei4)} }},"
                f"\"taipei5\" : {{ \"blue\" : {getBlueVotes(taipei5)}, \"green\" : {getGreenVotes(taipei5)} }},"
                f"\"taichung3\" : {{ \"blue\" : {getBlueVotes(taichung3)}, \"green\" : {getGreenVotes(taichung3)} }},"
                f"\"hualien\" : {{ \"blue\" : {getBlueVotes(hualien)}, \"green\" : {getGreenVotes(hualien)} }},"
                f"}}"
            f"}}"
        )
