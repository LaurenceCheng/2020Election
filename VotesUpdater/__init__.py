import logging
import pathlib
import pygsheets
import json

import azure.functions as func


def main(msg: func.QueueMessage) -> None:
    votesInfo = getVotesInfo(msg)
    
    wks = getGoogleWorkSheet()
    updateVotesInfo(wks, votesInfo)

def getVotesInfo(msg: func.QueueMessage):
    votes_json_string = msg.get_body().decode('utf-8')
    logging.info('Python queue trigger function processed a queue item: %s',
                 votes_json_string)

    parsed_json = (json.loads(votes_json_string))
    print(parsed_json['votes'])
    logging.info('[VotesUpdater] Got votes %s', parsed_json['votes'])
    return parsed_json['votes']

def getGoogleWorkSheet():
    service_file_path = pathlib.Path(__file__).parent / 'credentials.json'
    gc = pygsheets.authorize(service_file=service_file_path)
    sht = gc.open_by_key('< Google Spreadsheet ID >')

    wks_list = sht.worksheets()
    print(wks_list)

    return sht[0]

def updateVotesInfo(wks, votesInfo):
    updatePresidentVotes(wks, votesInfo['president'])
    updateTaipei3Votes(wks, votesInfo['taipei3'])
    updateTaipei4Votes(wks, votesInfo['taipei4'])
    updateTaipei5Votes(wks, votesInfo['taipei5'])
    updateTaichung3Votes(wks, votesInfo['taichung3'])
    updateHualienVotes(wks, votesInfo['hualien'])

def updatePresidentVotes(wks, votes):
    blueVoteCell = 'B15'
    greenVoteCell = 'C15'
    updateBlueAndGreenVotes(wks, blueVoteCell, greenVoteCell, votes)
    
def updateTaipei3Votes(wks, votes):
    blueVoteCell = 'C3'
    greenVoteCell = 'D3'
    updateBlueAndGreenVotes(wks, blueVoteCell, greenVoteCell, votes)

def updateTaipei4Votes(wks, votes):
    blueVoteCell = 'C5'
    greenVoteCell = 'D5'
    updateBlueAndGreenVotes(wks, blueVoteCell, greenVoteCell, votes)

def updateTaipei5Votes(wks, votes):
    blueVoteCell = 'C7'
    greenVoteCell = 'D7'
    updateBlueAndGreenVotes(wks, blueVoteCell, greenVoteCell, votes)

def updateTaichung3Votes(wks, votes):
    blueVoteCell = 'C9'
    greenVoteCell = 'D9'
    updateBlueAndGreenVotes(wks, blueVoteCell, greenVoteCell, votes)

def updateHualienVotes(wks, votes):
    blueVoteCell = 'C11'
    greenVoteCell = 'D11'
    updateBlueAndGreenVotes(wks, blueVoteCell, greenVoteCell, votes)

def updateBlueAndGreenVotes(wks, blueCellId, greenCellId, votes):
    updateCellVotes(wks, blueCellId, votes['blue'])
    updateCellVotes(wks, greenCellId, votes['green'])

def updateCellVotes(wks, cellId, votes):
    wks.update_value(cellId, votes)
