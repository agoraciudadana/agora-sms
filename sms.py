import requests
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Sequence, DateTime
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import time
from datetime import datetime
import argparse
import sys
import __main__
from argparse import RawTextHelpFormatter

class MessageDao:
    STATUS_QUEUED = 0
    STATUS_SENT_OK = 1
    STATUS_SENT_ERROR = -1

    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()
        self.messages = Table('messages', self.metadata,
            Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
            Column('created', DateTime()),
            Column('modified', DateTime()),
            Column('ip', String(20)),
            Column('dest', String(16)),
            Column('msg', String(160)),
            Column('status', Integer, index=True),
            Column('sms_status', String(20)),
            Column('sms_response', String(400))
        )

    def createTable(self):
        self.metadata.create_all(self.engine)

    def dropTable(self):
        self.metadata.drop_all(self.engine)

    def getAll(self, limit):
        s = select([self.messages]).order_by(self.messages.c.created).limit(limit)
        result = self.engine.execute(s)
        rows = result.fetchall()

        result.close

        return rows

    def getInStatus(self, status, limit):
        s = select([self.messages]).where(self.messages.c.status == status).order_by(self.messages.c.created).limit(limit)
        result = self.engine.execute(s)
        rows = result.fetchall()
        result.close

        return rows

    def getQueued(self, limit):        
        return self.getInStatus(self.STATUS_QUEUED, limit)        

    def insert(self, data):
        insert = self.messages.insert().values(data)        
        self.engine.execute(insert)

    def updateStatus(self, id, status):
        update = self.messages.update().where(self.messages.c.id == id).values(status=status, modified = datetime.now())
        self.engine.execute(update)

class SmsService:
    def __init__(self, endpoint, messageDao):
        self.endpoint = endpoint
        self.messageDao = messageDao

    def queue(self, message):
        message['created'] = datetime.now()
        message['status'] = MessageDao.STATUS_QUEUED
        self.messageDao.insert(message)

    def process(self, limit):
        queued = self.messageDao.getQueued(limit)
        for row in queued:
            print("processing " + str(row))            
            self.messageDao.updateStatus(row['id'], MessageDao.STATUS_SENT_OK)

        return len(queued)

    def getQueued(self, limit):
        return self.messageDao.getQueued(limit)

    def getAll(self, limit):
        return self.messageDao.getAll(limit)

    def getCredit(self):
        return self.endpoint.getCredit()


class AltiriaSmsEndpoint:
    
    def __init__(self, domainId, login, passwd, url, senderId='agoravot'):
        self.domainId = domainId
        self.login = login
        self.passwd = passwd
        self.url = url
        self.senderId = senderId

    ''' sends sms to one or multiple destinations (if the dest is an array, untested)'''
    def sendSms(self, dest, msg):
        
        data = {
            'cmd': 'sendsms',
            'domainId': domainId,
            'login': login,
            'passwd': passwd,
            'dest': dest,
            'msg': msg,
            'senderId': self.senderId
        }

        headers = {'Content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Accept': 'text/plain'}
        print("sending message.." + str(data))
        r = requests.post(self.url, data=data)
        
        return self.parseResponse(r)

    ''' obtains the remaining credit, found in ['lines'][0]['credits(0)'] '''
    def getCredit(self):
        
        data = {
            'cmd': 'getcredit',
            'domainId': self.domainId,
            'login': self.login,
            'passwd': self.passwd,

        }

        headers = {'Content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Accept': 'text/plain'}        
        r = requests.post(url, data=data)
        
        return self.parseResponse(r)

    ''' parses responses in altiria format into dictionaries, one for each line '''
    def parseResponse(self, response):
        nonEmpty = filter(lambda x: len(x.strip()) > 0, response.text.split("\n"))
        parse = lambda x: map(lambda x: (x[0].strip(), x[2].strip()), map(lambda x: x.partition(":"), x.split(" ")))
        lines = [dict(parse(x) +  [('error', x.startswith('ERROR'))]) for x in nonEmpty]
        
        result = {'response': response}
        result['lines'] = lines
        
        return result

# endpoint configuration
domainId = 'comercial'
login = ''
passwd = ''
url = 'http://www.altiria.net/api/http'
senderId = 'agoravot'
# db configuration
db_engine = 'sqlite:///data.db'
# db_engine = 'postgresql+psycopg2://smsservice:smsservice@localhost/smsservice'

def test_add(args):
  service = __getService()
  for counter in range(0, args.count):
    data = {
        'dest': '123123123',
        'msg': 'Hello World' + str(counter)
    }
    service.queue(data)

def test(args):
    service = __getService()
    service.messageDao.dropTable()
    service.messageDao.createTable()
    
    data = {
        'dest': '123123123',
        'msg': 'Hello World'
    }
    service.queue(data)
    service.queue(data)

    rows = service.getQueued()
    print("*** queued rows")
    __printRows(rows)

    processed = service.process()
    print("* processed " + str(processed) + " messages")

    rows = service.getQueued()
    print("*** queued")
    __printRows(rows)

    rows = service.getAll()
    print("*** all rows")
    __printRows(rows)

def __getEngine():
    return create_engine(db_engine, echo=False)    

def __getEndpoint():
    return AltiriaSmsEndpoint(domainId, login, passwd, url, senderId)

def __getService():
    sms = __getEndpoint()
    
    engine = __getEngine()
    messageDao = MessageDao(engine)
    messageDao.createTable()
    service = SmsService(sms, messageDao)

    return service

def __printRows(rows):
    for row in rows:
        print("* row " + str(row))

def reset_database(args):
    engine = __getEngine()
    messageDao = MessageDao(engine)
    messageDao.dropTable()
    messageDao.createTable()

def list_queued(args):
    service = __getService()
    print("** Listing pending messages..")
    rows = service.getQueued(args.count)
    __printRows(rows)

def list_all(args):
    service = __getService()
    print("** Listing all messages..")
    rows = service.getAll(args.count)
    __printRows(rows)

def show_credit(args):
    endpoint = __getEndpoint()
    print('getting credit..')
    credit = endpoint.getCredit()
    print(credit['lines'][0]['credit(0)'])

def process(args):
    service = __getService()
    service.process(args.count)

def send(args):
    endpoint = __getEndpoint()
    if(len(args.command) == 2):
      print('sending to ' + args.command[1])
    else:
      print('missing <target> <message> arguments')  

def main(argv):
    parser = argparse.ArgumentParser(description='Simple sms service', formatter_class=RawTextHelpFormatter)
    parser.add_argument('command', nargs='+', help='''show_credit: obtains and prints remaining credit
list_queued: lists queued messages
list_all: lists all messages
test: performs a simple test
reset_database: recreates the db *** WARNING cannot be undone
process: processes messages
send <target> <message>: send message to target (message must be double quoted)''')
    parser.add_argument('--count', help='number of messages to process or list', type=int, default = 10)
    args = parser.parse_args()
    command = args.command[0]
    if hasattr(__main__, command):
        eval(command + "(args)")
    else:
        parser.print_help()

if __name__ == "__main__":
   main(sys.argv[1:])
