#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3

con = sqlite3.connect(":memory:")  # :memory:
con.text_factory = str

cursor = con.cursor()
cursor.executescript("""CREATE TABLE SMS (id INTEGER PRIMARY KEY,
    imei VARCHAR(32) NOT NULL REFERENCES Subscription(IMEI) ON DELETE CASCADE,
    sender VARCHAR(32), flag VARCHAR(8), KEY CHAR(16), recipient VARCHAR(32), subject TEXT,
    device_time DATETIME, server_time DATETIME);
    """)

sms = [
    {'from': None, 'flag': 'OUT', 'Key': '1289528741609', 'receivedOn': '2010-11-12 09:49:30+0800', 'recipient': '1252013726712156', 'subject': ','},
    {'from': '1252013726712156', 'flag': 'READ', 'Key': '1289528741610', 'receivedOn': '2010-11-12 09:46:11+0800', 'recipient': None, 'subject': 'python: darling,where are you'},
    {'from': '10620121', 'flag': 'READ', 'Key': '1289528741611', 'receivedOn': '2010-11-11 23:05:38+0800', 'recipient': None, 'subject': '中文字体'}
    ]

imei = '207695107596066'

cursor.executemany("INSERT INTO SMS (imei, sender, flag, KEY, recipient, subject, device_time, server_time) VALUES (" + imei + ", :from, :flag, :Key, :receivedOn, :recipient, :subject, DATETIME('NOW'))", sms)

con.commit()

for row in cursor.execute("select * from SMS"):
    print row

cursor.close()
con.close()
