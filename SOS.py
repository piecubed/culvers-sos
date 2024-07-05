#!/usr/bin/python3
# Culver's SOS Data Aggregator
# (c) 2024 Matthew Francis

import threading, csv, pause, requests, io

from datetime import datetime, timedelta, timezone
from flask import Flask, make_response, request, Response

restaraunts = {}

app = Flask(__name__)
newDataLock = threading.Lock()
out = io.StringIO()


hourheader = [
    "Cal Dt",
    "Hour Nbr",
    "DT Avg Order Time",
    "DT Avg Line Time",
    "DT Avg Serve Time",
    "DT Avg Total Time",
    "DT Orders Over 5 Min",
    "DT Orders Over 7 Min",
    "DT Orders Over 10 Min",
    "DT Order Qty",
]

dayheader = [
    "Cal Dt",
    "DT Avg Order Time",
    "DT Avg Line Time",
    "DT Avg Serve Time",
    "DT Avg Total Time",
    "DT Orders Over 5 Min",
    "DT Orders Over 7 Min",
    "DT Orders Over 10 Min",
    "DT Order Qty",
]


@app.route("/SOS/hourly/one")
def getSOSByHourSingle():
    # Accepts query n for how many days ago to query
    n = int(request.args.get("days_ago", default=1))

    restNmbr = int(request.args.get("rest_nmbr", default=-1))
    
    if restNmbr not in restaraunts.keys():
        return Response(status=400)
    
    # Get data for restraunt N days ago
    # Lock for potential error with updater thread
    with newDataLock:
        day = getSinceDate(
            datetime.strftime((datetime.now() - timedelta(n)), "%Y-%m-%d"), restNmbr
        )
    
        # If data hasn't been fetched yet, try to update.
        if len(day) == 0:
            update()
            day = getSinceDate(
                datetime.strftime((datetime.now() - timedelta(n)), "%Y-%m-%d"), restNmbr
            )
    
    data = [hourheader]
    data.extend(day)

    # Write CSV Data
    out = io.StringIO()
    csv.writer(out).writerows(data)

    # Response
    response = make_response(out.getvalue())
    response.headers["Content-type"] = "text/csv"
    return response


@app.route("/SOS/hourly/range")
def getSOSByHourRange():
    # Accepts query n for how many days ago to query
    s = request.args.get("start")
    f = request.args.get("end")
    if s is None or f is None:
        return Response(status=400)

    restNmbr = int(request.args.get("rest_nmbr", default=-1))
    
    if restNmbr not in restaraunts.keys():
        return Response(status=400)
    
    start = datetime.fromisoformat(s)
    finish = datetime.fromisoformat(f)
    data = [hourheader]

    while finish >= start:
        data.extend(getSinceDate(finish.strftime("%Y-%m-%d"), restNmbr))
        finish -= timedelta(1)

    # Write CSV Data
    out = io.StringIO()
    csv.writer(out).writerows(data)

    # Response
    response = make_response(out.getvalue())
    response.headers["Content-type"] = "text/csv"
    return response


@app.route("/SOS/daily/one")
def getSOSByDaySingle():
    # Accepts query n for how many days ago to query
    n = int(request.args.get("days_ago", default=1))
    restNmbr = int(request.args.get("rest_nmbr", default=-1))
    
    if restNmbr not in restaraunts.keys():
        return Response(status=400)
    
    daySum = getDaySum(datetime.strftime((datetime.now() - timedelta(n)), "%Y-%m-%d"), restNmbr)
    data = [dayheader, daySum]

    # Write CSV Data
    out = io.StringIO()
    csv.writer(out).writerows(data)

    # Response
    response = make_response(out.getvalue())
    response.headers["Content-type"] = "text/csv"
    return response


@app.route("/SOS/daily/range")
def getSOSByDayRange():
    # Accepts query n for how many days ago to query
    s = request.args.get("start")
    f = request.args.get("end")
    if s is None or f is None:
        return Response(status=400)
    
    restNmbr = int(request.args.get("rest_nmbr", default=-1))
    
    if restNmbr not in restaraunts.keys():
        return Response(status=400)
    
    start = datetime.fromisoformat(s)
    finish = datetime.fromisoformat(f)
    data = [dayheader]

    while finish >= start:
        data.append(getDaySum(finish.strftime("%Y-%m-%d"), restNmbr))
        finish -= timedelta(1)

    # Write CSV Data
    out = io.StringIO()
    csv.writer(out).writerows(data)

    # Response
    response = make_response(out.getvalue())
    response.headers["Content-type"] = "text/csv"
    return response

@app.route("/SOS/update")
def updateSOS():
    update()
    lastHourly = getSinceDate(datetime.strftime((datetime.now() - timedelta(1)), "%Y-%m-%d"), 886)
    if len(lastHourly) == 0:
        return "0"
    return "1"


@app.route("/SOS/list_rest")
def listRest():
    out = io.StringIO()
    keys = list(restaraunts.keys())
    keys.sort()
    w = csv.writer(out)
    w.writerows([keys])
    response = make_response(out.getvalue())
    response.headers["Content-type"] = "text/csv"
    return response

def getDaySum(ymd, restNmbr):
    # Ensure data isn't being modified in updater thread
    with newDataLock:
        day = getSinceDate(ymd, restNmbr)
    order = 0
    line = 0
    serve = 0
    total = 0
    over5 = 0
    over7 = 0
    over10 = 0
    quantity = 0
    for row in day:
        order += float(row[2])
        line += float(row[3])
        serve += float(row[4])
        total += float(row[5])
        over5 += float(row[6])
        over7 += float(row[7])
        over10 += float(row[8])
        quantity += float(row[9])

    if len(day) == 0:
        return []
    return [
        row[0],
        order // len(day),
        line // len(day),
        serve // len(day),
        total // len(day),
        over5,
        over7,
        over10,
        quantity,
    ]


def splitDays(reader: csv.DictReader):
    t = {}
    start = datetime.now()
    
    for row in reader:
        try:
            t[int(row["Rest Nbr"])].append(
                [
                    row["Cal Dt"],
                    row["Hour Nbr"],
                    row["DT Avg Order Time"],
                    row["DT Avg Line Time"],
                    row["DT Avg Serve Time"],
                    row["DT Avg Total Time"],
                    row["DT Orders Over 5 Min"],
                    row["DT Orders Over 7 Min"],
                    row["DT Orders Over 10 Min"],
                    row["DT Order Qty"],
                ]
                )
        except KeyError:
            t[int(row["Rest Nbr"])] = [
                    row["Cal Dt"],
                    row["Hour Nbr"],
                    row["DT Avg Order Time"],
                    row["DT Avg Line Time"],
                    row["DT Avg Serve Time"],
                    row["DT Avg Total Time"],
                    row["DT Orders Over 5 Min"],
                    row["DT Orders Over 7 Min"],
                    row["DT Orders Over 10 Min"],
                    row["DT Order Qty"],
                ]

            
      
    daysByRest = {}
    
    for restNmbr, rList in t.items():
        daysByRest[restNmbr] = {}
        for row in rList:
            try:
                daysByRest[restNmbr][row[0]].append(row)
            except:
                daysByRest[restNmbr][row[0]] = [row]
    end = datetime.now()
    print(end-start)  
    return daysByRest


def getSinceDate(ymd, restNmbr):
    try:
        nDaysAgo = restaraunts[restNmbr][ymd]
        nDaysAgo.sort(key=lambda a: a[1])
        return nDaysAgo
    except KeyError:
        return []

def update():
    res = requests.get(
            "https://adlpunc01sa.blob.core.windows.net/uufg01ixtjvk/BRINK%20SOS%20HR%20DT%20REST%204%20WK/current.csv"
    )

    reader = csv.DictReader(
        res.content.decode("utf-16").splitlines(), dialect="excel", delimiter="|"
    )

    with newDataLock:
        global restaraunts
        restaraunts = splitDays(reader)
    
def updater():
    while True:
        update()

        # Wait until 8 AM tomorrow
        today = datetime.today()
        tomorrow = datetime(
            today.year,
            today.month,
            today.day,
            8,
            0,
            0,
            tzinfo=timezone(timedelta(hours=-5)),
        ) + timedelta(1)
        print("Time is", datetime.now())
        pause.until(tomorrow)


if __name__ == "__main__":
    threading.Thread(target=updater).start()
    app.run(
        "0.0.0.0", ssl_context=("/home/piesquared/cert.pem", "/home/piesquared/key.pem")
    )
