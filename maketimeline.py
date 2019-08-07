# maketimeline.py
# Created by Mickey G for PLITv2 use
# To create timeline objects from REQs and POs

print("This is WIP. Do not run just yet")
exit()

import pymongo
from pymongo import MongoClient
import sys
from datetime import datetime
import os
from tqdm import tqdm

po_status = {"A": "Approved",
             "C": "Complete",
             "D": "Dispatched",
             "I": "Initial",
             "O": "Open",
             "PA": "Pending Approval",
             "PX": "Pending Cancel",
             "X": "Cancelled"}


def is_valid_po(po):
    # status is not canceled
    # PO 'C' status is "Completer"
    valid_status = not (po["PO_Approval_Date"] ==
                        "" and po["Status"] in {"C", "D"})
    return valid_status


def getPOs(dbclient, req_lines_po_set):
    pos = []
    for po_num in req_lines_po:
        for db_po in db.PO_DATA.find({"$and": [{"PO_No": po_num},
                                               {"Business_Unit": req["Business_Unit"]}]}):
            if db_po and is_valid_po(db_po):
                pos.append(db_po)
    return pos


def getPOWorklists(po_arr):
    if len(pos) > 0 and 'worklist' in po_arr[0]:
        return pos[0]["worklist"]
    else:
        return []


def getReqEvents(req):
    # REQ Creation
    req_creation = {"ID": req["REQ_No"],
                    "Start_DTTM": req["REQ_Date"],
                    "EventType": "Creation",
                    "Text": "REQ Creation by {}".format(req["Buyer"]),
                    "Internal": False,
                    "Neutral": False,
                    "Lifecycle": "REQ"}
    # REQ Approval
    req_approval = {"ID": req["REQ_No"],
                    "Start_DTTM": req["Approved_On"],
                    "EventType": "REQ Approval by {}".format(req["Approved_By"]),
                    "Internal": True,
                    "Neutral": False,
                    "Lifecycle": "REQ"}
    return [req_creation, req_approval]


def getPOEvents(po_arr):
    result = []
    if len(po_arr) >= 1:
        po = po_arr[0]
    elif not po_arr:
        return []
    # PO Creation
    result.append({"ID": po["PO_No"],
                   "Start_DTTM": po["PO_Date"],
                   "EventType": "Creation",
                   "Text": "PO Creation",
                   "Internal": True,
                   "Neutral": False,
                   "Lifecycle": "PO"})
    # PO Approval, skip if not approved 
    if po["PO_Approval_Date"]:
        result.append({"ID": po["PO_No"],
                       "Start_DTTM": po["PO_Approval_Date"],
                       "EventType": "Approval",
                       "Text": "PO Approved by {}".format(po["Approved_By"]),
                       "Internal": True,
                       "Neutral": False,
                       "Lifecycle": "PO"})
    return result


def getReqWorklistEvents(req_worklists):
    pass
    results = []
    for req_wl in req_worklists:
        results.append({"ID": req_wl["Appr_Inst"],
                        "Start_DTTM": req_wl["Denial_Date_Time"],
                        "EventType": "",
                        "Text": "",
                        "Internal": "",
                        "Neutral": "",
                        "Lifecycle": ""})


def getPOWorklistEvents(po_worklists):
    results = []
    for po_wl in po_worklists:
        results.append({"ID": "",
                        "Start_DTTM": "",
                        "EventType": "",
                        "Text": "",
                        "Internal": "",
                        "Neutral": "",
                        "Lifecycle": ""})
    pass


def sort_events(tlevents):
    sorted(tlevents, key= lambda ele: ele["Start_DTTM"])
    for i in range(len(tlevents) - 1):
        tlevents[i]["End_DTTM"] = tlevents[i+1]["Start_DTTM"]
    tlevents[-1]["End_DTTM"] = datetime.now()


if not sys.argv[1]:
    raise ValueError('Arguments Needed: Write Database: One of (dev, local).')
writelocation = ['dev', 'prod'] if sys.argv[1] == 'both' else [sys.argv[1]]

serverlocation = os.environ['RUBIXLOCATION']

client = MongoClient()

for loc in writelocation:
    dbname = 'rubix-{}-{}'.format(serverlocation, loc)
    db = client[dbname]
    for req in tqdm(db.REQ_DATA.find()):
        req_lines_po = set([line["PO"]["PO_Number"]
                            for line in req["lines"] if line.get("PO", None)])
        if len(req_lines_po) > 1:
            print("REQ {} has {} POs associated with it. Did not insert these in DB.".format(
                req["REQ_No"], len(req_lines_po)))
            continue

        pos = getPOs(db, req_lines_po)

        if not pos:
            continue

        po_worklists = getPOWorklists(pos)
        req_worklists = req.get('worklist', [])
        req_evnets, po_events = getReqEvents(req), getPOEvents(pos)
        if len(pos) > 0:
            timeline_events = [*req_evnets, *po_events]
            sort_events(timeline_events)
            db.TIMELINE.insert_one(
                {"REQ_No": req["REQ_No"],
                 "PO_No": pos[0]["PO_No"],
                 "events": timeline_events})
        # print(len(req_worklists))
        # print(req_lines_po)
        # db.TIMELINE.insert_one(req)
