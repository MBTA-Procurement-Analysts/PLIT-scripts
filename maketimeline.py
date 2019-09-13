# maketimeline.py
# Created by Mickey G for PLITv2 use
# To create timeline objects from REQs and POs

#print("This is WIP. Do not run just yet")
#exit()

import pymongo
from pymongo import MongoClient
import sys
from datetime import datetime
import os
from tqdm import tqdm
import re
from collections import Counter
import pprint

pp = pprint.PrettyPrinter()

PO_STATUS_DICT = {"A": "Approved",
             "C": "Complete",
             "D": "Dispatched",
             "I": "Initial",
             "O": "Open",
             "PA": "Pending Approval",
             "PX": "Pending Cancel",
             "X": "Cancelled"}

PO_WL_STATUS = {
    "I": "Initiated",
    "A": "Approved",
    "D": "Denied"
}

# Regex pattern used to determine whether a PO Timeline is internal (on ____ field)
internal_pattern = re.compile(r".*Procurement.*", re.I)

class BundledPOError(Exception):
    def __init__(self, msg):
        self.msg = msg


def is_valid_po(po):
    # status is not canceled
    # PO 'C' status is "Completed", 'D' is "______"
    # These statuses with an empty PO Approval Date, in general, means the
    #   line is cancelled and the PO is marked as 'done'.
    valid_status = not (po.get("PO_Approval_Date", "") =="" and po["Status"] in {"C", "D"})
    if not valid_status:
        print("{} is not a valid PO with Approval Date of {} and Status of {}".format(po["PO_No"], po.get("PO_Approval_Date", ""), po["Status"]))
    return valid_status


def checkBundledPO(po):
    reqs = set([line["Requisition_Data"]["Req_ID"] for line in po["lines"] if line.get("Requisition_Data", None)])
    if len(reqs) > 1:
        raise BundledPOError("[BundledPO] {} REQs found with {}.".format(len(reqs), po["PO_No"]))

def getPOs(dbclient, req_lines_po_set):
    pos = []
    for po_num in req_lines_po:
        for db_po in db.PO_DATA.find({"$and": [{"PO_No": po_num},
                                               {"Business_Unit": req["Business_Unit"]}]}):
            checkBundledPO(db_po)
            if is_valid_po(db_po):
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
                    "Person": req["Buyer"],
                    "Internal": False,
                    "Neutral": False,
                    "Auto": False,
                    "Lifecycle": "REQ"}
    # REQ Approval
    req_approval = {"ID": req["REQ_No"],
                    "Start_DTTM": req["Approved_On"],
                    "EventType": "Approval",
                    "Text": "REQ Approved by {}".format(req["Approved_By"]),
                    "Person": req["Approved_By"],
                    "Internal": True,
                    "Neutral": False,
                    "Auto": False,
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
                   "Person": po["Buyer"],
                   "Internal": True,
                   "Neutral": False,
                   "Auto": False,
                   "Lifecycle": "PO"})
    # PO Approval, skip if not approved 
    if po["PO_Approval_Date"]:
        result.append({"ID": po["PO_No"],
                       "Start_DTTM": po["PO_Approval_Date"],
                       "EventType": "Approval",
                       "Text": "PO Approved by {}".format(po["Approved_By"]),
                       "Person": po["Approved_By"],
                       "Internal": True,
                       "Neutral": False,
                       "Auto": False,
                       "Lifecycle": "PO"})
    return result


def getReqWorklistEvents(req_worklists):
    results = []
    for req_wl in req_worklists:
        results.append({"ID": req_wl["Appr_Inst"],
                        "Start_DTTM": req_wl["Denial_Date_Time"],
                        "EventType": "",
                        "Text": "",
                        "Person": "",
                        "Internal": "",
                        "Neutral": "",
                        "Auto": False,
                        "Lifecycle": ""})
    return results

def getPOWorklistEvents(po_worklists):
    results = []
    po_worklists = sorted(po_worklists, key=lambda k: (k["Event_Date_Time"], k["Approval_Number"]))
    seen_timestamps = set()
    #if len(set([ele["Event_Date_Time"] for ele in po_worklists])) != len(po_worklists):
    #    pp.pprint(po_worklists)
    for po_wl in po_worklists:
        eventtype = PO_WL_STATUS.get(po_wl["Appr_Stat"], po_wl["Appr_Stat"])
        internal = internal_pattern.match(po_wl["Work_List"])
        auto_event = po_wl["Event_Date_Time"] in seen_timestamps
        if po_wl["Appr_Stat"] == "I":
            text = "PO Initiated."
        else:
            text = "{}, {}.".format(po_wl["Work_List"], po_wl["Approval_Number"])
        results.append({"ID": po_wl["Appr_Inst"],
                        "Start_DTTM": po_wl["Event_Date_Time"],
                        "EventType": eventtype,
                        "Person": po_wl["User"],
                        "Text": text,
                        "Internal": bool(internal),
                        "Auto": auto_event,
                        "Neutral": False,
                        "Lifecycle": "PO"})

        seen_timestamps.add(po_wl["Event_Date_Time"])
    return results


def sort_events(tlevents):
    tlevents = sorted(tlevents, key= lambda ele: ele["Start_DTTM"])
    for i in range(len(tlevents) - 1):
        tlevents[i]["End_DTTM"] = tlevents[i+1]["Start_DTTM"]
    tlevents[-1]["End_DTTM"] = datetime.now()

def write_complicated(db, reqNo, poNo, msg):
    db.TIMELINE.insert_one(
        {"REQ_No": reqNo,
         "PO_No": poNo,
         "Complicated": msg,
         "Business_Unit": "",
         "Footnote": "",
         "events": []})

if not sys.argv[1]:
    raise ValueError('Arguments Needed: Write Database: One of (dev, local).')
writelocation = ['dev', 'prod'] if sys.argv[1] == 'both' else [sys.argv[1]]

serverlocation = os.environ['RUBIXLOCATION']

if serverlocation == 'localmac':
    serverlocation = 'local'

client = MongoClient()

for loc in writelocation:
    dbname = 'rubix-{}-{}'.format(serverlocation, loc)
    db = client[dbname]
    db.TIMELINE.delete_many({})
    # For each REQ in the REQ DB
    for req in tqdm(db.REQ_DATA.find()):
        footnote = ""
        if False and len(req.get("worklist", [])) == 0:
            write_complicated(db,
                              req["REQ_No"],
                              "",
                              "[NoWorklist] Req {} does not have a worklist.".format(req["REQ_No"]))
            continue
        # Find POs using REQ Lines, skip if multiple
        req_lines_po = set([line["PO"]["PO_Number"]
                            for line in req["lines"] if line.get("PO", None)])
        if len(req_lines_po) > 1:
            write_complicated(db, 
                              req["REQ_No"], 
                              "", 
                              "[SPLITREQ] REQ {} has {} POs associated with it.".format(req["REQ_No"], len(req_lines_po)))
            continue

        # Get POs, skip if none or none valid found
        try:
            pos = getPOs(db, req_lines_po)
        except BundledPOError as e:
            write_complicated(db,
                              req["REQ_No"],
                              "",
                              e.msg)
            continue
        
        if not pos:
            write_complicated(db,
                              req["REQ_No"],
                              "",
                              "No POs associated with REQ {} are valid. Did not insert in DB.".format(req["REQ_No"]))
            continue

        po_worklists = getPOWorklists(pos)
        req_worklists = req.get('worklist', [])
        
        req_evnets = getReqEvents(req) 
        po_events = getPOEvents(pos)
        
        #req_worklist_events = getReqWorklistEvents(req_worklists)
        po_worklist_events = getPOWorklistEvents(po_worklists)
        if len(pos) > 0:
            timeline_events = [*req_evnets, *po_events, *po_worklist_events]
            sort_events(timeline_events)
            if pos[0]["PO_No"] == "7000006787":
                pp.pprint(timeline_events)
            db.TIMELINE.insert_one(
                {"REQ_No": req["REQ_No"],
                 "PO_No": pos[0]["PO_No"],
                 "Business_Unit": req["Business_Unit"],
                 "Complicated": "",
                 "Footnote": footnote,
                 "events": timeline_events})
        # print(len(req_worklists))
        # print(req_lines_po)
        # db.TIMELINE.insert_one(req)
