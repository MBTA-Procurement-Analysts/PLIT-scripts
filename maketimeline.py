# maketimeline.py
# Created by Mickey G for PLITv2 use
# To create timeline objects from REQs and POs

#print("This is WIP. Do not run just yet")
# exit()

import os
import re
import sys
from datetime import datetime
from collections import Counter
from tqdm import tqdm
import pymongo
from pymongo import MongoClient

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

WL_STATUS = {
    "I": "Initiated",
    "A": "Approved",
    "D": "Denied"
}

# Regex pattern used to determine whether a PO Timeline is internal (on ____ field)
internal_pattern = re.compile(r".*Procurement.*", re.I)


class POBundledError(Exception):
    def __init__(self, msg):
        self.msg = "[Bundled PO] {}".format(msg)


class PONoLineError(Exception):
    def __init__(self, msg):
        self.msg = "[PO No Lines] {}".format(msg)


def is_valid_po(po):
    # status is not canceled
    # PO 'C' status is "Completed", 'D' is "Dispatched"
    # These statuses with an empty PO Approval Date, in general, means the
    #   line is cancelled and the PO is marked as 'done'.
    valid_status = not (po.get("PO_Approval_Date", "") == "" and po["Status"] in {"C", "D"})
    if not valid_status:
        print("{} is not a valid PO with Approval Date of {} and Status of {}".format(
            po["PO_No"], po.get("PO_Approval_Date", ""), po["Status"]))
    return valid_status


def checkBundledPO(po):
    if po.get("lines", None) is None:
        raise PONoLineError(
            "PO has no Line information. It might be a DBE shell with no corresponding PO.")
    reqs = set([line["Requisition_Data"]["Req_ID"]
                for line in po["lines"] if line.get("Requisition_Data", None)])
    if len(reqs) > 1:
        raise POBundledError(
            "{} REQs found with {}.".format(len(reqs), po["PO_No"]))


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
    if po.get("PO_Approval_Date", None):
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
    req_worklists = sorted(req_worklists, key=lambda k: (
        k["Appr_Inst"], k["Approval_Number"], k["Date_Time"]))
    seen_timestamps = set()
    # if len(set([ele["Appr_Inst"] for ele in req_worklists])) > 1:
    #     pp.pprint(req_worklists)
    for req_wl in req_worklists:
        eventtype = WL_STATUS.get(req_wl["Appr_Stat"], req_wl["Appr_Stat"])
        auto_event = req_wl["Date_Time"] in seen_timestamps
        if req_wl["Appr_Stat"] == "I":
            text = "Req Initiated."
        else:
            text = "{}, {}".format(
                req_wl["Work_List"], req_wl["Approval_Number"])
        results.append({"ID": "REQWF-" + str(req_wl["Appr_Inst"]),
                        "Start_DTTM": req_wl["Date_Time"],
                        "EventType": "",
                        "Text": text,
                        "Person": req_wl["User"],
                        "Internal": "",
                        "Neutral": "",
                        "Auto": False,
                        "Lifecycle": "REQ_APPROVE_WF"})
    return results


def getPOWorklistEvents(po_worklists):
    results = []
    po_worklists = sorted(po_worklists, key=lambda k: (
        k["Appr_Inst"], k["Approval_Number"], k["Event_Date_Time"]))
    seen_timestamps = set()
    # if len(set([ele["Event_Date_Time"] for ele in po_worklists])) != len(po_worklists):
    #     pass
    for po_wl in po_worklists:
        eventtype = WL_STATUS.get(po_wl["Appr_Stat"], po_wl["Appr_Stat"])
        internal = internal_pattern.match(po_wl["Work_List"])
        auto_event = po_wl["Event_Date_Time"] in seen_timestamps
        if po_wl["Appr_Stat"] == "I":
            text = "PO Initiated."
        else:
            text = "{}, {}.".format(
                po_wl["Work_List"], po_wl["Approval_Number"])
        results.append({"ID": "POWF-" + str(po_wl["Appr_Inst"]),
                        "Start_DTTM": po_wl["Event_Date_Time"],
                        "EventType": eventtype,
                        "Person": po_wl["User"],
                        "Text": text,
                        "Internal": bool(internal),
                        "Auto": auto_event,
                        "Neutral": False,
                        "Lifecycle": "PO_APPROVE_WF"})

        seen_timestamps.add(po_wl["Event_Date_Time"])
    return results


def getReqHoldEvents(req_id, req_hold_arr):
    results = []
    req_hold_arr = sorted(req_hold_arr, key=lambda k: k["Hold_Line_Number"])
    for hold_line in req_hold_arr:
        results.append({"ID": req_id,
                        "Start_DTTM": hold_line["Hold_Start_Time"],
                        "EventType": "Hold Apply",
                        "Text": "On Hold ({}) ({}).".format(hold_line["Hold_Type"], hold_line["Hold_Comment"]),
                        "Internal": True,
                        "Neutral": False,
                        "Lifecycle": "REQ"})
        if hold_line["Hold_Status"] == "Accepted":
            results.append({"ID": req_id,
                            "Start_DTTM": hold_line["Hold_End_Time"],
                            "EventType": "Hold Release",
                            "Text": "Hold for ({}) ({}). Released. Duration is {} days.".format(hold_line["Hold_Type"], hold_line["Hold_Comment"], hold_line["Hold_Duration"]),
                            "Internal": True,
                            "Neutral": False,
                            "Lifecycle": "REQ_HOLD"})
    return results

def sort_events(tlevents):
    return sorted(tlevents, key=lambda ele: ele["Start_DTTM"])
    # for i in range(len(tlevents) - 1):
    #     tlevents[i]["End_DTTM"] = tlevents[i+1]["Start_DTTM"]
    # tlevents[-1]["End_DTTM"] = datetime.now()


def write_complicated(db, reqNo, poNo, msg):
    db.TIMELINE.insert_one(
        {"REQ_No": reqNo,
         "PO_No": poNo,
         "Complicated": msg,
         "Business_Unit": "",
         "Footnote": "",
         "events": []})

# ---------- MAIN FUNCTION BELOW ----------


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
        if len(req.get("worklist", [])) == 0:
            # write_complicated(db,
            #                   req["REQ_No"],
            #                   "",
            #                   "[REQNoWorklist] Req {} does not have a worklist.".format(req["REQ_No"]))
            pass
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
        except POBundledError as e:
            write_complicated(db,
                              req["REQ_No"],
                              "",
                              e.msg)
            continue
        except PONoLineError as e:
            write_complicated(db,
                              req["REQ_No"],
                              "",
                              e.msg)
            continue

        if not pos:
            footnote += "This REQ does not have POs associated with it. "
            # write_complicated(db,
            #                   req["REQ_No"],
            #                   "",
            #                   "[NoValidPO] No POs associated with REQ {} are valid. Did not insert in DB.".format(req["REQ_No"]))
            # continue

        # REQ On Hold Status
        req_hold_events = getReqHoldEvents(req["REQ_No"], req.get("Holds", []))
        po_worklists = getPOWorklists(pos)
        req_worklists = req.get('worklist', [])

        req_evnets = getReqEvents(req)
        po_events = getPOEvents(pos)

        req_worklist_events = getReqWorklistEvents(req_worklists)
        po_worklist_events = getPOWorklistEvents(po_worklists)
        if True:
            timeline_events = [*req_evnets, *po_events,
                               *req_worklist_events, *po_worklist_events, *req_hold_events]
            timeline_events = sort_events(timeline_events)
            db.TIMELINE.insert_one(
                {"REQ_No": req["REQ_No"],
                 "PO_No": pos[0]["PO_No"] if len(pos) != 0 else "",
                 "Business_Unit": req["Business_Unit"],
                 "Complicated": "",
                 "Footnote": footnote,
                 "events": timeline_events})
        # print(len(req_worklists))
        # print(req_lines_po)
        # db.TIMELINE.insert_one(req)
