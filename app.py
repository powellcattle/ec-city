from nosql.address import MongoAddress

__author__ = 'spowell'
import logging
import openlocationcode
from flask import Flask
from flask import request
from flask import render_template
from flask_jsonpify import jsonpify as jsonify
from nosql import mongo_setup
import phonetics
import re

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="flask_autocomplete.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

app = Flask(__name__)
flask_setup = {"host": "192.168.1.170", "port": "5000", "debug": True}
re_pattern = re.compile(r"\s+")

@app.before_first_request
def startup():
    mongo_setup.global_init()


@app.route("/_address_location")
def address_details():
    try:
        ret = {}

        res_address = request.args.get("address")
        found_address = MongoAddress.objects(st_city="EL CAMPO", ad_name_full=res_address).first()
        if found_address:
            for location in found_address.locations:
                if "E911" == location.source:
                    list_coords = location.coords["coordinates"]
                    ret = dict()
                    ret["E911"] = [{"EPSG": "2278"}, {"X": list_coords[0][0]},
                                   {"Y": list_coords[0][1]},
                                   {"PLUS CODE": openlocationcode.encode(
                                       list_coords[1][1], list_coords[1][0])}], \
                                  [{"EPSG": "4326"},
                                   {"X": list_coords[1][0]},
                                   {"Y": list_coords[1][1]},
                                   {"PLUS CODE": openlocationcode.encode(
                                       list_coords[1][1], list_coords[1][0])}]

        return jsonify(ret)
    except Exception as e:
        return (str(e))

@app.route("/phonetic/_compare", methods=["GET"])
def phonetic_street_compare():
    st_name = request.args.get("st_name")
    st_name = re_pattern.sub("", st_name.lower())
    soundex = phonetics.soundex(st_name)
    print(soundex)

    results = set()
    soundexes = MongoAddress.objects(st_city="EL CAMPO", soundex=soundex)
    if soundexes:
        for address in soundexes:
            if address["st_name"] in results:
                continue
            else:
                results.add(address["st_name"])

    metaphone = phonetics.metaphone(request.args.get("st_name").lower())
    metaphones = MongoAddress.objects(st_city="EL CAMPO", metaphone=metaphone)
    if soundexes:
        for address in metaphones:
            if address["st_name"] in results:
                continue
            else:
                results.add(address["st_name"])


        return jsonify({"phonetics": list(results)})
    else:
        return jsonify("")

@app.route("/phonetic/street", methods=["GET"])
def get_street():
    request_stname = request.args.get("st_name")

    if request_stname:
        print(request_stname)
        addresses = MongoAddress.objects(st_city="EL CAMPO", st_name__icontains=request_stname)

    results = set()
    if addresses:
        for address in addresses:
            if address["st_name"] in results:
                continue
            else:
                results.add(address["st_name"])
                if results.__len__() > 5:
                    break

        return jsonify({"st_names": list(results)})
    else:
        return jsonify(results)



@app.route("/address", methods=["GET"])
def get_address():
    req_address = request.args.get("get_address")

    if req_address:
        print(req_address)
        addresses = MongoAddress.objects(st_city="EL CAMPO", ad_name_full__icontains=req_address)[:5]
    else:
        print("find all")
        addresses = MongoAddress.objects(st_city="EL CAMPO")[:5]

    results = list()
    if addresses:
        for address in addresses:
            results.append(address.ad_name_full)
        return jsonify({"addresses": results})
    else:
        return jsonify(results)


@app.route("/phonetic")
def phonetic():
    try:
        return render_template("phonetic_mobile.html")
    except Exception as e:
        return str(e)

@app.route("/mapit")
def mapit():
    try:
        return render_template("address_mobile1.html")
    except Exception as e:
        return str(e)


if '__main__' == __name__:
    app.run(**flask_setup)
