import json
from datetime import date
import os


# Enregistre le fichier dans datalake
HOME = os.path.dirname(os.path.abspath(__file__))
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def store(rf, source, entity, response):
    TARGET_PATH = getPath(rf, source, entity, response)

    storeDir(response, TARGET_PATH)


def getPath(rf, source, entity, response):
    TARGET_PATH = DATALAKE_ROOT_FOLDER
    if rf:
        TARGET_PATH = raw(source, entity, response, TARGET_PATH)
    else:
        TARGET_PATH = formated(source, entity, response, TARGET_PATH)
    return TARGET_PATH


def raw(source, entity, response, path):
    path += "raw/"
    return sources(source, entity, response, path)


def formated(source, entity, response, path):
    path += "formated/"
    return sources(source, entity, response, path)


def sources(source, entity, response, path):
    if source == 1:
        path += "DATA/"
        return entitysource1(entity, response, path)
    if source == 2:
        path += "API/"
        return entitysource2(entity, response, path)


def entitysource1(entity, response, path):
    if entity == 1:
        path += "SCNF_TER_Horraire/"
    if entity == 2:
        path += "GARES/typeA/"
    return path


def entitysource2(entity, response, path):
    if entity == 1:
        path += "SCNF_TER_Horraire/"
    if entity == 2:
        path += "gares/"
    return path


def storeDir(response, path):
    print("Writing here: ", path)
    current_day = date.today().strftime("%Y%m%d")
    path += current_day + "/"
    if not os.path.exists(path):
        os.makedirs(path)
    print("Writing here: ", path)
    print(response)
    f = open(path + "response.json", "w+")
    f.write(json.dumps(response, indent=4))